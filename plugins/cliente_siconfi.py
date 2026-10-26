"""Cliente resiliente para a API pública SICONFI.

O limite divulgado pelo Tesouro é de uma requisição por segundo. O
``SiconfiRateLimiter`` espaça as requisições dentro do processo; o contrato vale
para a API inteira porque a ``siconfi_ingestion_dag`` roda uma task por vez
(``max_active_tasks=1``) e um run por vez. Paralelizar endpoints nunca deu
vazão: a API já limita tudo a 1 req/s. E o Airflow não fala com banco (ADR 0005
e 0009), então o limitador não pode morar no Postgres.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
import unicodedata
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://apidatalake.tesouro.gov.br/ords/cdwhprd/siconfi/tt"
RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}


class SiconfiRequestError(RuntimeError):
    """Erro HTTP da SICONFI com contexto suficiente para a fila de controle."""

    def __init__(self, status_code: int | None, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code


class SiconfiRetryableError(SiconfiRequestError):
    """Falha transitória: a unidade de trabalho deve voltar para a fila."""


class SiconfiPermanentError(SiconfiRequestError):
    """Falha de contrato/parâmetro: repetir não muda o resultado."""


@dataclass(frozen=True)
class SiconfiPage:
    endpoint: str
    params: dict[str, Any]
    offset: int
    items: list[dict[str, Any]]
    has_more: bool
    headers: dict[str, str]
    payload: dict[str, Any]


def request_hash(endpoint: str, params: dict[str, Any]) -> str:
    """Identificador determinístico da consulta, sem depender da ordem do dict."""
    canonical = json.dumps(
        {"endpoint": endpoint, "params": params},
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class SiconfiRateLimiter:
    """Garante ``interval_s`` entre o início de duas requisições deste processo."""

    def __init__(self, interval_s: float = 1.05) -> None:
        self.interval_s = interval_s
        self._next_allowed = 0.0

    def reserve(self) -> None:
        now = time.monotonic()
        wait_s = self._next_allowed - now
        if wait_s > 0:
            time.sleep(wait_s)
        self._next_allowed = max(now, self._next_allowed) + self.interval_s

    def close(self) -> None:
        """Sem recurso a liberar; existe para o cliente fechar tudo de um jeito só."""


class SiconfiClient:
    """GET paginado, rate-limited e com retry classificado da SICONFI."""

    def __init__(
        self,
        *,
        timeout_s: float = 90.0,
        max_retries: int = 4,
        page_limit: int = 5000,
        rate_limit_interval_s: float = 1.05,
    ) -> None:
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.page_limit = page_limit
        self.rate_limiter = SiconfiRateLimiter(interval_s=rate_limit_interval_s)
        self.client = httpx.Client(
            base_url=BASE_URL,
            headers={"Accept": "application/json"},
            timeout=httpx.Timeout(timeout_s),
            follow_redirects=True,
        )

    def close(self) -> None:
        self.client.close()
        self.rate_limiter.close()

    def _get(
        self, endpoint: str, params: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        path = f"/{endpoint.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            self.rate_limiter.reserve()
            try:
                response = self.client.get(path, params=params)
            except httpx.HTTPError as exc:
                if attempt == self.max_retries:
                    raise SiconfiRetryableError(None, f"erro de rede: {exc}") from exc
                self._backoff(attempt, None)
                continue

            if response.status_code == 200:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise SiconfiRetryableError(
                        200, "resposta 200 sem JSON válido"
                    ) from exc
                if not isinstance(payload, dict):
                    raise SiconfiPermanentError(200, "resposta não possui envelope JSON")
                return payload, {k.lower(): v for k, v in response.headers.items()}

            detail = response.text[:500]
            if response.status_code in RETRYABLE_STATUS:
                if attempt == self.max_retries:
                    raise SiconfiRetryableError(response.status_code, detail)
                self._backoff(attempt, response.headers.get("Retry-After"))
                continue
            raise SiconfiPermanentError(response.status_code, detail)

        raise AssertionError("loop de retry terminou inesperadamente")

    @staticmethod
    def _backoff(attempt: int, retry_after: str | None) -> None:
        try:
            base = float(retry_after) if retry_after else min(60.0, 2.0**attempt)
        except ValueError:
            base = min(60.0, 2.0**attempt)
        wait_s = base + random.uniform(0, min(1.0, base * 0.2))
        logger.warning("[siconfi] retry HTTP em %.2fs (tentativa %s)", wait_s, attempt)
        time.sleep(wait_s)

    def iter_pages(self, endpoint: str, params: dict[str, Any]) -> Iterator[SiconfiPage]:
        """Itera o envelope ORDS usando ``hasMore`` e ``offset``.

        O tamanho real padrão do servidor já variou (a documentação cita 5 mil
        e a resposta atual de ``/entes`` informa 6 mil), por isso o limite é
        enviado explicitamente e o avanço usa a quantidade realmente recebida.
        """
        offset = 0
        base_params = {k: v for k, v in params.items() if v is not None}
        while True:
            page_params = {**base_params, "offset": offset, "limit": self.page_limit}
            payload, headers = self._get(endpoint, page_params)
            raw_items = payload.get("items", [])
            if not isinstance(raw_items, list) or not all(
                isinstance(item, dict) for item in raw_items
            ):
                raise SiconfiPermanentError(200, "envelope sem lista 'items' de objetos")
            has_more = bool(payload.get("hasMore", False))
            yield SiconfiPage(
                endpoint=endpoint,
                params=base_params,
                offset=int(payload.get("offset", offset)),
                items=raw_items,
                has_more=has_more,
                headers=headers,
                payload=payload,
            )
            if not has_more:
                return
            if not raw_items:
                raise SiconfiPermanentError(200, "hasMore=true com página vazia")
            offset += len(raw_items)


# --- Planejamento a partir do extrato de entregas --------------------------
#
# O extrato identifica o demonstrativo pelo nome por extenso ("Relatório
# Resumido de Execução Orçamentária", "Balanço Anual (DCA)", "MSC
# Encerramento"...), não pela sigla. As funções abaixo traduzem uma linha dele
# nos parâmetros que os endpoints de fatos aceitam.

FACT_ENDPOINTS = (
    "rreo",
    "rgf",
    "dca",
    "msc_patrimonial",
    "msc_orcamentaria",
    "msc_controle",
)
# Mês que a API de MSC espera para a matriz de encerramento: o extrato informa
# ``periodo=1`` nessas linhas, mas ``me_referencia=1`` com ``MSCE`` volta vazio.
_MSCE_MONTH = 12
_MSC_CLASSES = (
    ("msc_patrimonial", (1, 2, 3, 4)),
    ("msc_orcamentaria", (5, 6)),
    ("msc_controle", (7, 8)),
)
_MSC_VALUE_TYPES = ("beginning_balance", "period_change", "ending_balance")
_MSC_MATRIX_TYPES = ("MSCC", "MSCE")
_PODERES = ("E", "L", "J", "M", "D")
_RREO_PERIODOS = (1, 2, 3, 4, 5, 6)
# Anexos que o endpoint /rreo aceita em ``no_anexo`` (spec do Tesouro).
_RREO_ANEXOS = (
    "RREO-Anexo 01",
    "RREO-Anexo 02",
    "RREO-Anexo 03",
    "RREO-Anexo 04",
    "RREO-Anexo 04 - RGPS",
    "RREO-Anexo 04 - RPPS",
    "RREO-Anexo 04.0 - RGPS",
    "RREO-Anexo 04.1",
    "RREO-Anexo 04.2",
    "RREO-Anexo 04.3 - RGPS",
    "RREO-Anexo 05",
    "RREO-Anexo 06",
    "RREO-Anexo 07",
    "RREO-Anexo 09",
    "RREO-Anexo 10 - RGPS",
    "RREO-Anexo 10 - RPPS",
    "RREO-Anexo 11",
    "RREO-Anexo 13",
    "RREO-Anexo 14",
)
# Instituição do extrato → poder no RGF, conferido na API: o Tribunal de Contas
# responde em ``co_poder=L``. A ordem importa: "Tribunal de Contas" antes de
# "Tribunal", "Ministério Público" antes de qualquer outro.
_PODER_POR_INSTITUICAO = (
    ("DEFENSORIA", "D"),
    ("MINISTERIO PUBLICO", "M"),
    ("TRIBUNAL DE CONTAS", "L"),
    ("TRIBUNAL", "J"),
    ("JUSTICA", "J"),
    ("ASSEMBLEIA", "L"),
    ("CAMARA", "L"),
    ("SENADO", "L"),
    ("PREFEITURA", "E"),
    ("GOVERNO", "E"),
    ("EXECUTIVO", "E"),
)

WorkUnit = tuple[str, dict[str, Any], str]


def _normalise_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in text if not unicodedata.combining(ch)).upper()


def _normalise_type(value: Any, prefix: str) -> str:
    return f"{prefix} Simplificado" if str(value).upper() == "S" else prefix


def classify_delivery(entregavel: Any) -> str | None:
    """Devolve ``rreo``, ``rgf``, ``dca``, ``msc`` ou ``None``."""
    text = _normalise_text(entregavel)
    if "RREO" in text or "RESUMIDO DE EXECUCAO ORCAMENTARIA" in text:
        return "rreo"
    if "RGF" in text or "GESTAO FISCAL" in text:
        return "rgf"
    if "DCA" in text or "QDCC" in text or "BALANCO ANUAL" in text:
        return "dca"
    if "MSC" in text:
        return "msc"
    return None


def poder_da_instituicao(instituicao: Any) -> str | None:
    """Poder do RGF (E, L, J, M, D) de quem entregou, ou ``None`` se desconhecido."""
    text = _normalise_text(instituicao)
    for keyword, poder in _PODER_POR_INSTITUICAO:
        if keyword in text:
            return poder
    return None


def work_units(item: dict[str, Any]) -> list[WorkUnit]:
    """Unidades ``(endpoint, params, revision)`` geradas por uma linha do extrato.

    Gera tudo o que a linha permite; o recorte configurado é aplicado depois,
    por ``PlanScope.allows``. No RGF, só o poder da instituição que entregou —
    os cinco apenas quando a instituição não é reconhecida.
    """
    entity = item.get("cod_ibge") or item.get("id_ente")
    year = item.get("exercicio") or item.get("an_referencia")
    if entity is None or year is None:
        return []
    kind = classify_delivery(item.get("entregavel"))
    period = item.get("periodo")
    periodicity = str(item.get("periodicidade", "")).upper()
    revision = "|".join(
        str(item.get(k, "")) for k in ("data_status", "status_relatorio", "forma_envio")
    )
    entity, year = int(entity), int(year)

    if kind == "rreo" and period is not None:
        return [
            (
                "rreo",
                {
                    "id_ente": entity,
                    "an_exercicio": year,
                    "nr_periodo": int(period),
                    "co_tipo_demonstrativo": _normalise_type(
                        item.get("tipo_relatorio"), "RREO"
                    ),
                },
                revision,
            )
        ]
    if kind == "rgf" and period is not None:
        poder = poder_da_instituicao(item.get("instituicao"))
        return [
            (
                "rgf",
                {
                    "id_ente": entity,
                    "an_exercicio": year,
                    "in_periodicidade": "S" if periodicity == "S" else "Q",
                    "nr_periodo": int(period),
                    "co_tipo_demonstrativo": _normalise_type(
                        item.get("tipo_relatorio"), "RGF"
                    ),
                    "co_poder": co_poder,
                },
                revision,
            )
            for co_poder in ((poder,) if poder else _PODERES)
        ]
    # A DCA não sai do extrato: é anual e fechada, e a API a entrega por ente e
    # exercício. Ela é planejada direto por ``PlanScope.dca_units``, o que evita
    # buscar o extrato de cada ano só para descobrir uma unidade que já se conhece.
    if kind == "msc" and period is not None:
        closing = periodicity == "A" or "ENCERRAMENTO" in _normalise_text(
            item.get("entregavel")
        )
        common = {
            "id_ente": entity,
            "an_referencia": year,
            "me_referencia": _MSCE_MONTH if closing else int(period),
            "co_tipo_matriz": "MSCE" if closing else "MSCC",
        }
        return [
            (
                endpoint,
                {**common, "classe_conta": account_class, "id_tv": value_type},
                revision,
            )
            for endpoint, classes in _MSC_CLASSES
            for account_class in classes
            for value_type in _MSC_VALUE_TYPES
        ]
    return []


# --- Recorte configurável ---------------------------------------------------


def _optional_set(
    config: Mapping[str, Any], key: str, cast: Callable[[Any], Any]
) -> frozenset | None:
    value = config.get(key)
    return None if value is None else frozenset(cast(v) for v in value)


def _optional_int(config: Mapping[str, Any], key: str) -> int | None:
    value = config.get(key)
    return None if value is None else int(value)


@dataclass(frozen=True)
class PlanScope:
    """O que entra na fila e o que sai dela. ``None`` num campo = sem filtro.

    A mesma regra vale no planejamento (``allows``) e no claim
    (``constraints``): estreitar o recorte depois que a fila já foi montada
    também para de buscar o que ficou de fora.

    A DCA é exceção: tem intervalo de anos próprio (``dca_start_year`` e
    ``dca_end_year``) e, sem ele, fica desligada. O balanço de um exercício só
    existe no ano seguinte, então o ano corrente não tem DCA para buscar.

    O RREO aceita ``rreo_periods`` (bimestres) e ``rreo_anexos``. Com anexos
    definidos, cada unidade pede só aqueles anexos (``no_anexo``) em vez do
    relatório inteiro: a RCL, por exemplo, é o Anexo 03 do bimestre 6.
    """

    start_year: int
    end_year: int
    endpoints: frozenset[str] | None = None
    rgf_poderes: frozenset[str] | None = None
    msc_months: frozenset[int] | None = None
    msc_classes: frozenset[int] | None = None
    msc_value_types: frozenset[str] | None = None
    msc_matrix_types: frozenset[str] | None = None
    rreo_periods: frozenset[int] | None = None
    rreo_anexos: frozenset[str] | None = None
    dca_start_year: int | None = None
    dca_end_year: int | None = None

    @classmethod
    def from_config(cls, config: Mapping[str, Any]) -> PlanScope:
        scope = cls(
            start_year=int(config["start_year"]),
            end_year=int(config["end_year"]),
            dca_start_year=_optional_int(config, "dca_start_year"),
            dca_end_year=_optional_int(config, "dca_end_year"),
            endpoints=_optional_set(config, "fact_endpoints", str),
            rgf_poderes=_optional_set(config, "rgf_poderes", lambda v: str(v).upper()),
            msc_months=_optional_set(config, "msc_months", int),
            msc_classes=_optional_set(config, "msc_classes", int),
            msc_value_types=_optional_set(config, "msc_value_types", str),
            msc_matrix_types=_optional_set(
                config, "msc_matrix_types", lambda v: str(v).upper()
            ),
            rreo_periods=_optional_set(config, "rreo_periods", int),
            rreo_anexos=_optional_set(config, "rreo_anexos", str),
        )
        for name, value, valid in (
            ("fact_endpoints", scope.endpoints, FACT_ENDPOINTS),
            ("rgf_poderes", scope.rgf_poderes, _PODERES),
            ("rreo_periods", scope.rreo_periods, _RREO_PERIODOS),
            ("rreo_anexos", scope.rreo_anexos, _RREO_ANEXOS),
            ("msc_value_types", scope.msc_value_types, _MSC_VALUE_TYPES),
            ("msc_matrix_types", scope.msc_matrix_types, _MSC_MATRIX_TYPES),
        ):
            unknown = sorted((value or frozenset()) - set(valid))
            if unknown:
                raise ValueError(f"siconfi_config: {name} inválido: {unknown}")
        if (scope.dca_start_year is None) != (scope.dca_end_year is None):
            raise ValueError(
                "siconfi_config: dca_start_year e dca_end_year devem vir juntos"
            )
        if (
            scope.dca_start_year is not None
            and scope.dca_end_year is not None
            and scope.dca_start_year > scope.dca_end_year
        ):
            raise ValueError(
                "siconfi_config: dca_start_year não pode ser maior que dca_end_year"
            )
        return scope

    def dca_years(self) -> range:
        """Exercícios de DCA a buscar; vazio quando o intervalo não foi configurado."""
        if self.dca_start_year is None or self.dca_end_year is None:
            return range(0)
        return range(self.dca_start_year, self.dca_end_year + 1)

    def dca_units(
        self, entity_ids: Iterable[int]
    ) -> Iterator[tuple[dict[str, Any], str | None]]:
        """Uma unidade de DCA por ente e exercício, sem passar pelo extrato.

        Sem marcador de revisão: a DCA entregue não é reenfileirada. Quem não
        entregou volta vazio e a unidade termina como ``no_data``.
        """
        if not self.enabled("dca"):
            return
        for entity_id in entity_ids:
            for year in self.dca_years():
                yield {"id_ente": int(entity_id), "an_exercicio": year}, None

    def enabled(self, endpoint: str) -> bool:
        if endpoint == "dca" and not self.dca_years():
            return False
        return endpoint not in FACT_ENDPOINTS or (
            self.endpoints is None or endpoint in self.endpoints
        )

    def constraints(self, endpoint: str) -> dict[str, frozenset]:
        """Valores permitidos por parâmetro da unidade de trabalho."""
        year_key = (
            "an_exercicio" if endpoint in ("rreo", "rgf", "dca") else "an_referencia"
        )
        years = (
            self.dca_years()
            if endpoint == "dca"
            else range(self.start_year, self.end_year + 1)
        )
        allowed: dict[str, frozenset | None] = {year_key: frozenset(years)}
        if endpoint == "rgf":
            allowed["co_poder"] = self.rgf_poderes
        if endpoint == "rreo":
            allowed["nr_periodo"] = self.rreo_periods
            allowed["no_anexo"] = self.rreo_anexos
        if endpoint.startswith("msc_"):
            allowed["me_referencia"] = self.msc_months
            allowed["classe_conta"] = self.msc_classes
            allowed["id_tv"] = self.msc_value_types
            allowed["co_tipo_matriz"] = self.msc_matrix_types
        return {key: values for key, values in allowed.items() if values is not None}

    def expand(self, endpoint: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Parâmetros de cada requisição que uma unidade do extrato vira.

        Com ``rreo_anexos``, o RREO vira uma requisição por anexo pedido (com
        ``no_anexo``); sem ele, e nos demais endpoints, a unidade passa como está.
        """
        if endpoint == "rreo" and self.rreo_anexos is not None:
            return [{**params, "no_anexo": anexo} for anexo in sorted(self.rreo_anexos)]
        return [dict(params)]

    def allows(self, endpoint: str, params: Mapping[str, Any]) -> bool:
        return self.enabled(endpoint) and all(
            params.get(key) in values
            for key, values in self.constraints(endpoint).items()
        )

    def fingerprint(self) -> str:
        """Muda quando o recorte muda — e aí o planejamento relê o extrato.

        Os anos da DCA ficam de fora: ela não vem do extrato, então mudá-los não
        justifica reler as centenas de milhares de linhas dele.
        """
        state = {
            name: sorted(value) if isinstance(value, frozenset) else value
            for name, value in vars(self).items()
            if not name.startswith("dca_")
        }
        return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()[:16]


def planned_units(item: dict[str, Any], scope: PlanScope) -> Iterator[WorkUnit]:
    """Unidades de uma linha do extrato, já no formato da fila e dentro do recorte."""
    for endpoint, params, revision in work_units(item):
        for unit_params in scope.expand(endpoint, params):
            if scope.allows(endpoint, unit_params):
                yield endpoint, unit_params, revision
