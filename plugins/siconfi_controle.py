"""Controle da ingestão SICONFI pelo Trino: fila, refresh de referência e planejamento.

O Airflow não fala com banco, só com o Trino (ADR 0005 e 0009): em produção ele
roda no Serpro e o Postgres fica no MinC. Por isso o controle segue o padrão dos
logs do SALIC v2 e do Mapas (``control.*_ingestion_log``): tabelas no Postgres,
criadas e escritas por SQL que sai pelo ``TrinoHook``.

Tudo é **só inserção**. O Trino não tem ``FOR UPDATE SKIP LOCKED`` nem
``ON CONFLICT``, então nenhuma linha é atualizada:

- ``siconfi_work_units``: cada unidade planejada (endpoint, chave, parâmetros e
  marcador de revisão). Uma revisão nova é uma linha nova; vale a mais recente.
- ``siconfi_work_events``: o resultado de cada tentativa. Uma unidade está
  pendente enquanto a revisão vigente dela não tem evento final.
- ``siconfi_reference_refresh``: cada refresh completo de ``/entes`` e
  ``/anexos-relatorios``, com a pasta do raw.
- ``siconfi_plan_keys``: arquivos do extrato já planejados, por recorte.

Sem trava de linha, a garantia de que duas tasks não processam a mesma unidade
vem da DAG: uma task por endpoint, um run por vez e uma task por vez
(``max_active_tasks=1``).

Todo o SQL é do Trino. A table function ``system.query`` e a procedure
``system.execute`` do conector, que mandariam SQL nativo ao Postgres, ficam de
fora: o Trino do servidor nega as duas ao usuário do Airflow (``Access Denied``,
26/10/2026). A versão vigente de cada unidade e o último evento de cada revisão
saem de ``row_number()``, calculado no Trino; o filtro por endpoint
e por chave desce ao Postgres. Pelo mesmo motivo a DAG não cria índice: o ADR
0014 traz o ``CREATE INDEX`` para quem administra o banco, se a fila ficar lenta.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Protocol, Sequence

from cliente_siconfi import request_hash
from trino_bronze import quote_ident, sql_literal

logger = logging.getLogger(__name__)

DEFAULT_CATALOG = "dw"
DEFAULT_SCHEMA = "control"
UNITS = "siconfi_work_units"
EVENTS = "siconfi_work_events"
REFERENCE = "siconfi_reference_refresh"
PLAN_KEYS = "siconfi_plan_keys"
FINAL_STATUSES = frozenset({"success", "no_data", "permanent_error"})
STATUSES = FINAL_STATUSES | {"retry"}
# Linhas por INSERT ... VALUES. O texto do SQL cresce com os parâmetros JSON; o
# limite do Trino é 1 milhão de caracteres por consulta.
_CHUNK = 500

_DDL = {
    UNITS: """(
        endpoint varchar, work_key varchar, params varchar,
        revision_marker varchar, planned_at timestamp(6) with time zone)""",
    EVENTS: """(
        endpoint varchar, work_key varchar, revision_marker varchar,
        status varchar, error_msg varchar, run_id varchar,
        next_attempt_at timestamp(6) with time zone,
        event_at timestamp(6) with time zone)""",
    REFERENCE: """(
        endpoint varchar, raw_prefix varchar, run_id varchar,
        refreshed_at timestamp(6) with time zone)""",
    PLAN_KEYS: """(
        source varchar, scope_hash varchar, key varchar,
        planned_at timestamp(6) with time zone)""",
}


class Executor(Protocol):
    """O que o controle precisa do Trino: executar e ler."""

    def run(self, sql: str) -> None: ...

    def records(self, sql: str) -> list[Sequence[Any]]: ...


class TrinoExecutor:
    """``Executor`` pelo ``TrinoHook``, como nas DAGs do SALIC v2 e do Mapas."""

    def __init__(self, conn_id: str = "trino_default") -> None:
        self.conn_id = conn_id

    def _hook(self) -> Any:
        from airflow.providers.trino.hooks.trino import TrinoHook

        return TrinoHook(trino_conn_id=self.conn_id)

    def run(self, sql: str) -> None:
        self._hook().run(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        return list(self._hook().get_records(sql))


TRINO_CONN_ID = "trino_default"
# Como nas DAGs Trino do SALIC v2 e do Mapas: o catálogo e o schema de controle
# vêm de Variables, porque em produção o nome do catálogo é escolha de quem
# administra o Trino.
TARGET_CATALOG_VAR = "siconfi_trino_target_catalog"
CONTROL_SCHEMA_VAR = "siconfi_trino_control_schema"


def controle_do_airflow(garantir_tabelas: bool = False) -> SiconfiControle:
    """O controle com o catálogo e o schema das Variables.

    ``garantir_tabelas`` cria o schema e as tabelas. Cada DAG faz isso
    uma vez por run, numa task própria antes das outras, e não em cada task.
    """
    from airflow.sdk import Variable

    controle = SiconfiControle(
        TrinoExecutor(TRINO_CONN_ID),
        catalog=Variable.get(TARGET_CATALOG_VAR, default=DEFAULT_CATALOG),
        schema=Variable.get(CONTROL_SCHEMA_VAR, default=DEFAULT_SCHEMA),
    )
    if garantir_tabelas:
        controle.ensure_tables()
    return controle


# ── literais ─────────────────────────────────────────────────────────────


def texto(valor: str | None) -> str:
    """Literal de texto para o Trino."""
    return "CAST(NULL AS varchar)" if valor is None else sql_literal(valor)


def instante(quando: datetime | None) -> str:
    """Literal ``timestamp(6) with time zone`` do Trino, sempre em UTC."""
    if quando is None:
        return "CAST(NULL AS timestamp(6) with time zone)"
    utc = quando.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"TIMESTAMP '{utc} UTC'"


def _lista(valores: Iterable[str]) -> str:
    """``('a', 'b')`` para um ``IN`` do Trino."""
    return "(" + ", ".join(sql_literal(v) for v in valores) + ")"


def params_json(params: Mapping[str, Any]) -> str:
    """Os parâmetros como o Postgres antigo guardava: JSON, chaves ordenadas."""
    return json.dumps(params, default=str, sort_keys=True)


class SiconfiControle:
    """Fila e estado da ingestão SICONFI, em ``<catalogo>.<schema>.siconfi_*``."""

    def __init__(
        self,
        executor: Executor,
        catalog: str = DEFAULT_CATALOG,
        schema: str = DEFAULT_SCHEMA,
    ) -> None:
        self.executor = executor
        self.catalog = catalog
        self.schema = schema
        # Revisão vigente de cada chave, por endpoint: lida uma vez por instância
        # (uma por task) e atualizada a cada inserção do ``enqueue_many``.
        self._revisoes: dict[str, dict[str, str | None]] = {}

    def _t(self, tabela: str) -> str:
        return f"{quote_ident(self.catalog)}.{quote_ident(self.schema)}.{tabela}"

    def _vigentes(self, where: str, colunas: str, por: str) -> str:
        """Unidades na revisão vigente: a linha mais recente de cada chave."""
        return f"""SELECT {colunas} FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY {por} ORDER BY planned_at DESC) AS n
                FROM {self._t(UNITS)} WHERE {where}
            ) WHERE n = 1"""

    def _ultimos_eventos(self, where: str, colunas: str, por: str) -> str:
        """O evento mais recente de cada revisão (``NULL`` é uma revisão)."""
        return f"""SELECT {colunas} FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY {por}, revision_marker ORDER BY event_at DESC) AS n
                FROM {self._t(EVENTS)} WHERE {where}
            ) WHERE n = 1"""

    def _insert(self, tabela: str, colunas: str, linhas: list[str]) -> None:
        for inicio in range(0, len(linhas), _CHUNK):
            self.executor.run(
                f"INSERT INTO {self._t(tabela)} ({colunas}) VALUES "
                + ", ".join(linhas[inicio : inicio + _CHUNK])
            )

    def verificar_catalogo(self) -> None:
        """Falha cedo, e dizendo onde mexer, se o catálogo não existe no Trino.

        O nome padrão (``dw``) é o do ambiente local; no servidor o catálogo do
        banco tem outro nome, dado pela Variable.
        """
        try:
            self.executor.records(f"SHOW SCHEMAS FROM {quote_ident(self.catalog)}")
        except Exception as exc:
            raise RuntimeError(
                f"o Trino não abriu o catálogo {self.catalog!r}. Confira a Variable "
                f"{TARGET_CATALOG_VAR}: ela deve ter o nome do catálogo do banco "
                f"no Trino deste ambiente. Erro original: {exc}"
            ) from exc
        # O padrão ``dw`` é o nome local: se um dia existir um ``dw`` no servidor,
        # é por esta linha do log que se vê onde o controle está gravando.
        logger.info("[siconfi] controle em %s.%s", self.catalog, self.schema)

    def ensure_tables(self) -> None:
        self.verificar_catalogo()
        self.executor.run(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{quote_ident(self.catalog)}.{quote_ident(self.schema)}"
        )
        for tabela, colunas in _DDL.items():
            self.executor.run(f"CREATE TABLE IF NOT EXISTS {self._t(tabela)} {colunas}")

    # ── fila ─────────────────────────────────────────────────────────────

    def _revisoes_vigentes(self, endpoint: str) -> dict[str, str | None]:
        """Chave → revisão vigente das unidades de ``endpoint``.

        Uma varredura do endpoint em vez de uma consulta por lote de chaves: sem
        índice (a DAG não cria), cada consulta varreria a tabela no Postgres, e o
        planejamento reenfileira todos os entes × anos a cada run.
        """
        if endpoint not in self._revisoes:
            self._revisoes[endpoint] = {
                str(row[0]): row[1]
                for row in self.executor.records(
                    self._vigentes(
                        f"endpoint = {sql_literal(endpoint)}",
                        "work_key, revision_marker",
                        "work_key",
                    )
                )
            }
        return self._revisoes[endpoint]

    def enqueue_many(
        self, endpoint: str, units: Iterable[tuple[dict[str, Any], str | None]]
    ) -> int:
        """Planeja unidades novas ou com revisão nova; devolve quantas entraram.

        Quem já está na fila com a mesma revisão não entra de novo: o planejamento
        relê o extrato e geraria a mesma unidade a cada execução.
        """
        planejadas: dict[str, tuple[str, str | None]] = {}
        for params, revision in units:
            planejadas[request_hash(endpoint, params)] = (params_json(params), revision)
        vigentes = self._revisoes_vigentes(endpoint)
        novas = [
            k
            for k, (_, revisao) in planejadas.items()
            if k not in vigentes or vigentes[k] != revisao
        ]
        agora = instante(datetime.now(timezone.utc))
        self._insert(
            UNITS,
            "endpoint, work_key, params, revision_marker, planned_at",
            [
                f"({texto(endpoint)}, {texto(k)}, {texto(planejadas[k][0])}, "
                f"{texto(planejadas[k][1])}, {agora})"
                for k in novas
            ],
        )
        vigentes.update((k, planejadas[k][1]) for k in novas)
        return len(novas)

    def pending(
        self,
        endpoint: str,
        limit: int,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Unidades cuja revisão vigente não tem evento final, mais antigas primeiro.

        ``retry`` volta a ser pendente depois de ``next_attempt_at``.
        """
        permitidos = {
            chave: sorted(str(v) for v in valores)
            for chave, valores in (param_filter or {}).items()
        }
        if any(not valores for valores in permitidos.values()):
            # Um recorte vazio (``"msc_months": []`` na Variable) não deixa
            # passar nada; e ``IN ()`` é erro de sintaxe no Trino.
            return []
        filtros = "".join(
            f" AND json_extract_scalar(u.params, {sql_literal('$.' + chave)}) IN "
            f"{_lista(valores)}"
            for chave, valores in sorted(permitidos.items())
        )
        e = f"endpoint = {sql_literal(endpoint)}"
        rows = self.executor.records(
            f"""WITH u AS (
                {self._vigentes(e, "work_key, params, revision_marker, planned_at",
                                "work_key")}
            ), ev AS (
                {self._ultimos_eventos(e, "work_key, revision_marker, status, "
                                          "next_attempt_at", "work_key")}
            )
            SELECT u.work_key, u.params, u.revision_marker
            FROM u LEFT JOIN ev ON ev.work_key = u.work_key
             AND ev.revision_marker IS NOT DISTINCT FROM u.revision_marker
            WHERE (ev.status IS NULL OR (ev.status = 'retry'
                   AND (ev.next_attempt_at IS NULL
                        OR ev.next_attempt_at <= current_timestamp)))
            {filtros}
            ORDER BY u.planned_at, u.work_key
            LIMIT {int(limit)}"""
        )
        return [
            {"work_key": key, "params": json.loads(params), "revision_marker": revision}
            for key, params, revision in rows
        ]

    def record_events(
        self, endpoint: str, events: list[Mapping[str, Any]], run_id: str
    ) -> None:
        """Grava o resultado das tentativas: ``work_key``, ``revision_marker``,
        ``status``, ``error`` e, no ``retry``, ``next_attempt_at``."""
        agora = datetime.now(timezone.utc)
        linhas = []
        for ev in events:
            if ev["status"] not in STATUSES:
                raise ValueError(f"status inválido: {ev['status']}")
            erro = ev.get("error")
            linhas.append(
                f"({texto(endpoint)}, {texto(ev['work_key'])}, "
                f"{texto(ev.get('revision_marker'))}, {texto(ev['status'])}, "
                f"{texto(erro[:2000] if erro else None)}, {texto(run_id)}, "
                f"{instante(ev.get('next_attempt_at'))}, "
                f"{instante(ev.get('event_at') or agora)})"
            )
        self._insert(
            EVENTS,
            "endpoint, work_key, revision_marker, status, error_msg, run_id, "
            "next_attempt_at, event_at",
            linhas,
        )

    def insert_units(self, units: list[Mapping[str, Any]]) -> None:
        """Insere unidades sem conferir a fila: só para converter a fila antiga."""
        self._insert(
            UNITS,
            "endpoint, work_key, params, revision_marker, planned_at",
            [
                f"({texto(u['endpoint'])}, {texto(u['work_key'])}, "
                f"{texto(u['params'])}, {texto(u.get('revision_marker'))}, "
                f"{instante(u['planned_at'])})"
                for u in units
            ],
        )

    def latest_status_counts(self) -> dict[str, int]:
        """Unidades por estado vigente: ``pending`` quando não há evento final."""
        rows = self.executor.records(
            f"""WITH u AS (
                {self._vigentes("true", "endpoint, work_key, revision_marker",
                                "endpoint, work_key")}
            ), ev AS (
                {self._ultimos_eventos("true", "endpoint, work_key, revision_marker, "
                                               "status", "endpoint, work_key")}
            )
            SELECT coalesce(ev.status, 'pending'), count(*)
            FROM u LEFT JOIN ev ON ev.endpoint = u.endpoint
             AND ev.work_key = u.work_key
             AND ev.revision_marker IS NOT DISTINCT FROM u.revision_marker
            GROUP BY 1"""
        )
        return {str(status): int(n) for status, n in rows}

    # ── tabelas de referência ────────────────────────────────────────────

    def reference_due(self, endpoint: str, refresh_hours: int) -> bool:
        rows = self.executor.records(
            f"SELECT count(*) FROM {self._t(REFERENCE)} "
            f"WHERE endpoint = {sql_literal(endpoint)} AND refreshed_at > "
            f"current_timestamp - INTERVAL '{int(refresh_hours)}' HOUR"
        )
        return not rows or int(rows[0][0]) == 0

    def mark_reference_refreshed(
        self, endpoint: str, raw_prefix: str, run_id: str
    ) -> None:
        self._insert(
            REFERENCE,
            "endpoint, raw_prefix, run_id, refreshed_at",
            [
                f"({texto(endpoint)}, {texto(raw_prefix)}, {texto(run_id)}, "
                f"{instante(datetime.now(timezone.utc))})"
            ],
        )

    def reference_prefix(self, endpoint: str) -> str | None:
        """Pasta do raw do último refresh completo de ``endpoint``."""
        rows = self.executor.records(
            f"SELECT raw_prefix FROM {self._t(REFERENCE)} "
            f"WHERE endpoint = {sql_literal(endpoint)} "
            f"ORDER BY refreshed_at DESC LIMIT 1"
        )
        return str(rows[0][0]) if rows else None

    # ── planejamento ─────────────────────────────────────────────────────

    def planned_keys(self, source: str, scope_hash: str) -> set[str]:
        rows = self.executor.records(
            f"SELECT key FROM {self._t(PLAN_KEYS)} "
            f"WHERE source = {sql_literal(source)} "
            f"AND scope_hash = {sql_literal(scope_hash)}"
        )
        return {str(row[0]) for row in rows}

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None:
        agora = instante(datetime.now(timezone.utc))
        self._insert(
            PLAN_KEYS,
            "source, scope_hash, key, planned_at",
            [
                f"({texto(source)}, {texto(scope_hash)}, {texto(k)}, {agora})"
                for k in keys
            ],
        )
