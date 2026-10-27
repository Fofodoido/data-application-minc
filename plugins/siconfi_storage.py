"""Gravação da ingestão SICONFI no raw do datalakehouse, planejamento e ingestão.

Cada página que a API devolve vira um arquivo no raw do MinIO (ADR 0009)::

    raw/siconfi/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>/<request_hash>_<offset>.json

O arquivo é o envelope da página -- os mesmos campos da antiga tabela de páginas
cruas (``raw_pages_<endpoint>``) -- com o ``payload`` intacto.

Uma página maior que ``datalakehouse.LIMITE_OBJETO_BYTES`` (o proxy do MinIO
recusa acima de 1 MiB) vira partes, ``<request_hash>_<offset>_p01de03.json`` e
seguintes. Cada parte é o envelope inteiro com uma fatia dos ``items`` do
``payload``, na ordem, e o campo ``_parte`` (``indice``, ``total`` e ``itens``).
O ``item_count`` continua sendo o da página: a soma dos itens das partes tem de
dar ele. Página que cabe no limite continua num arquivo só, sem ``_parte``.

Não há staging: o ADR 0009 não tem etapa entre o raw e a bronze, e a carga na bronze
(``siconfi.bronze_<entidade>``) lê o raw direto. Esta DAG só extrai.

O controle (fila, refresh das tabelas de referência e o que o planejamento já
leu) mora em ``siconfi_controle``, e sai pelo Trino.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Protocol

import datalakehouse
from cliente_siconfi import (
    FACT_ENDPOINTS,
    PlanScope,
    SiconfiClient,
    SiconfiPage,
    SiconfiPermanentError,
    SiconfiRetryableError,
    planned_units,
    request_hash,
)

FONTE = "siconfi"
VALID_ENDPOINTS = frozenset(
    {"anexos-relatorios", "entes", "extrato_entregas", *FACT_ENDPOINTS}
)


# ── raw ──────────────────────────────────────────────────────────────────


def entidade(endpoint: str) -> str:
    """Nome da entidade no datalakehouse e na bronze: o endpoint, com ``_``."""
    if endpoint not in VALID_ENDPOINTS:
        raise ValueError(f"endpoint SICONFI inválido: {endpoint}")
    return endpoint.replace("-", "_")


def envelope_da_pagina(
    page: SiconfiPage, run_id: str, fetched_at: datetime
) -> dict[str, Any]:
    """O que vai para o raw: a resposta e de qual requisição ela veio."""
    return {
        "endpoint": page.endpoint,
        "request_hash": request_hash(page.endpoint, page.params),
        "request_params": page.params,
        "page_offset": page.offset,
        "fetched_at": fetched_at.astimezone(timezone.utc).isoformat(),
        "run_id": run_id,
        "response_headers": page.headers,
        "payload": page.payload,
        "item_count": len(page.items),
    }


def key_raw_do_envelope(
    envelope: Mapping[str, Any], data: datetime, parte: tuple[int, int] | None = None
) -> str:
    """Key do raw: a pasta do run e, dentro dela, a requisição e o deslocamento.

    Determinística de propósito: a mesma página regravada no mesmo run (retry
    da task, ou a conversão das páginas antigas rodada de novo) cai na mesma
    key, em vez de duplicar. É a única sobrescrita que o raw admite, e só com o
    mesmo conteúdo: uma página antiga não tem duas versões no mesmo run.

    ``parte`` é ``(indice, total)`` de uma página dividida (ver ``partes_do_envelope``).
    """
    nome = f"{envelope['request_hash']}_{envelope['page_offset']}"
    if parte is not None:
        nome = datalakehouse.nome_da_parte(nome, *parte)
    return datalakehouse.caminho_no_run(
        datalakehouse.CAMADA_RAW,
        FONTE,
        entidade(envelope["endpoint"]),
        envelope["run_id"],
        data,
        nome,
    )


def _corpo(valor: Any) -> bytes:
    return json.dumps(valor, ensure_ascii=False, default=str).encode("utf-8")


def partes_do_envelope(
    envelope: Mapping[str, Any], limite: int | None = None
) -> list[dict[str, Any]]:
    """O envelope, ou as partes dele quando ele passa de ``limite`` bytes.

    Divide só os ``items`` do ``payload``, na ordem; o resto do envelope se repete
    em cada parte. Página que cabe volta como está, sem ``_parte``, e mantém o nome
    de arquivo de sempre. Sem ``limite``, vale ``datalakehouse.LIMITE_OBJETO_BYTES``.
    """
    limite = datalakehouse.LIMITE_OBJETO_BYTES if limite is None else limite
    if len(_corpo(envelope)) <= limite:
        return [dict(envelope)]
    payload = envelope.get("payload")
    if not isinstance(payload, Mapping) or not isinstance(payload.get("items"), list):
        raise ValueError(
            f"página de {envelope['endpoint']} com mais de {limite} bytes e sem "
            "lista de itens para dividir"
        )
    items: list[Any] = payload["items"]
    # O que se repete em cada parte, com o ``_parte`` no maior tamanho possível.
    fixo = len(
        _corpo(
            {
                **envelope,
                "payload": {**payload, "items": []},
                "_parte": {
                    "indice": len(items),
                    "total": len(items),
                    "itens": len(items),
                },
            }
        )
    )
    # Cada item ocupa o próprio JSON mais o separador ", " da lista.
    grupos = datalakehouse.fatiar_por_tamanho(
        items, lambda item: len(_corpo(item)) + 2, limite - fixo
    )
    return [
        {
            **envelope,
            "payload": {**payload, "items": grupo},
            "_parte": {"indice": indice, "total": len(grupos), "itens": len(grupo)},
        }
        for indice, grupo in enumerate(grupos, start=1)
    ]


def gravar_envelope(envelope: Mapping[str, Any], data: datetime) -> list[str]:
    """Grava o envelope no raw e devolve as keys: uma, ou uma por parte.

    É a única porta de escrita: a DAG de ingestão e a conversão das páginas
    antigas passam por aqui, então as duas produzem o mesmo caminho e formato.
    """
    keys = []
    for parte in partes_do_envelope(envelope):
        posicao = parte.get("_parte")
        key_raw = key_raw_do_envelope(
            parte, data, (posicao["indice"], posicao["total"]) if posicao else None
        )
        datalakehouse.gravar_bytes(_corpo(parte), key_raw)
        keys.append(key_raw)
    return keys


def persist_page(
    page: SiconfiPage,
    run_id: str,
    data: datetime,
    fetched_at: datetime | None = None,
) -> int:
    """Grava a página no raw; devolve quantos itens ela tinha.

    ``data`` é a data do run, não a da coleta: todas as páginas de um run ficam
    na mesma pasta, que é o que a carga da bronze lê.
    """
    envelope = envelope_da_pagina(page, run_id, fetched_at or datetime.now(timezone.utc))
    gravar_envelope(envelope, data)
    return len(page.items)


def prefixo_raw_do_run(endpoint: str, run_id: str, data: datetime) -> str:
    return datalakehouse.prefixo_do_run(
        datalakehouse.CAMADA_RAW, FONTE, entidade(endpoint), run_id, data
    )


def raw_keys(endpoint: str, prefixo: str | None = None) -> list[str]:
    """Keys dos JSON de raw de ``endpoint`` (ou só sob ``prefixo``), em ordem."""
    busca = prefixo or datalakehouse.prefixo(
        datalakehouse.CAMADA_RAW, FONTE, entidade(endpoint)
    )
    return sorted(
        objeto["Key"]
        for objeto in datalakehouse.listar_objetos(busca)
        if objeto["Key"].endswith(".json")
    )


def paginas_completas(keys: Iterable[str]) -> int:
    """Quantas páginas distintas há em ``keys``, contando só as que estão inteiras.

    Uma página dividida conta uma vez, e só se todas as partes estão lá: com uma
    parte faltando, faltam itens dela no raw.
    """
    # base -> total de partes -> índices vistos. Regravar a página com outro
    # tamanho pode deixar partes de duas divisões; vale a que estiver inteira.
    partes: dict[str, dict[int, set[int]]] = {}
    inteiras: set[str] = set()
    for key in keys:
        nome = key.removesuffix(".json")
        achado = datalakehouse.parte_do_nome(nome)
        if achado is None:
            inteiras.add(nome)
            continue
        base, indice, total = achado
        partes.setdefault(base, {}).setdefault(total, set()).add(indice)
    divididas = {
        base
        for base, por_total in partes.items()
        if any(vistas == set(range(1, total + 1)) for total, vistas in por_total.items())
    }
    return len(inteiras | divididas)


def itens_do_raw(key: str) -> list[dict[str, Any]]:
    """Itens de um arquivo do raw (a página, ou uma parte dela), com os tipos que a
    API mandou."""
    items = datalakehouse.ler_json(key)["payload"].get("items", [])
    return [item for item in items if isinstance(item, dict)]


def entity_ids_do_raw(prefixo: str | None) -> list[int]:
    """Códigos IBGE dos entes no raw de ``/entes`` sob ``prefixo``."""
    if not prefixo:
        return []
    ids = {
        int(item["cod_ibge"])
        for key in raw_keys("entes", prefixo)
        for item in itens_do_raw(key)
        if item.get("cod_ibge") is not None
    }
    return sorted(ids)


# ── planejamento ─────────────────────────────────────────────────────────


def _acumular(
    items: list[dict[str, Any]],
    scope: PlanScope,
    buffers: dict[str, list[tuple[dict[str, Any], str | None]]],
) -> None:
    for item in items:
        for endpoint, params, revision in planned_units(item, scope):
            buffers[endpoint].append((params, revision))


class _ControlePlanejamento(Protocol):
    def planned_keys(self, source: str, scope_hash: str) -> set[str]: ...

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None: ...

    def enqueue_many(
        self, endpoint: str, units: list[tuple[dict[str, Any], str | None]]
    ) -> int: ...


def plan_from_extrato(
    controle: _ControlePlanejamento,
    scope: PlanScope,
    budget: int,
    flush_size: int = 5000,
) -> dict[str, int]:
    """Enfileira as unidades dos arquivos do extrato ainda não planejados.

    Lê o raw: o marcador de revisão é ``str()`` de campos do item, e com tudo
    virando texto ele mudaria e reabriria unidades concluídas. Cada arquivo é
    planejado uma vez por recorte; mudar o recorte relê tudo. ``budget`` limita
    os itens lidos por execução, sempre em arquivos inteiros.

    As unidades e os arquivos lidos vão para o controle em lote, a cada
    ``flush_size`` unidades acumuladas e no fim, e não arquivo por arquivo.
    """
    fingerprint = scope.fingerprint()
    done = controle.planned_keys("extrato_entregas", fingerprint)
    created = {endpoint: 0 for endpoint in FACT_ENDPOINTS}
    buffers: dict[str, list[tuple[dict[str, Any], str | None]]] = {
        endpoint: [] for endpoint in FACT_ENDPOINTS
    }
    lidos: list[str] = []

    def flush() -> None:
        for endpoint in FACT_ENDPOINTS:
            if buffers[endpoint]:
                created[endpoint] += controle.enqueue_many(endpoint, buffers[endpoint])
                buffers[endpoint] = []
        # Só depois de enfileirar: se a task cair no meio, os arquivos são
        # relidos, e reenfileirar a mesma unidade não muda nada.
        if lidos:
            controle.mark_planned("extrato_entregas", fingerprint, list(lidos))
            lidos.clear()

    read = 0
    for key in raw_keys("extrato_entregas"):
        if read >= budget:
            break
        if key in done:
            continue
        items = itens_do_raw(key)
        _acumular(items, scope, buffers)
        lidos.append(key)
        read += len(items)
        if sum(len(b) for b in buffers.values()) >= flush_size:
            flush()
    flush()
    return created


# ── ingestão ─────────────────────────────────────────────────────────────


class _ControleFila(Protocol):
    def pending(
        self,
        endpoint: str,
        limit: int,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]: ...

    def record_events(
        self, endpoint: str, events: list[Mapping[str, Any]], run_id: str
    ) -> None: ...


def ingerir_endpoint(
    endpoint: str,
    controle: _ControleFila,
    client: SiconfiClient,
    scope: PlanScope,
    run_id: str,
    data: datetime,
    limite: int,
    prazo_s: float,
    retry_delay_s: int = 300,
    eventos_por_gravacao: int = 25,
) -> dict[str, int]:
    """Busca as unidades pendentes de ``endpoint`` e grava as páginas no raw.

    Para quando acabam as pendentes, depois de ``limite`` unidades ou quando o
    prazo se esgota. O resultado de cada unidade vai para o controle a cada
    ``eventos_por_gravacao`` unidades e no fim, inclusive se der erro no meio:
    uma unidade sem evento continua pendente e é refeita no próximo run.
    """
    summary = dict.fromkeys(
        ("pending", "success", "no_data", "retry", "permanent_error", "rows"), 0
    )
    if not scope.enabled(endpoint):
        return summary
    unidades = controle.pending(endpoint, limite, scope.constraints(endpoint))
    summary["pending"] = len(unidades)
    deadline = time.monotonic() + prazo_s
    eventos: list[Mapping[str, Any]] = []
    try:
        for unit in unidades:
            if time.monotonic() >= deadline:
                break
            next_attempt_at = None
            try:
                rows = sum(
                    persist_page(page, run_id, data)
                    for page in client.iter_pages(endpoint, unit["params"])
                )
                status, error = ("success" if rows else "no_data"), None
                summary["rows"] += rows
            except SiconfiRetryableError as exc:
                status, error = "retry", str(exc)
                next_attempt_at = datetime.now(timezone.utc) + timedelta(
                    seconds=retry_delay_s
                )
            except SiconfiPermanentError as exc:
                status = "no_data" if exc.status_code == 404 else "permanent_error"
                error = str(exc)
            eventos.append(
                {
                    "work_key": unit["work_key"],
                    "revision_marker": unit.get("revision_marker"),
                    "status": status,
                    "error": error,
                    "next_attempt_at": next_attempt_at,
                }
            )
            summary[status] += 1
            if len(eventos) >= eventos_por_gravacao:
                controle.record_events(endpoint, eventos, run_id)
                eventos = []
    finally:
        if eventos:
            controle.record_events(endpoint, eventos, run_id)
    return summary
