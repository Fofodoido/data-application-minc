"""Gravação da ingestão SICONFI no datalakehouse e controle retomável da fila.

Cada página que a API devolve vira dois arquivos no MinIO, no mesmo caminho
(ADR 0009 e 0011)::

    raw/siconfi/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>/<request_hash>_<offset>.json
    staging/siconfi/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>/<request_hash>_<offset>.parquet

O raw é o envelope da página -- os mesmos campos da antiga tabela de páginas
cruas (``raw_pages_<endpoint>``) -- com o ``payload`` intacto. O staging tem uma
linha por item, tudo texto, e é derivado só do raw. A carga na bronze
(``siconfi.bronze_<entidade>``) fica para a ponte; esta DAG só extrai.

O Postgres guarda apenas o controle, em ``siconfi_control``: a fila de
trabalho, o limitador de requisições, o último refresh das tabelas de
referência e quais arquivos do extrato o planejamento já leu.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator, Mapping, Protocol

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import cursor as Cursor
from psycopg2.extras import execute_values

import datalakehouse
from cliente_siconfi import (
    CONTROL_SCHEMA,
    FACT_ENDPOINTS,
    PlanScope,
    SiconfiPage,
    planned_units,
    request_hash,
)

FONTE = "siconfi"
# Depois disso, uma unidade em ``running`` volta a poder ser reservada. O
# orçamento de tempo das tasks (``max_run_minutes``) precisa ficar abaixo.
CLAIM_LEASE_MINUTES = 60
VALID_ENDPOINTS = frozenset(
    {"anexos-relatorios", "entes", "extrato_entregas", *FACT_ENDPOINTS}
)
_CONTROL = sql.Identifier(CONTROL_SCHEMA)

_DDL = [
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {control}"),
    sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {control}.work_queue (
            endpoint TEXT NOT NULL,
            work_key TEXT NOT NULL,
            params JSONB NOT NULL,
            revision_marker TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            claimed_at TIMESTAMPTZ,
            claimed_by TEXT,
            next_attempt_at TIMESTAMPTZ,
            last_error TEXT,
            last_success_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (endpoint, work_key)
        )
        """
    ),
    sql.SQL(
        "CREATE INDEX IF NOT EXISTS work_queue_claim_idx "
        "ON {control}.work_queue (endpoint, status, updated_at)"
    ),
    sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {control}.reference_state (
            endpoint TEXT PRIMARY KEY,
            refreshed_at TIMESTAMPTZ NOT NULL
        )
        """
    ),
    # Pasta do raw do último refresh completo: é dela que o planejamento lê os
    # entes. Linha antiga, sem pasta, conta como refresh vencido.
    sql.SQL(
        "ALTER TABLE {control}.reference_state ADD COLUMN IF NOT EXISTS raw_prefix TEXT"
    ),
    # Arquivos do extrato já planejados, por recorte. Substitui a marca d'água
    # pelo id da linha, que só existia enquanto o extrato morava no Postgres.
    sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {control}.plan_keys (
            source TEXT NOT NULL,
            scope_hash TEXT NOT NULL,
            key TEXT NOT NULL,
            planned_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (source, scope_hash, key)
        )
        """
    ),
]
_UPSERT_WORK = sql.SQL(
    """
    INSERT INTO {control}.work_queue (endpoint, work_key, params, revision_marker)
    VALUES %s
    ON CONFLICT (endpoint, work_key) DO UPDATE SET
        params = EXCLUDED.params,
        status = CASE
            WHEN {control}.work_queue.revision_marker
                 IS DISTINCT FROM EXCLUDED.revision_marker
            THEN 'pending' ELSE {control}.work_queue.status END,
        revision_marker = EXCLUDED.revision_marker,
        updated_at = now()
    RETURNING (xmax = 0) AS inserted
    """
).format(control=_CONTROL)


# ── datalakehouse ────────────────────────────────────────────────────────


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


def staging_da_pagina(envelope: Mapping[str, Any]) -> bytes:
    """Parquet do staging a partir do envelope do raw: uma linha por item.

    Recebe o envelope, e não a página, para que o mesmo código converta as
    páginas copiadas do Postgres antigo.
    """
    origem = {
        "_request_hash": envelope["request_hash"],
        "_page_offset": envelope["page_offset"],
        "_fetched_at": envelope["fetched_at"],
        "_run_id": envelope["run_id"],
        # Em texto: um dict aqui seria achatado em uma coluna por parâmetro.
        "_request_params": json.dumps(
            envelope["request_params"], ensure_ascii=False, sort_keys=True, default=str
        ),
    }
    return datalakehouse.registros_para_parquet(
        list(envelope["payload"].get("items", [])),
        enriquecer=lambda _: origem,
        dt_ingest=datetime.fromisoformat(envelope["fetched_at"]),
    )


def key_raw_do_envelope(envelope: Mapping[str, Any], data: datetime) -> str:
    """Key do raw: a pasta do run e, dentro dela, a requisição e o deslocamento.

    Determinística de propósito: a mesma página regravada no mesmo run (retry
    da task, ou a conversão das páginas antigas rodada de novo) cai na mesma
    key, em vez de duplicar.
    """
    return datalakehouse.caminho_no_run(
        datalakehouse.CAMADA_RAW,
        FONTE,
        entidade(envelope["endpoint"]),
        envelope["run_id"],
        data,
        f"{envelope['request_hash']}_{envelope['page_offset']}",
    )


def gravar_envelope(envelope: Mapping[str, Any], data: datetime) -> str:
    """Grava o envelope no raw e o Parquet dele no staging; devolve a key do raw.

    É a única porta de escrita: a DAG de ingestão e a conversão das páginas
    antigas passam por aqui, então as duas produzem o mesmo caminho e formato.
    """
    key_raw = key_raw_do_envelope(envelope, data)
    datalakehouse.gravar_bytes(
        json.dumps(envelope, ensure_ascii=False, default=str).encode("utf-8"), key_raw
    )
    datalakehouse.gravar_bytes(
        staging_da_pagina(envelope), datalakehouse.raw_para_staging_key(key_raw)
    )
    return key_raw


def persist_page(
    page: SiconfiPage,
    run_id: str,
    data: datetime,
    fetched_at: datetime | None = None,
) -> int:
    """Grava a página no raw e no staging; devolve quantos itens ela tinha.

    ``data`` é a data do run, não a da coleta: todas as páginas de um run ficam
    na mesma pasta, que é o que a ponte carrega.
    """
    envelope = envelope_da_pagina(page, run_id, fetched_at or datetime.now(timezone.utc))
    gravar_envelope(envelope, data)
    return len(page.items)


def prefixo_raw_do_run(endpoint: str, run_id: str, data: datetime) -> str:
    return datalakehouse.prefixo_do_run(
        datalakehouse.CAMADA_RAW, FONTE, entidade(endpoint), run_id, data
    )


def prefixo_staging_do_run(endpoint: str, run_id: str, data: datetime) -> str:
    return datalakehouse.prefixo_do_run(
        datalakehouse.CAMADA_STAGING, FONTE, entidade(endpoint), run_id, data
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


def itens_do_raw(key: str) -> list[dict[str, Any]]:
    """Itens de uma página do raw, com os tipos que a API mandou."""
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


class _ControlePlanejamento(Protocol):
    def planned_keys(self, source: str, scope_hash: str) -> set[str]: ...

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None: ...

    def enqueue_many(
        self, endpoint: str, units: list[tuple[dict[str, Any], str | None]]
    ) -> int: ...


def plan_from_extrato(
    storage: _ControlePlanejamento,
    scope: PlanScope,
    budget: int,
    flush_size: int = 5000,
) -> dict[str, int]:
    """Enfileira as unidades dos arquivos do extrato ainda não planejados.

    Lê o raw, não o staging: o marcador de revisão é ``str()`` de campos do
    item, e com tudo virando texto ele mudaria e reabriria unidades concluídas.
    Cada arquivo é planejado uma vez por recorte; mudar o recorte relê tudo.
    ``budget`` limita os itens lidos por execução, sempre em arquivos inteiros.
    """
    fingerprint = scope.fingerprint()
    done = storage.planned_keys("extrato_entregas", fingerprint)
    created = {endpoint: 0 for endpoint in FACT_ENDPOINTS}
    buffers: dict[str, list[tuple[dict[str, Any], str | None]]] = {
        endpoint: [] for endpoint in FACT_ENDPOINTS
    }

    def flush(endpoint: str) -> None:
        if buffers[endpoint]:
            created[endpoint] += storage.enqueue_many(endpoint, buffers[endpoint])
            buffers[endpoint] = []

    read = 0
    for key in raw_keys("extrato_entregas"):
        if read >= budget:
            break
        if key in done:
            continue
        items = itens_do_raw(key)
        for item in items:
            for endpoint, params, revision in planned_units(item, scope):
                buffers[endpoint].append((params, revision))
                if len(buffers[endpoint]) >= flush_size:
                    flush(endpoint)
        for endpoint in FACT_ENDPOINTS:
            flush(endpoint)
        # Só depois de enfileirar: se a task cair no meio, o arquivo é relido,
        # e reenfileirar a mesma unidade não muda nada.
        storage.mark_planned("extrato_entregas", fingerprint, [key])
        read += len(items)
    return created


# ── controle no Postgres ─────────────────────────────────────────────────


class SiconfiStorage:
    """Fila de trabalho e estado do planejamento, em ``siconfi_control``."""

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    @contextmanager
    def _cursor(self) -> Iterator[Cursor]:
        """Cursor numa conexão própria: commit ao sair sem erro, e sempre fecha.

        ``with psycopg2.connect()`` sozinho só encerra a transação — não fecha
        a conexão.
        """
        conn = psycopg2.connect(self.conn_str)
        try:
            with conn, conn.cursor() as cur:
                yield cur
        finally:
            conn.close()

    @staticmethod
    def _validate(endpoint: str) -> None:
        if endpoint not in VALID_ENDPOINTS:
            raise ValueError(f"endpoint SICONFI inválido: {endpoint}")

    def ensure_tables(self) -> None:
        with self._cursor() as cur:
            for statement in _DDL:
                cur.execute(statement.format(control=_CONTROL))

    def reference_due(self, endpoint: str, refresh_hours: int) -> bool:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT refreshed_at, raw_prefix FROM {}.reference_state "
                    "WHERE endpoint = %s"
                ).format(_CONTROL),
                (endpoint,),
            )
            row = cur.fetchone()
        if row is None or row[1] is None:
            return True
        refreshed_at: datetime = row[0]
        return refreshed_at <= datetime.now(timezone.utc) - timedelta(hours=refresh_hours)

    def mark_reference_refreshed(self, endpoint: str, raw_prefix: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.reference_state (endpoint, refreshed_at, raw_prefix) "
                    "VALUES (%s, now(), %s) ON CONFLICT (endpoint) DO UPDATE "
                    "SET refreshed_at = EXCLUDED.refreshed_at, "
                    "raw_prefix = EXCLUDED.raw_prefix"
                ).format(_CONTROL),
                (endpoint, raw_prefix),
            )

    def reference_prefix(self, endpoint: str) -> str | None:
        """Pasta do raw do último refresh completo de ``endpoint``."""
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT raw_prefix FROM {}.reference_state WHERE endpoint = %s"
                ).format(_CONTROL),
                (endpoint,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def planned_keys(self, source: str, scope_hash: str) -> set[str]:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT key FROM {}.plan_keys WHERE source = %s AND scope_hash = %s"
                ).format(_CONTROL),
                (source, scope_hash),
            )
            return {row[0] for row in cur.fetchall()}

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None:
        if not keys:
            return
        with self._cursor() as cur:
            execute_values(
                cur,
                sql.SQL(
                    "INSERT INTO {}.plan_keys (source, scope_hash, key) VALUES %s "
                    "ON CONFLICT DO NOTHING"
                )
                .format(_CONTROL)
                .as_string(cur),
                [(source, scope_hash, key) for key in keys],
            )

    def enqueue_many(
        self,
        endpoint: str,
        units: Iterable[tuple[dict[str, Any], str | None]],
        chunk_size: int = 1000,
    ) -> int:
        """Insere/atualiza várias unidades de trabalho numa única conexão.

        O planejamento produz dezenas de milhares de unidades por execução; uma
        conexão por unidade tornava ``plan_facts`` impraticável contra um
        Postgres remoto. As chaves são deduplicadas porque ``ON CONFLICT DO
        UPDATE`` recusa afetar a mesma linha duas vezes no mesmo comando.
        """
        self._validate(endpoint)
        deduped: dict[str, tuple[str, str, str | None]] = {}
        for params, revision_marker in units:
            key = request_hash(endpoint, params)
            deduped[key] = (key, json.dumps(params, default=str), revision_marker)
        if not deduped:
            return 0

        rows = [(endpoint, *unit) for unit in deduped.values()]
        inserted = 0
        with self._cursor() as cur:
            query = _UPSERT_WORK.as_string(cur)
            for start in range(0, len(rows), chunk_size):
                returned = execute_values(
                    cur,
                    query,
                    rows[start : start + chunk_size],
                    template="(%s, %s, %s::jsonb, %s)",
                    page_size=chunk_size,
                    fetch=True,
                )
                inserted += sum(1 for (is_new,) in returned if is_new)
        return inserted

    def claim(
        self,
        endpoint: str,
        limit: int,
        worker_id: str,
        lease_minutes: int = CLAIM_LEASE_MINUTES,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Reserva até ``limit`` unidades cujos ``params`` caibam em ``param_filter``."""
        self._validate(endpoint)
        filters = sorted((param_filter or {}).items())
        filter_sql = sql.SQL("").join(
            sql.SQL(" AND (params->>{}) = ANY(%s)").format(sql.Literal(key))
            for key, _ in filters
        )
        filter_values = [sorted(str(v) for v in values) for _, values in filters]
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    WITH candidates AS (
                        SELECT endpoint, work_key
                        FROM {control}.work_queue
                        WHERE endpoint = %s AND (
                            status IN ('pending', 'retry')
                            OR (status = 'running'
                                AND claimed_at < now() - (%s * interval '1 minute'))
                        )
                        AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                        {filters}
                        ORDER BY updated_at, work_key
                        FOR UPDATE SKIP LOCKED
                        LIMIT %s
                    )
                    UPDATE {control}.work_queue q
                    SET status = 'running', attempts = q.attempts + 1,
                        claimed_at = now(), claimed_by = %s, updated_at = now()
                    FROM candidates c
                    WHERE q.endpoint = c.endpoint AND q.work_key = c.work_key
                    RETURNING q.work_key, q.params, q.attempts
                    """
                ).format(control=_CONTROL, filters=filter_sql),
                (endpoint, lease_minutes, *filter_values, limit, worker_id),
            )
            return [
                {"work_key": key, "params": params, "attempts": attempts}
                for key, params, attempts in cur.fetchall()
            ]

    def release(self, endpoint: str, work_keys: list[str], worker_id: str) -> None:
        """Devolve à fila unidades reservadas e não processadas.

        Sem isso, uma task que para no meio — por tempo ou por erro — deixa as
        unidades em ``running`` até o lease vencer, e a nova tentativa do
        Airflow não encontra nada para fazer.
        """
        if not work_keys:
            return
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}.work_queue
                    SET status = 'pending', claimed_at = NULL, claimed_by = NULL
                    WHERE endpoint = %s AND work_key = ANY(%s)
                      AND status = 'running' AND claimed_by = %s
                    """
                ).format(_CONTROL),
                (endpoint, work_keys, worker_id),
            )

    def complete(
        self,
        endpoint: str,
        work_key: str,
        status: str,
        error: str | None = None,
        retry_delay_s: int = 300,
    ) -> None:
        if status not in {"success", "no_data", "permanent_error", "retry"}:
            raise ValueError(f"status inválido: {status}")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    UPDATE {}.work_queue
                    SET status = %(status)s, last_error = %(error)s,
                        next_attempt_at = CASE WHEN %(status)s = 'retry'
                            THEN now() + (%(retry_delay_s)s * interval '1 second')
                            ELSE NULL END,
                        last_success_at = CASE
                            WHEN %(status)s IN ('success', 'no_data') THEN now()
                            ELSE last_success_at END,
                        updated_at = now()
                    WHERE endpoint = %(endpoint)s AND work_key = %(work_key)s
                    """
                ).format(_CONTROL),
                {
                    "status": status,
                    "error": error[:2000] if error else None,
                    "retry_delay_s": retry_delay_s,
                    "endpoint": endpoint,
                    "work_key": work_key,
                },
            )
