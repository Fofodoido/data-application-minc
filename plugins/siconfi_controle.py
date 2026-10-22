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
(``max_active_tasks=1``). As leituras que precisam de ``DISTINCT ON`` saem pela
table function ``system.query`` do conector, que executa SQL nativo no Postgres.

O ``CREATE TABLE`` do Trino não cria índice. Os índices que essas leituras usam
são criados pela procedure ``system.execute`` do conector, que repassa o
``CREATE INDEX`` ao Postgres -- também pelo Trino, sem ``psycopg2``. Sem eles,
cada consulta da fila e cada lote do planejamento varrem a tabela inteira, e
``siconfi_work_events`` só cresce.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Protocol, Sequence

from cliente_siconfi import request_hash
from trino_bronze import passthrough, quote_ident, quote_pg_ident, sql_literal

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
# Um índice por consulta: a unidade vigente por chave (``DISTINCT ON (work_key)
# ... ORDER BY work_key, planned_at DESC``), o último evento por revisão, os
# arquivos já planejados por recorte e o último refresh de cada referência.
_INDICES = {
    UNITS: "(endpoint, work_key, planned_at DESC)",
    EVENTS: "(endpoint, work_key, revision_marker, event_at DESC)",
    PLAN_KEYS: "(source, scope_hash)",
    REFERENCE: "(endpoint, refreshed_at DESC)",
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


def controle_do_airflow() -> SiconfiControle:
    """O controle com o catálogo e o schema das Variables, e as tabelas criadas."""
    from airflow.sdk import Variable

    controle = SiconfiControle(
        TrinoExecutor(TRINO_CONN_ID),
        catalog=Variable.get(TARGET_CATALOG_VAR, default=DEFAULT_CATALOG),
        schema=Variable.get(CONTROL_SCHEMA_VAR, default=DEFAULT_SCHEMA),
    )
    controle.ensure_tables()
    return controle


# ── literais ─────────────────────────────────────────────────────────────


def texto(valor: str | None) -> str:
    """Literal de texto para o Trino (e para o SQL nativo do Postgres)."""
    return "CAST(NULL AS varchar)" if valor is None else sql_literal(valor)


def instante(quando: datetime | None) -> str:
    """Literal ``timestamp(6) with time zone`` do Trino, sempre em UTC."""
    if quando is None:
        return "CAST(NULL AS timestamp(6) with time zone)"
    utc = quando.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return f"TIMESTAMP '{utc} UTC'"


def _array_pg(valores: Iterable[str]) -> str:
    return "ARRAY[" + ", ".join(sql_literal(v) for v in valores) + "]::text[]"


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

    # nomes: completo no Trino, e só schema.tabela no SQL nativo do Postgres
    def _t(self, tabela: str) -> str:
        return f"{quote_ident(self.catalog)}.{quote_ident(self.schema)}.{tabela}"

    def _pg(self, tabela: str) -> str:
        return f"{quote_pg_ident(self.schema)}.{tabela}"

    def _nativo(self, sql: str) -> list[Sequence[Any]]:
        return self.executor.records(passthrough(quote_ident(self.catalog), sql))

    def _insert(self, tabela: str, colunas: str, linhas: list[str]) -> None:
        for inicio in range(0, len(linhas), _CHUNK):
            self.executor.run(
                f"INSERT INTO {self._t(tabela)} ({colunas}) VALUES "
                + ", ".join(linhas[inicio : inicio + _CHUNK])
            )

    def ensure_tables(self) -> None:
        self.executor.run(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{quote_ident(self.catalog)}.{quote_ident(self.schema)}"
        )
        for tabela, colunas in _DDL.items():
            self.executor.run(f"CREATE TABLE IF NOT EXISTS {self._t(tabela)} {colunas}")
        for tabela, colunas in _INDICES.items():
            self.executor.run(
                f"CALL {quote_ident(self.catalog)}.system.execute(query => "
                + sql_literal(
                    f"CREATE INDEX IF NOT EXISTS {tabela}_busca_idx "
                    f"ON {self._pg(tabela)} {colunas}"
                )
                + ")"
            )

    # ── fila ─────────────────────────────────────────────────────────────

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
        agora = datetime.now(timezone.utc)
        chaves = list(planejadas)
        entraram = 0
        for inicio in range(0, len(chaves), _CHUNK):
            lote = chaves[inicio : inicio + _CHUNK]
            vigentes = {
                row[0]: row[1]
                for row in self._nativo(
                    f"SELECT DISTINCT ON (work_key) work_key, revision_marker "
                    f"FROM {self._pg(UNITS)} WHERE endpoint = {sql_literal(endpoint)} "
                    f"AND work_key = ANY({_array_pg(lote)}) "
                    f"ORDER BY work_key, planned_at DESC"
                )
            }
            novas = [
                k for k in lote if k not in vigentes or vigentes[k] != planejadas[k][1]
            ]
            self._insert(
                UNITS,
                "endpoint, work_key, params, revision_marker, planned_at",
                [
                    f"({texto(endpoint)}, {texto(k)}, {texto(planejadas[k][0])}, "
                    f"{texto(planejadas[k][1])}, {instante(agora)})"
                    for k in novas
                ],
            )
            entraram += len(novas)
        return entraram

    def pending(
        self,
        endpoint: str,
        limit: int,
        param_filter: Mapping[str, Iterable[Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Unidades cuja revisão vigente não tem evento final, mais antigas primeiro.

        ``retry`` volta a ser pendente depois de ``next_attempt_at``.
        """
        filtros = "".join(
            f" AND (u.params::jsonb ->> {sql_literal(chave)}) = "
            f"ANY({_array_pg(sorted(str(v) for v in valores))})"
            for chave, valores in sorted((param_filter or {}).items())
        )
        e = sql_literal(endpoint)
        rows = self._nativo(
            f"""WITH u AS (
                SELECT DISTINCT ON (work_key) work_key, params, revision_marker,
                       planned_at
                FROM {self._pg(UNITS)} WHERE endpoint = {e}
                ORDER BY work_key, planned_at DESC
            ), e AS (
                SELECT DISTINCT ON (work_key, revision_marker) work_key,
                       revision_marker, status, next_attempt_at
                FROM {self._pg(EVENTS)} WHERE endpoint = {e}
                ORDER BY work_key, revision_marker, event_at DESC
            )
            SELECT u.work_key, u.params, u.revision_marker
            FROM u LEFT JOIN e ON e.work_key = u.work_key
             AND e.revision_marker IS NOT DISTINCT FROM u.revision_marker
            WHERE (e.status IS NULL OR (e.status = 'retry'
                   AND (e.next_attempt_at IS NULL OR e.next_attempt_at <= now())))
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
        rows = self._nativo(
            f"""WITH u AS (
                SELECT DISTINCT ON (endpoint, work_key) endpoint, work_key,
                       revision_marker
                FROM {self._pg(UNITS)} ORDER BY endpoint, work_key, planned_at DESC
            ), e AS (
                SELECT DISTINCT ON (endpoint, work_key, revision_marker) endpoint,
                       work_key, revision_marker, status
                FROM {self._pg(EVENTS)}
                ORDER BY endpoint, work_key, revision_marker, event_at DESC
            )
            SELECT coalesce(e.status, 'pending'), count(*)
            FROM u LEFT JOIN e ON e.endpoint = u.endpoint AND e.work_key = u.work_key
             AND e.revision_marker IS NOT DISTINCT FROM u.revision_marker
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
