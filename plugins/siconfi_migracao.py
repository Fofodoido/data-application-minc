"""Conversão única das páginas SICONFI antigas para o datalakehouse.

Até outubro de 2026 a ingestão gravava cada página em
``siconfi_bronze.raw_pages_<endpoint>``, no Postgres. Esta conversão lê essas
tabelas, sem escrever nelas, e grava cada página no raw e no staging pelo mesmo
``siconfi_storage.gravar_envelope`` que a DAG de ingestão usa. O resultado é
indistinguível do que a DAG teria gravado, exceto pelo campo ``_migrado_de``
do raw (``siconfi_bronze.raw_pages_rreo#123``).

A pasta é a do run original, pela data no ``run_id``: páginas de um run que
passou da meia-noite ficam juntas, como ficariam na DAG nova. As tabelas
``*_items`` não entram: só repetiam o conteúdo das páginas.

O progresso fica em ``siconfi_control.migracao_raw_pages`` (último
``raw_page_id`` copiado por tabela) e só avança depois que o lote inteiro foi
gravado. Interrompida, a conversão recomeça do último lote completo; regravar
uma página cai na mesma key.
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Mapping, Protocol

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection
from psycopg2.extras import RealDictCursor

from cliente_siconfi import CONTROL_SCHEMA
from siconfi_storage import VALID_ENDPOINTS, entidade, gravar_envelope

ORIGEM_SCHEMA = "siconfi_bronze"
ENDPOINTS = tuple(sorted(VALID_ENDPOINTS))
_COLUNAS = (
    "raw_page_id",
    "endpoint",
    "request_hash",
    "request_params",
    "page_offset",
    "fetched_at",
    "run_id",
    "response_headers",
    "payload",
    "item_count",
)
_DATA_NO_RUN_ID = re.compile(r"__(\d{4})-(\d{2})-(\d{2})T")

_SQL_LOTE = sql.SQL(
    "SELECT {colunas} FROM {schema}.{tabela} "
    "WHERE raw_page_id > %s ORDER BY raw_page_id LIMIT %s"
)
_SQL_EXISTE = sql.SQL("SELECT to_regclass(%s) IS NOT NULL")
_SQL_TOTAL = sql.SQL(
    "SELECT count(*), coalesce(max(raw_page_id), 0) FROM {schema}.{tabela}"
)
# Tudo o que a conversão manda para a origem. O teste confere que é só leitura.
_SQL_ORIGEM = (_SQL_LOTE, _SQL_EXISTE, _SQL_TOTAL)

_CONTROL = sql.Identifier(CONTROL_SCHEMA)
_DDL_PROGRESSO = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {control}.migracao_raw_pages (
        tabela TEXT PRIMARY KEY,
        ultimo_raw_page_id BIGINT NOT NULL,
        paginas_copiadas BIGINT NOT NULL,
        atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """
).format(control=_CONTROL)


def tabela_antiga(endpoint: str) -> str:
    return f"raw_pages_{entidade(endpoint)}"


def data_do_run(run_id: str, fetched_at: datetime) -> datetime:
    """Data da pasta: a do ``run_id`` (como na DAG nova) ou, sem ela, a da coleta."""
    achado = _DATA_NO_RUN_ID.search(run_id)
    if achado:
        ano, mes, dia = (int(parte) for parte in achado.groups())
        return datetime(ano, mes, dia, tzinfo=timezone.utc)
    return fetched_at.astimezone(timezone.utc)


def envelope_da_linha(linha: Mapping[str, Any]) -> dict[str, Any]:
    """O envelope que a DAG nova grava, montado a partir de uma linha antiga."""
    fetched_at: datetime = linha["fetched_at"]
    return {
        "endpoint": linha["endpoint"],
        "request_hash": linha["request_hash"],
        "request_params": linha["request_params"],
        "page_offset": int(linha["page_offset"]),
        "fetched_at": fetched_at.astimezone(timezone.utc).isoformat(),
        "run_id": linha["run_id"],
        "response_headers": linha["response_headers"],
        "payload": linha["payload"],
        "item_count": int(linha["item_count"]),
        "_migrado_de": (
            f"{ORIGEM_SCHEMA}.{tabela_antiga(linha['endpoint'])}"
            f"#{linha['raw_page_id']}"
        ),
    }


def gravar_linha(linha: Mapping[str, Any]) -> str:
    """Grava uma página antiga no raw e no staging; devolve a key do raw."""
    return gravar_envelope(
        envelope_da_linha(linha), data_do_run(linha["run_id"], linha["fetched_at"])
    )


class Progresso(Protocol):
    def ler(self, tabela: str) -> tuple[int, int]: ...

    def salvar(self, tabela: str, ultimo: int, copiadas: int) -> None: ...


def migrar_endpoint(
    ler_lote: Callable[[int, int], list[dict[str, Any]]],
    progresso: Progresso,
    endpoint: str,
    lote: int,
    limite: int | None,
    prazo_s: float | None = None,
    paralelo: int = 8,
) -> dict[str, Any]:
    """Copia as páginas de ``endpoint`` depois da última já copiada.

    Para quando a origem acaba, quando ``limite`` páginas foram copiadas neste
    run ou quando ``prazo_s`` se esgota; o progresso só avança por lote inteiro.
    """
    tabela = tabela_antiga(endpoint)
    ultimo, copiadas = progresso.ler(tabela)
    prazo = None if prazo_s is None else time.monotonic() + prazo_s
    neste_run, terminou = 0, False
    with ThreadPoolExecutor(max_workers=paralelo) as executor:
        while limite is None or neste_run < limite:
            if prazo is not None and time.monotonic() >= prazo:
                break
            pedido = lote if limite is None else min(lote, limite - neste_run)
            linhas = ler_lote(ultimo, pedido)
            if not linhas:
                terminou = True
                break
            # list() espera todas e repassa a primeira exceção: o progresso
            # abaixo só roda se o lote inteiro foi gravado.
            list(executor.map(gravar_linha, linhas))
            ultimo = int(linhas[-1]["raw_page_id"])
            copiadas += len(linhas)
            neste_run += len(linhas)
            progresso.salvar(tabela, ultimo, copiadas)
    return {
        "tabela": tabela,
        "copiadas_neste_run": neste_run,
        "total_copiadas": copiadas,
        "ultimo_raw_page_id": ultimo,
        "terminou": terminou,
    }


# ── Postgres ─────────────────────────────────────────────────────────────


@contextmanager
def conexao_origem(dsn: str) -> Iterator[Connection]:
    """Conexão só de leitura com o banco que tem o ``siconfi_bronze``."""
    conn = psycopg2.connect(
        dsn, keepalives=1, keepalives_idle=15, keepalives_interval=5, keepalives_count=3
    )
    conn.set_session(readonly=True, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()


def tabela_existe(conn: Connection, endpoint: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(_SQL_EXISTE, (f"{ORIGEM_SCHEMA}.{tabela_antiga(endpoint)}",))
        row = cur.fetchone()
    return bool(row and row[0])


def leitor_de_lotes(
    conn: Connection, endpoint: str
) -> Callable[[int, int], list[dict[str, Any]]]:
    query = _SQL_LOTE.format(
        colunas=sql.SQL(", ").join(sql.Identifier(c) for c in _COLUNAS),
        schema=sql.Identifier(ORIGEM_SCHEMA),
        tabela=sql.Identifier(tabela_antiga(endpoint)),
    )

    def ler_lote(depois_de: int, limite: int) -> list[dict[str, Any]]:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (depois_de, limite))
            return [dict(row) for row in cur.fetchall()]

    return ler_lote


def total_na_origem(conn: Connection, endpoint: str) -> tuple[int, int]:
    """Quantas páginas a tabela antiga tem, e o maior ``raw_page_id``."""
    with conn.cursor() as cur:
        cur.execute(
            _SQL_TOTAL.format(
                schema=sql.Identifier(ORIGEM_SCHEMA),
                tabela=sql.Identifier(tabela_antiga(endpoint)),
            )
        )
        row = cur.fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


class ProgressoPostgres:
    """Progresso em ``siconfi_control.migracao_raw_pages``, no banco de controle."""

    def __init__(self, conn_str: str) -> None:
        self.conn_str = conn_str

    @contextmanager
    def _cursor(self) -> Iterator[Any]:
        conn = psycopg2.connect(self.conn_str)
        try:
            with conn, conn.cursor() as cur:
                yield cur
        finally:
            conn.close()

    def garantir_tabela(self) -> None:
        with self._cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(_CONTROL))
            cur.execute(_DDL_PROGRESSO)

    def ler(self, tabela: str) -> tuple[int, int]:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT ultimo_raw_page_id, paginas_copiadas "
                    "FROM {}.migracao_raw_pages WHERE tabela = %s"
                ).format(_CONTROL),
                (tabela,),
            )
            row = cur.fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    def salvar(self, tabela: str, ultimo: int, copiadas: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.migracao_raw_pages "
                    "(tabela, ultimo_raw_page_id, paginas_copiadas) VALUES (%s, %s, %s) "
                    "ON CONFLICT (tabela) DO UPDATE SET "
                    "ultimo_raw_page_id = EXCLUDED.ultimo_raw_page_id, "
                    "paginas_copiadas = EXCLUDED.paginas_copiadas, atualizado_em = now()"
                ).format(_CONTROL),
                (tabela, ultimo, copiadas),
            )
