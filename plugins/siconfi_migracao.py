"""Conversão única do SICONFI antigo para o raw do datalakehouse e o controle novo.

Até outubro de 2026 a ingestão gravava cada página em
``siconfi_bronze.raw_pages_<endpoint>`` e a fila em ``siconfi_control.work_queue``,
os dois no Postgres. Esta conversão:

- **páginas**: lê as tabelas antigas, sem escrever nelas, e grava cada página no
  raw pelo mesmo ``siconfi_storage.gravar_envelope`` que a DAG de ingestão usa.
  O conteúdo é o da DAG nova mais o campo ``_migrado_de``
  (``siconfi_bronze.raw_pages_rreo#123``). A ordem das chaves e a forma dos
  números seguem o que o JSONB guardou, que não é byte a byte o que a API
  mandou. A pasta é a do run original, pela data no ``run_id``. As tabelas
  ``*_items`` não entram: só repetiam o conteúdo das páginas.
- **fila**: copia as unidades e o estado delas para o controle novo
  (``siconfi_controle``, pelo Trino), para a DAG nova não refazer o que já foi
  baixado.

**Exceção ao ADR 0009.** A leitura da origem e o progresso usam ``psycopg2``: é
a única forma de ler o Postgres antigo, e a conversão roda uma vez, no ambiente
atual, antes da mudança do Airflow para o Serpro. A ingestão não usa nada disto.

O progresso fica em ``siconfi_control.migracao_raw_pages`` e
``siconfi_control.migracao_fila`` e só avança depois que o lote inteiro foi
gravado. Interrompida, a conversão recomeça do último lote completo; regravar
uma página cai na mesma key.
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, Mapping, Protocol

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as Connection
from psycopg2.extras import RealDictCursor

import datalakehouse
from siconfi_controle import FINAL_STATUSES, SiconfiControle, params_json
from siconfi_storage import FONTE, VALID_ENDPOINTS, entidade, gravar_envelope

ORIGEM_SCHEMA = "siconfi_bronze"
# Schema do controle antigo: tem a fila antiga e guarda o progresso desta conversão.
CONTROLE_ANTIGO = "siconfi_control"
ENDPOINTS = tuple(sorted(VALID_ENDPOINTS))
RUN_ID_FILA = "migracao_fila"
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
_COLUNAS_FILA = (
    "endpoint",
    "work_key",
    "params",
    "revision_marker",
    "status",
    "last_error",
    "last_success_at",
    "next_attempt_at",
    "updated_at",
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
_SQL_DUPLICATAS = sql.SQL(
    "SELECT count(*) FROM (SELECT 1 FROM {schema}.{tabela} "
    "GROUP BY run_id, request_hash, page_offset HAVING count(*) > 1) d"
)
_SQL_RUNS = sql.SQL(
    "SELECT run_id, min(fetched_at), count(DISTINCT (request_hash, page_offset)) "
    "FROM {schema}.{tabela} GROUP BY run_id"
)
_SQL_LOTE_FILA = sql.SQL(
    "SELECT {colunas} FROM {schema}.work_queue "
    "WHERE (endpoint, work_key) > (%s, %s) ORDER BY endpoint, work_key LIMIT %s"
)
_SQL_STATUS_FILA = sql.SQL(
    "SELECT status, count(*) FROM {schema}.work_queue GROUP BY status"
)
# Tudo o que a conversão manda para a origem. O teste confere que é só leitura.
_SQL_ORIGEM = (
    _SQL_LOTE,
    _SQL_EXISTE,
    _SQL_TOTAL,
    _SQL_DUPLICATAS,
    _SQL_RUNS,
    _SQL_LOTE_FILA,
    _SQL_STATUS_FILA,
)

_CONTROL = sql.Identifier(CONTROLE_ANTIGO)
_DDL_PROGRESSO = [
    sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {control}.migracao_raw_pages (
            tabela TEXT PRIMARY KEY,
            ultimo_raw_page_id BIGINT NOT NULL,
            paginas_copiadas BIGINT NOT NULL,
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    ).format(control=_CONTROL),
    sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {control}.migracao_fila (
            fila TEXT PRIMARY KEY,
            ultimo_endpoint TEXT NOT NULL,
            ultimo_work_key TEXT NOT NULL,
            convertidas BIGINT NOT NULL,
            atualizado_em TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    ).format(control=_CONTROL),
]


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
    """Grava uma página antiga no raw; devolve a key."""
    return gravar_envelope(
        envelope_da_linha(linha), data_do_run(linha["run_id"], linha["fetched_at"])
    )


# ── páginas ──────────────────────────────────────────────────────────────


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
    paralelo: int = 4,
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


def avaliar_tabela(
    paginas_na_origem: int,
    distintas_na_origem: int,
    copiadas: int,
    objetos_no_raw: int,
) -> dict[str, Any]:
    """Situação de uma tabela: ``completo``, ``em_andamento`` ou ``inconsistente``.

    ``inconsistente`` é a cópia dada como terminada sem os arquivos esperados no
    raw: página perdida, chave repetida que virou um objeto só, ou bucket errado.
    Parar pelo limite ou pelo prazo não é inconsistência: é ``em_andamento``.
    """
    if copiadas < paginas_na_origem:
        situacao = "em_andamento"
    elif objetos_no_raw == distintas_na_origem == paginas_na_origem:
        situacao = "completo"
    else:
        situacao = "inconsistente"
    return {
        "paginas_na_origem": paginas_na_origem,
        "paginas_copiadas": copiadas,
        "objetos_no_raw": objetos_no_raw,
        "faltam": paginas_na_origem - copiadas,
        "situacao": situacao,
    }


def objetos_dos_runs(endpoint: str, runs: list[tuple[str, datetime, int]]) -> int:
    """Quantos arquivos existem no raw nas pastas dos runs antigos de ``endpoint``.

    A DAG nova nunca grava numa pasta de run antigo, então o que estiver nessas
    pastas veio da conversão.
    """
    total = 0
    for run_id, min_fetched_at, _ in runs:
        prefixo = datalakehouse.prefixo_do_run(
            datalakehouse.CAMADA_RAW,
            FONTE,
            entidade(endpoint),
            run_id,
            data_do_run(run_id, min_fetched_at),
        )
        total += sum(
            1 for o in datalakehouse.listar_objetos(prefixo) if o["Key"].endswith(".json")
        )
    return total


# ── fila ─────────────────────────────────────────────────────────────────


def unidade_e_evento(
    linha: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Uma linha da fila antiga no formato do controle novo.

    ``success``, ``no_data`` e ``permanent_error`` viram evento final; ``retry``
    vira evento de retry com o mesmo ``next_attempt_at``; ``pending`` e ``running``
    ficam sem evento, ou seja, pendentes.
    """
    params = linha["params"]
    unidade = {
        "endpoint": linha["endpoint"],
        "work_key": linha["work_key"],
        "params": params if isinstance(params, str) else params_json(params),
        "revision_marker": linha["revision_marker"],
        "planned_at": linha["updated_at"],
    }
    status = linha["status"]
    if status not in FINAL_STATUSES and status != "retry":
        return unidade, None
    evento = {
        "work_key": linha["work_key"],
        "revision_marker": linha["revision_marker"],
        "status": status,
        "error": linha["last_error"],
        "next_attempt_at": linha["next_attempt_at"] if status == "retry" else None,
        "event_at": linha["last_success_at"] or linha["updated_at"],
    }
    return unidade, evento


class ProgressoFila(Protocol):
    def ler_fila(self) -> tuple[str, str, int]: ...

    def salvar_fila(self, endpoint: str, work_key: str, convertidas: int) -> None: ...


def converter_fila(
    ler_lote: Callable[[str, str, int], list[dict[str, Any]]],
    controle: SiconfiControle,
    progresso: ProgressoFila,
    lote: int = 1000,
    prazo_s: float | None = None,
) -> dict[str, Any]:
    """Copia a fila antiga para o controle novo, a partir de onde parou."""
    endpoint, work_key, convertidas = progresso.ler_fila()
    prazo = None if prazo_s is None else time.monotonic() + prazo_s
    neste_run, terminou = 0, False
    while prazo is None or time.monotonic() < prazo:
        linhas = ler_lote(endpoint, work_key, lote)
        if not linhas:
            terminou = True
            break
        unidades, eventos = [], defaultdict(list)
        for linha in linhas:
            unidade, evento = unidade_e_evento(linha)
            unidades.append(unidade)
            if evento:
                eventos[linha["endpoint"]].append(evento)
        controle.insert_units(unidades)
        for ep, evs in eventos.items():
            controle.record_events(ep, evs, RUN_ID_FILA)
        endpoint, work_key = linhas[-1]["endpoint"], linhas[-1]["work_key"]
        convertidas += len(linhas)
        neste_run += len(linhas)
        progresso.salvar_fila(endpoint, work_key, convertidas)
    return {
        "convertidas_neste_run": neste_run,
        "total_convertidas": convertidas,
        "terminou": terminou,
    }


def status_da_fila_antiga(
    contagem: Mapping[str, int],
) -> dict[str, int]:
    """A contagem antiga nos estados do controle novo (``running`` é pendente)."""
    novo: dict[str, int] = defaultdict(int)
    for status, n in contagem.items():
        novo["pending" if status in ("pending", "running") else status] += n
    return dict(novo)


# ── Postgres antigo ──────────────────────────────────────────────────────


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


def _tabela_sql(query: sql.SQL, endpoint: str) -> sql.Composed:
    composto: sql.Composed = query.format(
        schema=sql.Identifier(ORIGEM_SCHEMA),
        tabela=sql.Identifier(tabela_antiga(endpoint)),
        colunas=sql.SQL(", ").join(sql.Identifier(c) for c in _COLUNAS),
    )
    return composto


def _um(conn: Connection, query: sql.Composable, args: tuple = ()) -> Any:
    with conn.cursor() as cur:
        cur.execute(query, args)
        row = cur.fetchone()
    return row[0] if row else None


def tabela_existe(conn: Connection, endpoint: str) -> bool:
    return bool(_um(conn, _SQL_EXISTE, (f"{ORIGEM_SCHEMA}.{tabela_antiga(endpoint)}",)))


def fila_existe(conn: Connection) -> bool:
    return bool(_um(conn, _SQL_EXISTE, (f"{CONTROLE_ANTIGO}.work_queue",)))


def duplicatas_na_origem(conn: Connection, endpoint: str) -> int:
    """Chaves ``(run_id, request_hash, page_offset)`` com mais de uma página.

    Cada chave vira uma key do raw: duas páginas com a mesma chave se
    sobrescreveriam. Medido em 21/10/2026: zero nas 9 tabelas.
    """
    return int(_um(conn, _tabela_sql(_SQL_DUPLICATAS, endpoint)) or 0)


def exigir_sem_duplicatas(conn: Connection, endpoint: str) -> None:
    """Recusa converter uma tabela com chave repetida, em vez de escolher uma.

    Duas páginas com a mesma chave cairiam no mesmo arquivo do raw, e a última
    gravada venceria em silêncio. Como hoje não há nenhuma, a regra de qual vale
    fica para quando houver, decidida por gente e não pela ordem das threads.
    """
    repetidas = duplicatas_na_origem(conn, endpoint)
    if repetidas:
        raise ValueError(
            f"{tabela_antiga(endpoint)}: {repetidas} chaves (run_id, request_hash, "
            "page_offset) com mais de uma página. Cada chave é um arquivo no raw, e "
            "as páginas se sobrescreveriam. Defina qual vale antes de converter."
        )


def leitor_de_lotes(
    conn: Connection, endpoint: str
) -> Callable[[int, int], list[dict[str, Any]]]:
    query = _tabela_sql(_SQL_LOTE, endpoint)

    def ler_lote(depois_de: int, limite: int) -> list[dict[str, Any]]:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (depois_de, limite))
            return [dict(row) for row in cur.fetchall()]

    return ler_lote


def total_na_origem(conn: Connection, endpoint: str) -> tuple[int, int]:
    """Quantas páginas a tabela antiga tem, e o maior ``raw_page_id``."""
    with conn.cursor() as cur:
        cur.execute(_tabela_sql(_SQL_TOTAL, endpoint))
        row = cur.fetchone()
    return (int(row[0]), int(row[1])) if row else (0, 0)


def runs_na_origem(conn: Connection, endpoint: str) -> list[tuple[str, datetime, int]]:
    """Por run antigo: ``run_id``, primeira coleta e páginas distintas."""
    with conn.cursor() as cur:
        cur.execute(_tabela_sql(_SQL_RUNS, endpoint))
        return [(str(r), f, int(n)) for r, f, n in cur.fetchall()]


def leitor_da_fila(conn: Connection) -> Callable[[str, str, int], list[dict[str, Any]]]:
    query = _SQL_LOTE_FILA.format(
        colunas=sql.SQL(", ").join(sql.Identifier(c) for c in _COLUNAS_FILA),
        schema=_CONTROL,
    )

    def ler_lote(endpoint: str, work_key: str, limite: int) -> list[dict[str, Any]]:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (endpoint, work_key, limite))
            return [dict(row) for row in cur.fetchall()]

    return ler_lote


def contagem_da_fila_antiga(conn: Connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(_SQL_STATUS_FILA.format(schema=_CONTROL))
        return {str(s): int(n) for s, n in cur.fetchall()}


class ProgressoPostgres:
    """Progresso em ``siconfi_control.migracao_*``, no banco de controle antigo."""

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

    def garantir_tabelas(self) -> None:
        with self._cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(_CONTROL))
            for ddl in _DDL_PROGRESSO:
                cur.execute(ddl)

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

    def ler_fila(self) -> tuple[str, str, int]:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "SELECT ultimo_endpoint, ultimo_work_key, convertidas "
                    "FROM {}.migracao_fila WHERE fila = 'work_queue'"
                ).format(_CONTROL)
            )
            row = cur.fetchone()
        return (str(row[0]), str(row[1]), int(row[2])) if row else ("", "", 0)

    def salvar_fila(self, endpoint: str, work_key: str, convertidas: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "INSERT INTO {}.migracao_fila "
                    "(fila, ultimo_endpoint, ultimo_work_key, convertidas) "
                    "VALUES ('work_queue', %s, %s, %s) "
                    "ON CONFLICT (fila) DO UPDATE SET "
                    "ultimo_endpoint = EXCLUDED.ultimo_endpoint, "
                    "ultimo_work_key = EXCLUDED.ultimo_work_key, "
                    "convertidas = EXCLUDED.convertidas, atualizado_em = now()"
                ).format(_CONTROL),
                (endpoint, work_key, convertidas),
            )
