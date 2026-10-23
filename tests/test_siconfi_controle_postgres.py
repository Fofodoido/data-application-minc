"""Semântica do controle da ingestão SICONFI contra um Postgres de verdade.

``test_siconfi_controle.py`` confere o SQL que o controle **escreve**. Aqui ele é
**executado**: as consultas nativas que vão pelo ``system.query`` (a fila
pendente, o estado vigente, a busca do planejamento) rodam como estão num
Postgres, e os ``INSERT`` e ``CREATE`` do controle também. Do Trino só se tira o
envelope: o prefixo do catálogo, a table function ``system.query`` e a procedure
``system.execute``. Uma mudança no join, no ``IS NOT DISTINCT FROM`` ou na regra
do retry quebra estes testes.

Rodam quando ``SICONFI_TEST_PG_DSN`` aponta para um Postgres onde se pode criar
schema. Cada teste usa um schema próprio e o apaga no fim. Sem a variável, são
pulados. Para rodar com um Postgres descartável::

    docker run -d --name pg-teste -p 55432:5432 \\
        -e POSTGRES_HOST_AUTH_METHOD=trust postgres:15-alpine
    export SICONFI_TEST_PG_DSN=postgresql://postgres@localhost:55432/postgres
    poetry run pytest tests/test_siconfi_controle_postgres.py
"""

import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import pytest

from cliente_siconfi import request_hash
from siconfi_controle import SiconfiControle

_DSN = os.environ.get("SICONFI_TEST_PG_DSN")

pytestmark = pytest.mark.skipif(
    not _DSN, reason="SICONFI_TEST_PG_DSN não definida: precisa de um Postgres"
)

_CATALOGO = "dw"
_ENVELOPE = re.compile(
    r'^(?:SELECT \* FROM TABLE\("dw"\.system\.query|CALL "dw"\.system\.execute)'
    r"\(query => '(?P<sql>.*)'\)\)?$",
    re.DOTALL,
)


class _PostgresSemTrino:
    """``Executor`` que roda o SQL do controle direto no Postgres."""

    def __init__(self, conn: Any) -> None:
        self.conn = conn

    @staticmethod
    def _traduzir(sql: str) -> str:
        envelope = _ENVELOPE.match(sql.strip())
        if envelope:
            return envelope.group("sql").replace("''", "'")
        return sql.replace(f'"{_CATALOGO}".', "")

    def run(self, sql: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(self._traduzir(sql))

    def records(self, sql: str) -> list[Sequence[Any]]:
        with self.conn.cursor() as cur:
            cur.execute(self._traduzir(sql))
            return list(cur.fetchall())


@pytest.fixture
def controle() -> Iterator[SiconfiControle]:
    import psycopg2

    conn = psycopg2.connect(_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        # O Trino grava instantes em UTC; o literal ``TIMESTAMP '... UTC'`` vira
        # o mesmo instante no Postgres quando a sessão está em UTC.
        cur.execute("SET TIME ZONE 'UTC'")
    schema = f"siconfi_teste_{uuid.uuid4().hex[:10]}"
    executor = _PostgresSemTrino(conn)
    try:
        c = SiconfiControle(executor, catalog=_CATALOGO, schema=schema)
        c.ensure_tables()
        yield c
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def _p(ente: int, ano: int = 2025) -> dict[str, Any]:
    return {"id_ente": ente, "an_exercicio": ano}


def _evento(params: dict[str, Any], revisao: str | None, status: str, **kw: Any) -> dict:
    return {
        "work_key": request_hash("rreo", params),
        "revision_marker": revisao,
        "status": status,
        **kw,
    }


def _pendentes(c: SiconfiControle, **filtro: Any) -> list[int]:
    return sorted(u["params"]["id_ente"] for u in c.pending("rreo", 100, filtro or None))


def test_unidade_sem_evento_e_pendente(controle: SiconfiControle) -> None:
    controle.enqueue_many("rreo", [(_p(1), "r"), (_p(2), None)])
    assert _pendentes(controle) == [1, 2]


@pytest.mark.parametrize("status", ["success", "no_data", "permanent_error"])
def test_evento_final_fecha_a_unidade(controle: SiconfiControle, status: str) -> None:
    controle.enqueue_many("rreo", [(_p(1), "r"), (_p(2), "r")])
    controle.record_events("rreo", [_evento(_p(1), "r", status)], "run")
    assert _pendentes(controle) == [2]


def test_retry_espera_o_horario_e_depois_volta(controle: SiconfiControle) -> None:
    agora = datetime.now(timezone.utc)
    controle.enqueue_many("rreo", [(_p(1), "r"), (_p(2), "r")])
    controle.record_events(
        "rreo",
        [
            _evento(_p(1), "r", "retry", next_attempt_at=agora + timedelta(hours=1)),
            _evento(_p(2), "r", "retry", next_attempt_at=agora - timedelta(minutes=1)),
        ],
        "run",
    )
    assert _pendentes(controle) == [2]


def test_retry_depois_success_fecha_pelo_evento_mais_recente(
    controle: SiconfiControle,
) -> None:
    controle.enqueue_many("rreo", [(_p(1), "r")])
    antes = datetime.now(timezone.utc) - timedelta(minutes=10)
    controle.record_events(
        "rreo", [_evento(_p(1), "r", "retry", event_at=antes, next_attempt_at=antes)], "a"
    )
    assert _pendentes(controle) == [1]
    controle.record_events("rreo", [_evento(_p(1), "r", "success")], "b")
    assert _pendentes(controle) == []


def test_revisao_nova_reabre_e_replanejar_igual_nao_duplica(
    controle: SiconfiControle,
) -> None:
    assert controle.enqueue_many("rreo", [(_p(1), "r1")]) == 1
    controle.record_events("rreo", [_evento(_p(1), "r1", "success")], "run")
    assert controle.enqueue_many("rreo", [(_p(1), "r1")]) == 0
    assert _pendentes(controle) == []
    assert controle.enqueue_many("rreo", [(_p(1), "r2")]) == 1
    assert _pendentes(controle) == [1]


def test_revisao_nula_fecha_com_evento_de_revisao_nula(
    controle: SiconfiControle,
) -> None:
    """A DCA e o extrato não têm marcador de revisão: o join precisa casar NULL."""
    controle.enqueue_many("rreo", [(_p(1), None)])
    controle.record_events("rreo", [_evento(_p(1), None, "no_data")], "run")
    assert _pendentes(controle) == []


def test_filtro_do_recorte_e_limite(controle: SiconfiControle) -> None:
    controle.enqueue_many(
        "rreo", [(_p(1, 2024), None), (_p(2, 2025), None), (_p(3, 2025), None)]
    )
    assert _pendentes(controle, an_exercicio=[2025]) == [2, 3]
    assert len(controle.pending("rreo", 1)) == 1


def test_estado_vigente_por_unidade(controle: SiconfiControle) -> None:
    controle.enqueue_many("rreo", [(_p(e), "r") for e in (1, 2, 3, 4)])
    agora = datetime.now(timezone.utc)
    controle.record_events(
        "rreo",
        [
            _evento(_p(1), "r", "success"),
            _evento(_p(2), "r", "retry", next_attempt_at=agora),
            _evento(_p(3), "r", "permanent_error"),
        ],
        "run",
    )
    controle.enqueue_many("rreo", [(_p(1), "r-nova")])
    assert controle.latest_status_counts() == {
        "pending": 2,
        "retry": 1,
        "permanent_error": 1,
    }


def test_referencia_e_planejamento(controle: SiconfiControle) -> None:
    assert controle.reference_due("entes", 168)
    controle.mark_reference_refreshed("entes", "raw/siconfi/entes/a/", "run-1")
    controle.mark_reference_refreshed("entes", "raw/siconfi/entes/b/", "run-2")
    assert not controle.reference_due("entes", 168)
    assert controle.reference_prefix("entes") == "raw/siconfi/entes/b/"

    controle.mark_planned("extrato_entregas", "h1", ["a", "b"])
    controle.mark_planned("extrato_entregas", "h2", ["c"])
    assert controle.planned_keys("extrato_entregas", "h1") == {"a", "b"}


def test_indices_das_consultas_existem(controle: SiconfiControle) -> None:
    linhas = controle.executor.records(
        "SELECT tablename FROM pg_indexes "
        f"WHERE schemaname = '{controle.schema}' AND indexname LIKE '%_busca_idx'"
    )
    assert {row[0] for row in linhas} == {
        "siconfi_work_units",
        "siconfi_work_events",
        "siconfi_plan_keys",
        "siconfi_reference_refresh",
    }
