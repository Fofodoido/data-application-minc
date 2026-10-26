"""Semântica do controle da ingestão SICONFI contra um Trino de verdade.

``test_siconfi_controle.py`` confere o SQL que o controle **escreve**. Aqui ele é
**executado**, num Trino com catálogo PostgreSQL e com a mesma restrição do
Trino do servidor: o usuário não pode executar a table function
``system.query`` nem a procedure ``system.execute`` (em 26/10/2026 o servidor
respondeu ``Access Denied: Cannot execute function hml_analytics.system.query``).
Uma mudança no join, no ``IS NOT DISTINCT FROM``, na regra do retry ou o uso de
SQL nativo quebra estes testes.

Rodam quando ``SICONFI_TEST_TRINO`` aponta para esse Trino (``host:porta``);
``SICONFI_TEST_TRINO_CATALOG`` dá o catálogo (padrão ``hml_analytics``). Cada
teste usa um schema próprio e o apaga no fim. Sem a variável, são pulados.

A configuração do Trino descartável está em ``tests/trino_acesso_restrito/``: o
catálogo ``hml_analytics`` aponta para um Postgres descartável, e o
``rules.json`` libera tabelas e schemas, só as funções embutidas e nenhuma
procedure. Para subir os dois e rodar::

    docker network create siconfi-teste
    docker run -d --name pg-siconfi-teste --network siconfi-teste \\
        -e POSTGRES_HOST_AUTH_METHOD=trust postgres:15-alpine
    docker run -d --name trino-siconfi-teste --network siconfi-teste \\
        -p 58080:8080 -v "$PWD/tests/trino_acesso_restrito:/etc/trino:ro" \\
        trinodb/trino:478
    export SICONFI_TEST_TRINO=localhost:58080
    poetry run pytest tests/test_siconfi_controle_trino.py

O cliente ``trino`` vem com o provider do Airflow; fora da imagem do Airflow,
``pip install trino``.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

import pytest

from cliente_siconfi import request_hash
from siconfi_controle import EVENTS, PLAN_KEYS, REFERENCE, UNITS, SiconfiControle

_TRINO = os.environ.get("SICONFI_TEST_TRINO")
_CATALOGO = os.environ.get("SICONFI_TEST_TRINO_CATALOG", "hml_analytics")

pytestmark = pytest.mark.skipif(
    not _TRINO, reason="SICONFI_TEST_TRINO não definida: precisa de um Trino"
)


class _Trino:
    """``Executor`` pelo cliente ``trino``, com o usuário que o Airflow usaria."""

    def __init__(self, endereco: str) -> None:
        import trino

        host, porta = endereco.rsplit(":", 1)
        self.conn = trino.dbapi.connect(host=host, port=int(porta), user="airflow")

    def _executar(self, sql: str) -> list[Sequence[Any]]:
        cur = self.conn.cursor()
        cur.execute(sql)
        return list(cur.fetchall())

    def run(self, sql: str) -> None:
        self._executar(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        return self._executar(sql)


@pytest.fixture
def controle() -> Iterator[SiconfiControle]:
    executor = _Trino(str(_TRINO))
    schema = f"siconfi_teste_{uuid.uuid4().hex[:10]}"
    try:
        c = SiconfiControle(executor, catalog=_CATALOGO, schema=schema)
        c.ensure_tables()
        yield c
    finally:
        for tabela in (UNITS, EVENTS, REFERENCE, PLAN_KEYS):
            executor.run(f'DROP TABLE IF EXISTS "{_CATALOGO}"."{schema}".{tabela}')
        executor.run(f'DROP SCHEMA IF EXISTS "{_CATALOGO}"."{schema}"')


def test_o_trino_de_teste_nega_o_sql_nativo_como_o_servidor() -> None:
    """Sem isto, os outros testes passariam num Trino sem restrição."""
    executor = _Trino(str(_TRINO))
    with pytest.raises(Exception, match="Access Denied"):
        executor.records(
            f"SELECT * FROM TABLE({_CATALOGO}.system.query(query => 'SELECT 1'))"
        )
    with pytest.raises(Exception, match="Access Denied"):
        executor.run(f"CALL {_CATALOGO}.system.execute(query => 'SELECT 1')")


def test_catalogo_inexistente_falha_com_o_nome_da_variable() -> None:
    c = SiconfiControle(_Trino(str(_TRINO)), catalog="nao_existe", schema="control")
    with pytest.raises(RuntimeError, match="siconfi_trino_target_catalog"):
        c.ensure_tables()


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


def test_replanejar_revisao_nula_nao_duplica(controle: SiconfiControle) -> None:
    assert controle.enqueue_many("rreo", [(_p(1), None)]) == 1
    assert controle.enqueue_many("rreo", [(_p(1), None)]) == 0
    assert controle.enqueue_many("rreo", [(_p(1), "r")]) == 1


def test_filtro_do_recorte_e_limite(controle: SiconfiControle) -> None:
    controle.enqueue_many(
        "rreo", [(_p(1, 2024), None), (_p(2, 2025), None), (_p(3, 2025), None)]
    )
    assert _pendentes(controle, an_exercicio=[2025]) == [2, 3]
    assert _pendentes(controle, an_exercicio=[2024, 2025], id_ente=[1, 3]) == [1, 3]
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
