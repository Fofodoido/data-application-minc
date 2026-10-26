"""Controle da ingestão SICONFI pelo Trino: só inserção, como no SALIC v2 e no Mapas.

O que estes testes garantem:

- o controle nunca atualiza nem apaga linha: todo SQL de escrita é ``CREATE`` ou
  ``INSERT`` (o Trino não tem ``FOR UPDATE SKIP LOCKED`` nem ``ON CONFLICT``);
- tudo sai pelo catálogo e schema configurados, em SQL do Trino: nada de
  ``system.query`` nem ``system.execute``, que o Trino do servidor nega;
- replanejar uma unidade com a mesma revisão não a põe de novo na fila, e uma
  revisão nova põe;
- os literais escapam aspas e os instantes saem em UTC.

Estes testes olham o **texto** do SQL, com um Trino falso: não dizem se a fila
pendente ou o estado vigente estão certos. A semântica das consultas é conferida
executando-as num Trino de verdade, com as mesmas permissões do servidor, em
``test_siconfi_controle_trino.py`` (roda com ``SICONFI_TEST_TRINO`` definida).
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

import pytest

from cliente_siconfi import request_hash
from siconfi_controle import SiconfiControle, instante, params_json, texto


class _TrinoFalso:
    """Guarda o SQL e responde às leituras com o que o teste mandar."""

    def __init__(self, respostas: list[list[Sequence[Any]]] | None = None) -> None:
        self.executados: list[str] = []
        self.lidos: list[str] = []
        self.respostas = list(respostas or [])

    def run(self, sql: str) -> None:
        self.executados.append(sql)

    def records(self, sql: str) -> list[Sequence[Any]]:
        self.lidos.append(sql)
        return self.respostas.pop(0) if self.respostas else []


def _controle(respostas: Any = None) -> tuple[SiconfiControle, _TrinoFalso]:
    trino = _TrinoFalso(respostas)
    return SiconfiControle(trino, catalog="dw", schema="control"), trino


def _todos_os_metodos(controle: SiconfiControle) -> None:
    controle.ensure_tables()
    controle.enqueue_many("rreo", [({"id_ente": 1}, "r1")])
    controle.pending("rreo", 10, {"an_exercicio": [2025]})
    controle.record_events(
        "rreo", [{"work_key": "k", "revision_marker": "r", "status": "success"}], "run"
    )
    controle.insert_units(
        [
            {
                "endpoint": "rreo",
                "work_key": "k",
                "params": "{}",
                "planned_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
            }
        ]
    )
    controle.latest_status_counts()
    controle.reference_due("entes", 168)
    controle.mark_reference_refreshed("entes", "raw/siconfi/entes/x/", "run")
    controle.reference_prefix("entes")
    controle.planned_keys("extrato_entregas", "abc")
    controle.mark_planned("extrato_entregas", "abc", ["raw/a.json"])


def test_controle_so_cria_e_insere() -> None:
    controle, trino = _controle()
    _todos_os_metodos(controle)
    for sql in trino.executados:
        assert sql.split()[0] in {"CREATE", "INSERT"}, sql
    for sql in trino.executados + trino.lidos:
        for proibido in ("UPDATE ", "DELETE ", "MERGE ", "ON CONFLICT", "FOR UPDATE"):
            assert proibido not in sql.upper(), sql


def test_nada_vai_como_sql_nativo_ao_postgres() -> None:
    """O Trino do servidor nega ``system.query`` e ``system.execute`` ao usuário
    do Airflow (``Access Denied``, 26/10/2026)."""
    controle, trino = _controle()
    _todos_os_metodos(controle)
    for sql in trino.executados + trino.lidos:
        for proibido in ("system.query", "system.execute", "CALL ", "::", "ANY("):
            assert proibido not in sql, sql


def test_tabelas_saem_no_catalogo_e_schema_configurados() -> None:
    controle, trino = _controle()
    controle.ensure_tables()
    assert trino.executados[0] == 'CREATE SCHEMA IF NOT EXISTS "dw"."control"'
    criadas = {sql.split()[5] for sql in trino.executados[1:] if "TABLE" in sql[:13]}
    assert criadas == {
        f'"dw"."control".{t}'
        for t in (
            "siconfi_work_units",
            "siconfi_work_events",
            "siconfi_reference_refresh",
            "siconfi_plan_keys",
        )
    }


def test_leitura_da_fila_filtra_no_trino() -> None:
    controle, trino = _controle([[("k1", '{"id_ente": 1}', "r1")]])
    pendentes = controle.pending("rreo", 7, {"an_exercicio": [2025, 2024]})

    (sql,) = trino.lidos
    assert sql.startswith("WITH u AS (")
    assert sql.count("row_number() OVER") == 2 and "LIMIT 7" in sql
    assert "json_extract_scalar(u.params, '$.an_exercicio') IN ('2024', '2025')" in sql
    assert "endpoint = 'rreo'" in sql
    assert pendentes == [
        {"work_key": "k1", "params": {"id_ente": 1}, "revision_marker": "r1"}
    ]


def test_replanejar_com_a_mesma_revisao_nao_volta_para_a_fila() -> None:
    mesma = {"id_ente": 1}
    nova_revisao = {"id_ente": 2}
    inedita = {"id_ente": 3}
    vigentes = [
        (request_hash("rreo", mesma), "r"),
        (request_hash("rreo", nova_revisao), "r-antiga"),
    ]
    controle, trino = _controle([vigentes])

    entraram = controle.enqueue_many(
        "rreo", [(mesma, "r"), (nova_revisao, "r-nova"), (inedita, None)]
    )

    assert entraram == 2
    (insert,) = trino.executados
    assert request_hash("rreo", mesma) not in insert
    assert request_hash("rreo", nova_revisao) in insert
    assert request_hash("rreo", inedita) in insert


def test_planejamento_le_o_endpoint_uma_vez_e_lembra_o_que_inseriu() -> None:
    """Sem índice, cada leitura varre a tabela: uma por endpoint, não por lote."""
    controle, trino = _controle([[]])
    unidades = [({"id_ente": e}, "r") for e in range(1200)]
    assert controle.enqueue_many("rreo", unidades) == 1200
    assert controle.enqueue_many("rreo", unidades) == 0
    assert controle.enqueue_many("rreo", [({"id_ente": 1}, "r2")]) == 1
    (leitura,) = trino.lidos
    assert "endpoint = 'rreo'" in leitura and " IN " not in leitura
    assert len(trino.executados) == 4  # 3 INSERTs de 500 no 1º lote, 1 no último


def test_recorte_vazio_devolve_fila_vazia_sem_consultar() -> None:
    """``"msc_months": []`` na Variable viraria ``IN ()``, erro no Trino."""
    controle, trino = _controle()
    assert controle.pending("msc_orcamentaria", 10, {"me_referencia": []}) == []
    assert trino.lidos == []


def test_nada_novo_nao_executa_insert() -> None:
    controle, trino = _controle([[(request_hash("rreo", {"id_ente": 1}), "r")]])
    assert controle.enqueue_many("rreo", [({"id_ente": 1}, "r")]) == 0
    assert trino.executados == []


def test_evento_de_retry_leva_a_proxima_tentativa_e_erro_curto() -> None:
    controle, trino = _controle()
    depois = datetime(2026, 10, 21, 15, 0, tzinfo=timezone.utc)
    controle.record_events(
        "rreo",
        [
            {
                "work_key": "k",
                "revision_marker": None,
                "status": "retry",
                "error": "x" * 5000,
                "next_attempt_at": depois,
            }
        ],
        "run",
    )
    (sql,) = trino.executados
    assert "TIMESTAMP '2026-10-21 15:00:00.000000 UTC'" in sql
    assert "'" + "x" * 2000 + "'" in sql and "x" * 2001 not in sql
    assert "CAST(NULL AS varchar)" in sql


def test_status_desconhecido_e_recusado() -> None:
    controle, _ = _controle()
    with pytest.raises(ValueError, match="status inválido"):
        controle.record_events("rreo", [{"work_key": "k", "status": "running"}], "r")


def test_insert_grande_e_quebrado_em_lotes() -> None:
    controle, trino = _controle()
    controle.mark_planned("extrato_entregas", "h", [f"k{i}" for i in range(1200)])
    assert len(trino.executados) == 3


def test_referencia_vence_quando_nao_ha_refresh_recente() -> None:
    controle, trino = _controle([[(0,)], [(1,)]])
    assert controle.reference_due("entes", 168) is True
    assert controle.reference_due("entes", 168) is False
    assert "INTERVAL '168' HOUR" in trino.lidos[0]


def test_literais_escapam_aspas_e_instantes_saem_em_utc() -> None:
    assert texto("d'água") == "'d''água'"
    assert texto(None) == "CAST(NULL AS varchar)"
    brt = datetime(2026, 10, 21, 9, 0, tzinfo=timezone(timedelta(hours=-3)))
    assert instante(brt) == "TIMESTAMP '2026-10-21 12:00:00.000000 UTC'"
    assert params_json({"b": 1, "a": 2}) == '{"a": 2, "b": 1}'


class _TrinoSemCatalogo(_TrinoFalso):
    def records(self, sql: str) -> list[Sequence[Any]]:
        raise RuntimeError("line 1:14: Catalog 'dw' not found")


def test_catalogo_inexistente_falha_cedo_e_aponta_a_variable() -> None:
    """Foi o primeiro erro no servidor: lá o catálogo não se chama ``dw``."""
    trino = _TrinoSemCatalogo()
    controle = SiconfiControle(trino, catalog="dw", schema="control")
    with pytest.raises(RuntimeError, match="siconfi_trino_target_catalog"):
        controle.ensure_tables()
    assert trino.executados == []
