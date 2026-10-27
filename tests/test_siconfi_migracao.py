"""Conversão do SICONFI antigo: páginas para o raw e fila para o controle novo.

O que estes testes garantem:

- uma página antiga convertida cai na mesma key e tem o mesmo raw que a DAG nova
  gravaria para a mesma página, mais o ``_migrado_de``;
- a pasta é a do run (data do ``run_id``), não a da coleta: páginas de um run
  que passou da meia-noite continuam na mesma pasta;
- a conversão continua de onde parou, só avança o progresso depois de gravar o
  lote inteiro e respeita o limite de páginas e o prazo;
- a conferência compara a origem com os arquivos que existem no raw, e acusa
  quando uma tabela dada como copiada não tem os arquivos;
- a fila antiga vira unidades e eventos do controle novo sem perder o estado:
  o que já foi baixado não volta a ser pendente;
- a origem só é lida: nenhum comando de escrita no Postgres antigo.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import siconfi_migracao
import siconfi_storage
from cliente_siconfi import SiconfiPage, request_hash
from siconfi_migracao import (
    avaliar_fila,
    avaliar_tabela,
    chaves_divergentes,
    conexao_origem,
    converter_fila,
    data_do_run,
    envelope_da_linha,
    exigir_sem_duplicatas,
    migrar_endpoint,
    paginas_dos_runs,
    pendencias,
    status_da_fila_antiga,
    tabela_antiga,
    unidade_e_evento,
)
from siconfi_storage import persist_page

_BRT = timezone(timedelta(hours=-3))
_RUN = "scheduled__2026-09-28T13:00:00+00:00"
_DATA_DO_RUN = datetime(2026, 9, 28, 13, 0, tzinfo=timezone.utc)
# Coletada no dia seguinte ao do run, em UTC; o Postgres devolve no fuso dele.
_COLETA_UTC = datetime(2026, 9, 29, 0, 30, tzinfo=timezone.utc)
_COLETA_BRT = _COLETA_UTC.astimezone(_BRT)
_ITEMS = [{"cod_ibge": 2506707, "conta": "RCL", "valor": 1234.5, "rotulo": None}]
_PARAMS = {"id_ente": 2506707, "nr_periodo": 6, "an_exercicio": 2025}


class _BucketFalso:
    def __init__(self) -> None:
        self.objetos: dict[str, bytes] = {}

    def gravar_bytes(self, conteudo: bytes, key: str) -> str:
        self.objetos[key] = conteudo
        return key

    def listar_objetos(self, prefixo: str) -> list[dict[str, Any]]:
        return [{"Key": key} for key in self.objetos if key.startswith(prefixo)]


@pytest.fixture
def bucket() -> Any:
    falso = _BucketFalso()
    lake = siconfi_storage.datalakehouse
    with (
        patch.object(lake, "gravar_bytes", falso.gravar_bytes),
        patch.object(lake, "listar_objetos", falso.listar_objetos),
    ):
        yield falso


def _linha(raw_page_id: int = 41, **muda: Any) -> dict[str, Any]:
    """Uma linha de ``raw_pages_rreo`` como o psycopg2 devolve."""
    linha = {
        "raw_page_id": raw_page_id,
        "endpoint": "rreo",
        "request_hash": request_hash("rreo", _PARAMS),
        "request_params": _PARAMS,
        "page_offset": 0,
        "fetched_at": _COLETA_BRT,
        "run_id": _RUN,
        "response_headers": {"content-type": "application/json"},
        "payload": {"items": _ITEMS, "hasMore": False, "offset": 0},
        "item_count": len(_ITEMS),
    }
    return {**linha, **muda}


def _page() -> SiconfiPage:
    return SiconfiPage(
        endpoint="rreo",
        params=_PARAMS,
        offset=0,
        items=_ITEMS,
        has_more=False,
        headers={"content-type": "application/json"},
        payload={"items": _ITEMS, "hasMore": False, "offset": 0},
    )


# ── coerência com a DAG nova ─────────────────────────────────────────────


def test_pagina_convertida_e_igual_a_que_a_dag_gravaria(bucket: _BucketFalso) -> None:
    persist_page(_page(), _RUN, _DATA_DO_RUN, fetched_at=_COLETA_UTC)
    da_dag = dict(bucket.objetos)
    bucket.objetos.clear()

    siconfi_migracao.gravar_linha(_linha())
    convertida = dict(bucket.objetos)

    assert set(convertida) == set(da_dag)
    (key,) = da_dag
    assert key.startswith("raw/")
    raw_dag, raw_conv = json.loads(da_dag[key]), json.loads(convertida[key])
    assert raw_conv.pop("_migrado_de") == "siconfi_bronze.raw_pages_rreo#41"
    assert raw_conv == raw_dag


def test_fetched_at_sai_em_utc_qualquer_que_seja_o_fuso_do_banco() -> None:
    assert envelope_da_linha(_linha())["fetched_at"] == "2026-09-29T00:30:00+00:00"


def test_pasta_e_a_do_run_mesmo_coletada_no_dia_seguinte() -> None:
    assert data_do_run(_RUN, _COLETA_BRT).date() == _DATA_DO_RUN.date()


def test_run_id_sem_data_usa_a_coleta_em_utc() -> None:
    assert data_do_run("backfill_x", _COLETA_BRT) == _COLETA_UTC


@pytest.mark.parametrize(
    ("endpoint", "tabela"),
    [("anexos-relatorios", "raw_pages_anexos_relatorios"), ("dca", "raw_pages_dca")],
)
def test_tabela_antiga_de_cada_endpoint(endpoint: str, tabela: str) -> None:
    assert tabela_antiga(endpoint) == tabela


def test_origem_so_e_lida() -> None:
    comandos = [str(s).upper() for s in siconfi_migracao._SQL_ORIGEM]
    for comando in comandos:
        assert "SELECT" in comando
        for escrita in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"):
            assert escrita not in comando


def test_conexao_com_a_origem_e_so_de_leitura() -> None:
    """O Postgres recusa qualquer escrita numa sessão ``readonly``: mesmo que um
    SQL de escrita entrasse por engano, ele falharia na origem."""
    conn = MagicMock()
    with patch.object(siconfi_migracao.psycopg2, "connect", return_value=conn):
        with conexao_origem("dbname=x"):
            pass
    conn.set_session.assert_called_once_with(readonly=True, autocommit=True)
    conn.close.assert_called_once()


class _ConexaoFalsa:
    def __init__(self, resposta: int) -> None:
        self.resposta = resposta

    def cursor(self) -> Any:
        cur = MagicMock()
        cur.__enter__.return_value.fetchone.return_value = (self.resposta,)
        return cur


def test_tabela_com_chave_repetida_nao_e_convertida() -> None:
    with pytest.raises(ValueError, match="raw_pages_rreo: 2 chaves"):
        exigir_sem_duplicatas(_ConexaoFalsa(2), "rreo")
    exigir_sem_duplicatas(_ConexaoFalsa(0), "rreo")


# ── retomada, limite e prazo ─────────────────────────────────────────────


class _OrigemFalsa:
    def __init__(self, ids: list[int]) -> None:
        self.linhas = [_linha(i, page_offset=i) for i in ids]

    def ler_lote(self, depois_de: int, limite: int) -> list[dict[str, Any]]:
        return [linha for linha in self.linhas if linha["raw_page_id"] > depois_de][
            :limite
        ]


class _ProgressoFalso:
    def __init__(self) -> None:
        self.estado: dict[str, tuple[int, int]] = {}
        self.fila = ("", "", 0)

    def ler(self, tabela: str) -> tuple[int, int]:
        return self.estado.get(tabela, (0, 0))

    def salvar(self, tabela: str, ultimo: int, copiadas: int) -> None:
        self.estado[tabela] = (ultimo, copiadas)

    def ler_fila(self) -> tuple[str, str, int]:
        return self.fila

    def salvar_fila(self, endpoint: str, work_key: str, convertidas: int) -> None:
        self.fila = (endpoint, work_key, convertidas)


def test_continua_de_onde_parou_e_respeita_o_limite(bucket: _BucketFalso) -> None:
    origem, progresso = _OrigemFalsa([3, 7, 8, 12, 20]), _ProgressoFalso()

    primeira = migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=3)
    assert primeira["copiadas_neste_run"] == 3
    assert progresso.ler("raw_pages_rreo") == (8, 3)

    segunda = migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=None)
    assert segunda["copiadas_neste_run"] == 2
    assert segunda["terminou"]
    assert progresso.ler("raw_pages_rreo") == (20, 5)
    assert len(bucket.objetos) == 5


def test_progresso_so_avanca_depois_do_lote_inteiro(bucket: _BucketFalso) -> None:
    origem, progresso = _OrigemFalsa([1, 2, 3, 4]), _ProgressoFalso()
    original = siconfi_migracao.gravar_linha

    def falha_no_id_4(linha: dict[str, Any]) -> str:
        if linha["raw_page_id"] == 4:
            raise ConnectionError("MinIO caiu")
        return original(linha)

    with (
        patch.object(siconfi_migracao, "gravar_linha", falha_no_id_4),
        pytest.raises(ConnectionError),
    ):
        migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=None)
    assert progresso.ler("raw_pages_rreo") == (2, 2)

    migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=None)
    assert progresso.ler("raw_pages_rreo") == (4, 4)


def test_prazo_esgotado_para_antes_do_proximo_lote(bucket: _BucketFalso) -> None:
    origem, progresso = _OrigemFalsa([1, 2, 3]), _ProgressoFalso()
    resumo = migrar_endpoint(
        origem.ler_lote, progresso, "rreo", lote=1, limite=None, prazo_s=0
    )
    assert resumo["copiadas_neste_run"] == 0
    assert not resumo["terminou"]


# ── conferência contra o raw ─────────────────────────────────────────────


@pytest.mark.parametrize(
    ("origem", "distintas", "copiadas", "objetos", "situacao"),
    [
        (10, 10, 10, 10, "completo"),
        (10, 10, 4, 4, "em_andamento"),  # parou pelo limite ou pelo prazo
        (10, 10, 10, 9, "inconsistente"),  # página perdida
        (10, 9, 10, 9, "inconsistente"),  # chave repetida virou um objeto só
        (0, 0, 0, 0, "completo"),  # tabela vazia
    ],
)
def test_situacao_de_cada_tabela(
    origem: int, distintas: int, copiadas: int, objetos: int, situacao: str
) -> None:
    assert avaliar_tabela(origem, distintas, copiadas, objetos)["situacao"] == situacao


def test_objetos_contados_so_nas_pastas_dos_runs_antigos(bucket: _BucketFalso) -> None:
    siconfi_migracao.gravar_linha(_linha(1, page_offset=0))
    siconfi_migracao.gravar_linha(_linha(2, page_offset=5000))
    # Um arquivo da DAG nova, em outro run, não entra na conta.
    persist_page(_page(), "manual__2026-10-21T10:00:00+00:00", _DATA_DO_RUN)

    assert paginas_dos_runs("rreo", [(_RUN, _COLETA_BRT, 2)]) == 2


def _linha_grande(raw_page_id: int = 7) -> dict[str, Any]:
    """Uma página de ~2,6 MB, como as maiores da MSC orçamentária."""
    items = [
        {"conta": f"{i:09d}", "valor": i * 1.5, "rotulo": "x" * 480} for i in range(5000)
    ]
    return _linha(
        raw_page_id,
        payload={"items": items, "hasMore": True, "offset": 0},
        item_count=len(items),
    )


def test_pagina_grande_vira_partes_e_conta_como_uma(bucket: _BucketFalso) -> None:
    keys = siconfi_migracao.gravar_linha(_linha_grande())

    assert len(keys) == 3 and keys == sorted(keys)
    assert all(
        len(bucket.objetos[k]) <= siconfi_storage.datalakehouse.LIMITE_OBJETO_BYTES
        for k in keys
    )
    assert paginas_dos_runs("rreo", [(_RUN, _COLETA_BRT, 1)]) == 1


def test_pagina_com_parte_faltando_nao_conta(bucket: _BucketFalso) -> None:
    keys = siconfi_migracao.gravar_linha(_linha_grande())
    del bucket.objetos[keys[1]]

    assert paginas_dos_runs("rreo", [(_RUN, _COLETA_BRT, 1)]) == 0
    assert avaliar_tabela(1, 1, 1, 0)["situacao"] == "inconsistente"


# ── fila ─────────────────────────────────────────────────────────────────


def _fila(status: str, work_key: str = "k", endpoint: str = "rreo") -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "work_key": work_key,
        "params": {"id_ente": 1, "an_exercicio": 2025},
        "revision_marker": "rev",
        "status": status,
        "last_error": "erro" if status != "success" else None,
        "last_success_at": datetime(2026, 9, 30, tzinfo=timezone.utc),
        "next_attempt_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 9, 29, tzinfo=timezone.utc),
    }


@pytest.mark.parametrize("status", ["success", "no_data", "permanent_error"])
def test_estado_final_antigo_vira_evento_final(status: str) -> None:
    unidade, evento = unidade_e_evento(_fila(status))
    assert evento is not None
    assert evento["status"] == status
    assert evento["revision_marker"] == unidade["revision_marker"] == "rev"
    assert evento["next_attempt_at"] is None
    assert json.loads(unidade["params"]) == {"an_exercicio": 2025, "id_ente": 1}


@pytest.mark.parametrize("status", ["pending", "running"])
def test_pendente_e_em_andamento_continuam_pendentes(status: str) -> None:
    _, evento = unidade_e_evento(_fila(status))
    assert evento is None


def test_retry_antigo_mantem_a_proxima_tentativa() -> None:
    _, evento = unidade_e_evento(_fila("retry"))
    assert evento is not None and evento["status"] == "retry"
    assert evento["next_attempt_at"] == datetime(2026, 10, 1, tzinfo=timezone.utc)


def test_contagem_antiga_nos_estados_novos() -> None:
    antiga = {"success": 3, "pending": 5, "running": 2, "retry": 1}
    assert status_da_fila_antiga(antiga) == {"success": 3, "pending": 7, "retry": 1}


class _ControleFalso:
    def __init__(self) -> None:
        self.unidades: list[dict[str, Any]] = []
        self.eventos: list[tuple[str, dict[str, Any]]] = []

    def insert_units(self, units: list[dict[str, Any]]) -> None:
        self.unidades.extend(units)

    def record_events(self, endpoint: str, events: list, run_id: str) -> None:
        self.eventos.extend((endpoint, dict(e)) for e in events)


def test_fila_e_convertida_em_lotes_e_continua_de_onde_parou() -> None:
    linhas = [
        _fila("success", "a", "dca"),
        _fila("pending", "b", "rgf"),
        _fila("no_data", "c", "rreo"),
    ]

    def ler(endpoint: str, work_key: str, limite: int) -> list[dict[str, Any]]:
        depois = [
            r for r in linhas if (r["endpoint"], r["work_key"]) > (endpoint, work_key)
        ]
        return sorted(depois, key=lambda r: (r["endpoint"], r["work_key"]))[:limite]

    controle, progresso = _ControleFalso(), _ProgressoFalso()
    primeira = converter_fila(ler, controle, progresso, lote=2, prazo_s=None)

    assert primeira["total_convertidas"] == 3 and primeira["terminou"]
    assert [u["work_key"] for u in controle.unidades] == ["a", "b", "c"]
    assert sorted((ep, ev["work_key"]) for ep, ev in controle.eventos) == [
        ("dca", "a"),
        ("rreo", "c"),
    ]
    assert progresso.fila == ("rreo", "c", 3)

    segunda = converter_fila(ler, controle, progresso, lote=2)
    assert segunda["convertidas_neste_run"] == 0
    assert len(controle.unidades) == 3


_ANTIGA = {"success": 3, "pending": 5, "running": 2}


@pytest.mark.parametrize(
    ("convertidas", "nova", "situacao"),
    [
        (10, {"success": 3, "pending": 7}, "completo"),
        # A ingestão nova baixou e planejou depois: estados mudam, nada falta.
        (10, {"success": 6, "pending": 9, "no_data": 1}, "completo"),
        (4, {"success": 3, "pending": 1}, "em_andamento"),
        # Conversão dada como terminada, mas o controle perdeu unidades.
        (10, {"success": 3, "pending": 5}, "inconsistente"),
    ],
)
def test_situacao_da_fila_convertida(
    convertidas: int, nova: dict[str, int], situacao: str
) -> None:
    resultado = avaliar_fila(_ANTIGA, convertidas, nova)
    assert resultado["situacao"] == situacao
    assert resultado["antiga"] == {"success": 3, "pending": 7}


def test_chave_antiga_igual_ao_request_hash_de_hoje() -> None:
    """É o que impede a ingestão nova de baixar de novo o que já foi baixado."""
    params = {"id_ente": 1, "an_exercicio": 2025}
    certa = {
        "endpoint": "rreo",
        "work_key": request_hash("rreo", params),
        "params": params,
    }
    errada = {"endpoint": "rreo", "work_key": "outra-coisa", "params": params}
    assert chaves_divergentes([certa]) == []
    assert chaves_divergentes([certa, errada]) == ["outra-coisa"]


def test_chave_divergente_torna_a_fila_inconsistente() -> None:
    nova = {"success": 3, "pending": 7}
    resultado = avaliar_fila(_ANTIGA, 10, nova, chaves_conferidas=200, divergentes=1)
    assert resultado["situacao"] == "inconsistente"
    assert resultado["chaves_divergentes"] == 1


def test_pendencias_sao_so_o_que_esta_em_andamento() -> None:
    relatorio = {
        "raw_pages_rreo": {"situacao": "completo"},
        "raw_pages_rgf": {"situacao": "em_andamento"},
        "raw_pages_dca": {"situacao": "ausente"},
        "fila": {"situacao": "em_andamento"},
    }
    assert pendencias(relatorio) == ["fila", "raw_pages_rgf"]
    assert pendencias({"fila": {"situacao": "completo"}}) == []
