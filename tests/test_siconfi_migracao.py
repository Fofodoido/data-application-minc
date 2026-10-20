"""Conversão das páginas antigas do Postgres para o raw e o staging do datalakehouse.

O que estes testes garantem:

- uma página antiga convertida cai na mesma key e gera o mesmo staging que a
  DAG nova gravaria para a mesma página -- a ponte e a bronze não distinguem
  as duas origens, só o campo ``_migrado_de`` do raw diz de onde veio;
- a pasta é a do run (data do ``run_id``), não a da coleta: páginas de um run
  que passou da meia-noite continuam na mesma pasta;
- a conversão continua de onde parou, só avança o progresso depois de gravar o
  lote inteiro e respeita o limite de páginas e o prazo;
- a origem só é lida: nenhum comando de escrita no ``siconfi_bronze``.
"""

import io
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

import siconfi_migracao
import siconfi_storage
from cliente_siconfi import SiconfiPage, request_hash
from siconfi_migracao import (
    data_do_run,
    envelope_da_linha,
    migrar_endpoint,
    tabela_antiga,
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


@pytest.fixture
def bucket() -> Any:
    falso = _BucketFalso()
    with patch.object(siconfi_storage.datalakehouse, "gravar_bytes", falso.gravar_bytes):
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
    key_raw = next(k for k in da_dag if k.startswith("raw/"))
    key_staging = next(k for k in da_dag if k.startswith("staging/"))

    raw_dag, raw_conv = json.loads(da_dag[key_raw]), json.loads(convertida[key_raw])
    assert raw_conv.pop("_migrado_de") == "siconfi_bronze.raw_pages_rreo#41"
    assert raw_conv == raw_dag
    pd.testing.assert_frame_equal(
        pd.read_parquet(io.BytesIO(convertida[key_staging])),
        pd.read_parquet(io.BytesIO(da_dag[key_staging])),
    )


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
    comandos = " ".join(str(s) for s in siconfi_migracao._SQL_ORIGEM).upper()
    assert "SELECT" in comandos
    for escrita in ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"):
        assert escrita not in comandos


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

    def ler(self, tabela: str) -> tuple[int, int]:
        return self.estado.get(tabela, (0, 0))

    def salvar(self, tabela: str, ultimo: int, copiadas: int) -> None:
        self.estado[tabela] = (ultimo, copiadas)


def test_continua_de_onde_parou_e_respeita_o_limite(bucket: _BucketFalso) -> None:
    origem, progresso = _OrigemFalsa([3, 7, 8, 12, 20]), _ProgressoFalso()

    primeira = migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=3)
    assert primeira["copiadas_neste_run"] == 3
    assert progresso.ler("raw_pages_rreo") == (8, 3)

    segunda = migrar_endpoint(origem.ler_lote, progresso, "rreo", lote=2, limite=None)
    assert segunda["copiadas_neste_run"] == 2
    assert segunda["terminou"]
    assert progresso.ler("raw_pages_rreo") == (20, 5)
    assert len([k for k in bucket.objetos if k.startswith("raw/")]) == 5


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
