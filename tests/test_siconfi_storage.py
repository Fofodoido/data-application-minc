"""SICONFI no datalakehouse: cada página vira um JSON no raw e um Parquet no staging.

O que estes testes garantem (ADR 0009, 0010 e 0011):

- nenhuma camada é gravada no Postgres: o ``siconfi_bronze`` não aparece mais no
  código da ingestão, e o Postgres fica só com o controle (``siconfi_control``);
- o raw de uma página tem os mesmos campos da antiga ``raw_pages_<endpoint>``,
  com o ``payload`` intacto -- é o que torna 1:1 a cópia das páginas antigas;
- o staging é o raw com outro prefixo e extensão: uma linha por item, tudo
  texto, com a requisição de origem em cada linha;
- o planejamento lê o raw e gera as mesmas unidades que gerava do banco, e
  cada arquivo do extrato é planejado uma vez por recorte.
"""

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd
import pytest

import siconfi_storage
from cliente_siconfi import PlanScope, SiconfiPage, request_hash, work_units
from siconfi_storage import (
    VALID_ENDPOINTS,
    entidade,
    entity_ids_do_raw,
    envelope_da_pagina,
    itens_do_raw,
    persist_page,
    plan_from_extrato,
    raw_keys,
)

_RAIZ = Path(__file__).resolve().parents[1]
_DATA = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
_COLETA = datetime(2026, 10, 20, 9, 15, 30, tzinfo=timezone.utc)
_RUN = "scheduled__2026-10-20T09:00:00+00:00"
_RUN_NA_KEY = "scheduled__2026-10-20T09_00_00_00_00"

# Colunas da antiga siconfi_bronze.raw_pages_<endpoint>, sem o raw_page_id serial.
_COLUNAS_RAW_PAGES = {
    "endpoint",
    "request_hash",
    "request_params",
    "page_offset",
    "fetched_at",
    "run_id",
    "response_headers",
    "payload",
    "item_count",
}

_ITEM_RREO = {
    "cod_ibge": 3550308,
    "exercicio": 2024,
    "entregavel": "Relatório Resumido de Execução Orçamentária",
    "periodo": 6,
    "periodicidade": "B",
    "tipo_relatorio": "N",
    "data_status": "2025-01-30",
    "status_relatorio": "HO",
    "forma_envio": "P",
}


def _page(endpoint: str, items: list[dict[str, Any]], offset: int = 0) -> SiconfiPage:
    params = {"id_ente": 3550308, "an_exercicio": 2024}
    return SiconfiPage(
        endpoint=endpoint,
        params=params,
        offset=offset,
        items=items,
        has_more=False,
        headers={"content-type": "application/json"},
        payload={"items": items, "hasMore": False, "offset": offset, "count": len(items)},
    )


class _BucketFalso:
    """O MinIO em memória: key -> bytes."""

    def __init__(self) -> None:
        self.objetos: dict[str, bytes] = {}

    def gravar_bytes(self, conteudo: bytes, key: str) -> str:
        self.objetos[key] = conteudo
        return key

    def ler_json(self, key: str) -> Any:
        return json.loads(self.objetos[key])

    def listar_objetos(self, prefixo: str) -> list[dict[str, Any]]:
        return [{"Key": key} for key in self.objetos if key.startswith(prefixo)]


@pytest.fixture
def bucket() -> Any:
    falso = _BucketFalso()
    lake = siconfi_storage.datalakehouse
    with (
        patch.object(lake, "gravar_bytes", falso.gravar_bytes),
        patch.object(lake, "ler_json", falso.ler_json),
        patch.object(lake, "listar_objetos", falso.listar_objetos),
    ):
        yield falso


def _parquet(bucket: _BucketFalso, key: str) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(bucket.objetos[key]))


# ── nada de camada no Postgres ───────────────────────────────────────────


@pytest.mark.parametrize(
    "arquivo",
    ["plugins/siconfi_storage.py", "dags/data_ingest/siconfi/siconfi_ingestion_dag.py"],
)
def test_ingestao_nao_grava_mais_no_schema_siconfi_bronze(arquivo: str) -> None:
    assert "siconfi_bronze" not in (_RAIZ / arquivo).read_text(encoding="utf-8")


def test_postgres_so_guarda_o_schema_de_controle() -> None:
    ddl = " ".join(str(statement) for statement in siconfi_storage._DDL)
    assert "CREATE SCHEMA IF NOT EXISTS {control}" in ddl
    assert "{bronze}" not in ddl


# ── caminho ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("endpoint", "esperado"),
    [
        ("anexos-relatorios", "anexos_relatorios"),
        ("msc_orcamentaria", "msc_orcamentaria"),
    ],
)
def test_entidade_e_o_endpoint_com_sublinhado(endpoint: str, esperado: str) -> None:
    assert entidade(endpoint) == esperado


def test_nomes_de_entidade_nao_se_repetem() -> None:
    assert len({entidade(e) for e in VALID_ENDPOINTS}) == len(VALID_ENDPOINTS)


def test_endpoint_invalido_e_recusado() -> None:
    with pytest.raises(ValueError, match="inválido"):
        entidade("nao_existe")


def test_pagina_vira_um_raw_e_um_staging_no_mesmo_caminho(bucket: _BucketFalso) -> None:
    page = _page("rreo", [{"valor": 1}], offset=5000)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)

    nome = f"{request_hash('rreo', page.params)}_5000"
    pasta = f"siconfi/rreo/ano=2026/mes=10/dia=20/{_RUN_NA_KEY}/{nome}"
    assert set(bucket.objetos) == {f"raw/{pasta}.json", f"staging/{pasta}.parquet"}


def test_regravar_a_mesma_pagina_no_mesmo_run_nao_cria_outro_arquivo(
    bucket: _BucketFalso,
) -> None:
    page = _page("rreo", [{"valor": 1}])
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)
    assert len(bucket.objetos) == 2


# ── raw ──────────────────────────────────────────────────────────────────


def test_raw_tem_os_campos_da_antiga_raw_pages() -> None:
    envelope = envelope_da_pagina(_page("rgf", [{"a": 1}]), _RUN, _COLETA)
    assert set(envelope) == _COLUNAS_RAW_PAGES


def test_payload_volta_do_raw_intacto_e_com_os_tipos(bucket: _BucketFalso) -> None:
    items = [{"inteiro": 7, "decimal": 1.5, "nulo": None, "lista": [1, "a"], "t": True}]
    page = _page("msc_orcamentaria", items)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)

    (key_raw,) = raw_keys("msc_orcamentaria")
    envelope = bucket.ler_json(key_raw)
    assert envelope["payload"] == page.payload
    assert envelope["request_params"] == page.params
    assert envelope["item_count"] == 1
    assert envelope["fetched_at"] == _COLETA.isoformat()
    assert itens_do_raw(key_raw) == items


# ── staging ──────────────────────────────────────────────────────────────


def test_staging_tem_uma_linha_por_item_tudo_texto(bucket: _BucketFalso) -> None:
    items = [{"conta": "1.1", "valor": 10.5}, {"conta": "1.2", "valor": None}]
    persist_page(_page("msc_controle", items), _RUN, _DATA, fetched_at=_COLETA)

    (key,) = [k for k in bucket.objetos if k.startswith("staging/")]
    df = _parquet(bucket, key)
    assert len(df) == 2
    assert all(str(tipo) == "string" for tipo in df.dtypes)
    assert df["valor"].tolist()[0] == "10.5"
    assert pd.isna(df["valor"].tolist()[1])


def test_staging_diz_de_qual_requisicao_veio_cada_linha(bucket: _BucketFalso) -> None:
    page = _page("rreo", [{"valor": 1}], offset=10)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)

    (key,) = [k for k in bucket.objetos if k.startswith("staging/")]
    linha = _parquet(bucket, key).iloc[0]
    assert linha["_request_hash"] == request_hash("rreo", page.params)
    assert linha["_page_offset"] == "10"
    assert linha["_run_id"] == _RUN
    assert linha["_fetched_at"] == _COLETA.isoformat()
    assert json.loads(linha["_request_params"]) == page.params


def test_pagina_vazia_tambem_vai_para_o_raw_e_o_staging(bucket: _BucketFalso) -> None:
    gravados = persist_page(_page("dca", []), _RUN, _DATA, fetched_at=_COLETA)
    assert gravados == 0
    assert sorted(k.split("/")[0] for k in bucket.objetos) == ["raw", "staging"]


# ── planejamento a partir do raw ─────────────────────────────────────────


def test_item_lido_do_raw_gera_as_mesmas_unidades(bucket: _BucketFalso) -> None:
    """O marcador de revisão é ``str()`` de campos do item: se o tipo mudasse
    (o que aconteceria lendo o staging, tudo texto), unidades já concluídas
    voltariam para ``pending``."""
    persist_page(_page("extrato_entregas", [_ITEM_RREO]), _RUN, _DATA)
    (key,) = raw_keys("extrato_entregas")
    assert [work_units(i) for i in itens_do_raw(key)] == [work_units(_ITEM_RREO)]


def test_entes_vem_do_raw_do_ultimo_refresh(bucket: _BucketFalso) -> None:
    entes = [{"cod_ibge": 3550308}, {"cod_ibge": "53"}, {"nome": "sem código"}]
    persist_page(_page("entes", entes), _RUN, _DATA)
    persist_page(_page("entes", [{"cod_ibge": 99}]), "outro-run", _DATA)

    prefixo = f"raw/siconfi/entes/ano=2026/mes=10/dia=20/{_RUN_NA_KEY}/"
    assert entity_ids_do_raw(prefixo) == [53, 3550308]
    assert entity_ids_do_raw(None) == []


class _ControleFalso:
    """O que ``plan_from_extrato`` usa do ``SiconfiStorage``."""

    def __init__(self) -> None:
        self.planejadas: set[tuple[str, str, str]] = set()
        self.fila: list[tuple[str, dict[str, Any], str | None]] = []

    def planned_keys(self, source: str, scope_hash: str) -> set[str]:
        return {k for s, h, k in self.planejadas if (s, h) == (source, scope_hash)}

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None:
        self.planejadas.update((source, scope_hash, k) for k in keys)

    def enqueue_many(self, endpoint: str, units: list[Any]) -> int:
        self.fila.extend((endpoint, params, rev) for params, rev in units)
        return len(units)


def _escopo(**extra: Any) -> PlanScope:
    return PlanScope.from_config({"start_year": 2024, "end_year": 2024, **extra})


def test_cada_arquivo_do_extrato_e_planejado_uma_vez(bucket: _BucketFalso) -> None:
    persist_page(_page("extrato_entregas", [_ITEM_RREO]), _RUN, _DATA)
    controle = _ControleFalso()

    primeira = plan_from_extrato(controle, _escopo(), budget=1000)
    segunda = plan_from_extrato(controle, _escopo(), budget=1000)

    assert primeira["rreo"] == 1
    assert segunda["rreo"] == 0
    assert controle.fila[0][1]["id_ente"] == 3550308


def test_mudar_o_recorte_replaneja_o_extrato(bucket: _BucketFalso) -> None:
    persist_page(_page("extrato_entregas", [_ITEM_RREO]), _RUN, _DATA)
    controle = _ControleFalso()
    plan_from_extrato(controle, _escopo(), budget=1000)

    novo = plan_from_extrato(controle, _escopo(rreo_periods=[6]), budget=1000)
    assert novo["rreo"] == 1


def test_orcamento_para_entre_arquivos_e_continua_depois(bucket: _BucketFalso) -> None:
    for offset in (0, 1, 2):
        persist_page(_page("extrato_entregas", [_ITEM_RREO], offset), _RUN, _DATA)
    controle = _ControleFalso()

    plan_from_extrato(controle, _escopo(), budget=2)
    assert len(controle.planned_keys("extrato_entregas", _escopo().fingerprint())) == 2
    plan_from_extrato(controle, _escopo(), budget=2)
    assert len(controle.planned_keys("extrato_entregas", _escopo().fingerprint())) == 3
