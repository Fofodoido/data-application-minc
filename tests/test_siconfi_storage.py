"""SICONFI no datalakehouse: cada página vira um JSON no raw; a ingestão não usa banco.

O que estes testes garantem (ADR 0005, 0009 e 0010):

- nenhuma camada é gravada no Postgres, e a ingestão não usa ``psycopg2``: o
  ``siconfi_bronze`` não aparece no código, e o controle sai pelo Trino;
- não há staging: cada página vira um arquivo, só no raw;
- o raw de uma página tem os mesmos campos da antiga ``raw_pages_<endpoint>``,
  com o ``payload`` intacto -- é o que torna 1:1 a cópia das páginas antigas;
- o planejamento lê o raw e gera as mesmas unidades que gerava do banco, cada
  arquivo do extrato é planejado uma vez por recorte e o controle recebe tudo
  em lote;
- a ingestão grava o resultado de cada unidade, inclusive quando falha no meio.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

import pytest

import siconfi_storage
from cliente_siconfi import (
    PlanScope,
    SiconfiPage,
    SiconfiPermanentError,
    SiconfiRetryableError,
    request_hash,
    work_units,
)
from siconfi_storage import (
    VALID_ENDPOINTS,
    entidade,
    entity_ids_do_raw,
    envelope_da_pagina,
    ingerir_endpoint,
    itens_do_raw,
    ContagemDePaginas,
    contar_paginas,
    partes_do_envelope,
    persist_page,
    plan_from_extrato,
    raw_keys,
)

_RAIZ = Path(__file__).resolve().parents[1]
_DATA = datetime(2026, 10, 20, 9, 0, tzinfo=timezone.utc)
_COLETA = datetime(2026, 10, 20, 9, 15, 30, tzinfo=timezone.utc)
_RUN = "scheduled__2026-10-20T09:00:00+00:00"
_RUN_NA_KEY = "scheduled__2026-10-20T09_00_00_00_00"
_DAG = "dags/data_ingest/siconfi/siconfi_ingestion_dag.py"

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

    def apagar_objeto(self, key: str) -> None:
        self.objetos.pop(key, None)


@pytest.fixture
def bucket() -> Any:
    falso = _BucketFalso()
    lake = siconfi_storage.datalakehouse
    with (
        patch.object(lake, "gravar_bytes", falso.gravar_bytes),
        patch.object(lake, "ler_json", falso.ler_json),
        patch.object(lake, "listar_objetos", falso.listar_objetos),
        patch.object(lake, "apagar_objeto", falso.apagar_objeto),
    ):
        yield falso


# ── nada de camada nem de banco na ingestão ──────────────────────────────


@pytest.mark.parametrize("arquivo", ["plugins/siconfi_storage.py", _DAG])
def test_ingestao_nao_grava_mais_no_schema_siconfi_bronze(arquivo: str) -> None:
    assert "siconfi_bronze" not in (_RAIZ / arquivo).read_text(encoding="utf-8")


def test_ingestao_funciona_sem_driver_de_postgres() -> None:
    """ADR 0005: o Airflow só fala com o Trino. Um psycopg2 aqui passa no
    ambiente local e quebra em produção. Em vez de procurar texto, importa os
    módulos da ingestão num processo onde o psycopg2 não existe."""
    codigo = (
        "import sys; sys.modules['psycopg2'] = None\n"
        "import cliente_siconfi, siconfi_controle, siconfi_storage\n"
        "assert 'psycopg2' not in [m for m in sys.modules if sys.modules[m]]\n"
    )
    ambiente = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(
            str(_RAIZ / pasta) for pasta in ("plugins", "helpers", "dags")
        ),
    }
    resultado = subprocess.run(
        [sys.executable, "-c", codigo], capture_output=True, text=True, env=ambiente
    )
    assert resultado.returncode == 0, resultado.stderr


def test_dag_de_ingestao_nao_pede_conexao_de_postgres() -> None:
    """A DAG não importa no ambiente de teste (Airflow 3 só na imagem), então
    aqui a guarda é pelo texto: nenhum ``get_postgres_conn`` nem ``psycopg2``."""
    codigo = (_RAIZ / _DAG).read_text(encoding="utf-8")
    assert "get_postgres_conn" not in codigo and "psycopg2" not in codigo


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


def test_pagina_vira_um_arquivo_so_no_raw(bucket: _BucketFalso) -> None:
    page = _page("rreo", [{"valor": 1}], offset=5000)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)

    nome = f"{request_hash('rreo', page.params)}_5000"
    pasta = f"siconfi/rreo/ano=2026/mes=10/dia=20/{_RUN_NA_KEY}/{nome}"
    assert set(bucket.objetos) == {f"raw/{pasta}.json"}


def test_regravar_a_mesma_pagina_no_mesmo_run_nao_cria_outro_arquivo(
    bucket: _BucketFalso,
) -> None:
    page = _page("rreo", [{"valor": 1}])
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)
    persist_page(page, _RUN, _DATA, fetched_at=_COLETA)
    assert len(bucket.objetos) == 1


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


def test_pagina_vazia_tambem_vai_para_o_raw(bucket: _BucketFalso) -> None:
    gravados = persist_page(_page("dca", []), _RUN, _DATA, fetched_at=_COLETA)
    assert gravados == 0
    assert [k.split("/")[0] for k in bucket.objetos] == ["raw"]


# ── planejamento a partir do raw ─────────────────────────────────────────


def test_item_lido_do_raw_gera_as_mesmas_unidades(bucket: _BucketFalso) -> None:
    """O marcador de revisão é ``str()`` de campos do item: se o tipo mudasse,
    unidades já concluídas voltariam a ser pendentes."""
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
    """O que ``plan_from_extrato`` e ``ingerir_endpoint`` usam do controle."""

    def __init__(self, pendentes: list[dict[str, Any]] | None = None) -> None:
        self.planejadas: set[tuple[str, str, str]] = set()
        self.fila: list[tuple[str, dict[str, Any], str | None]] = []
        self.chamadas_mark_planned = 0
        self.pendentes = pendentes or []
        self.filtro: Any = None
        self.eventos: list[list[dict[str, Any]]] = []

    def planned_keys(self, source: str, scope_hash: str) -> set[str]:
        return {k for s, h, k in self.planejadas if (s, h) == (source, scope_hash)}

    def mark_planned(self, source: str, scope_hash: str, keys: list[str]) -> None:
        self.chamadas_mark_planned += 1
        self.planejadas.update((source, scope_hash, k) for k in keys)

    def enqueue_many(self, endpoint: str, units: list[Any]) -> int:
        self.fila.extend((endpoint, params, rev) for params, rev in units)
        return len(units)

    def pending(self, endpoint: str, limit: int, param_filter: Any = None) -> list:
        self.filtro = param_filter
        return self.pendentes[:limit]

    def record_events(self, endpoint: str, events: list, run_id: str) -> None:
        self.eventos.append([dict(e) for e in events])


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


def test_arquivos_lidos_vao_para_o_controle_em_lote(bucket: _BucketFalso) -> None:
    """Uma gravação por arquivo era uma ida ao banco por arquivo."""
    for offset in range(5):
        persist_page(_page("extrato_entregas", [_ITEM_RREO], offset), _RUN, _DATA)
    controle = _ControleFalso()

    plan_from_extrato(controle, _escopo(), budget=1000, flush_size=5000)

    assert controle.chamadas_mark_planned == 1
    assert len(controle.planejadas) == 5


# ── ingestão ─────────────────────────────────────────────────────────────


class _ClienteFalso:
    """Responde por ``id_ente``: 1 com uma página, 2 vazio, 3 retry, 4 404, 5 400."""

    def iter_pages(self, endpoint: str, params: dict[str, Any]) -> Iterator[SiconfiPage]:
        ente = params["id_ente"]
        if ente == 3:
            raise SiconfiRetryableError(503, "fora do ar")
        if ente == 4:
            raise SiconfiPermanentError(404, "não existe")
        if ente == 5:
            raise SiconfiPermanentError(400, "parâmetro inválido")
        if ente == 6:
            raise RuntimeError("MinIO caiu")
        yield SiconfiPage(
            endpoint=endpoint,
            params=params,
            offset=0,
            items=[{"v": 1}] if ente == 1 else [],
            has_more=False,
            headers={},
            payload={"items": [{"v": 1}] if ente == 1 else []},
        )


def _unidade(ente: int) -> dict[str, Any]:
    params = {"id_ente": ente, "an_exercicio": 2024}
    return {"work_key": f"k{ente}", "params": params, "revision_marker": f"r{ente}"}


def _ingerir(controle: _ControleFalso, **extra: Any) -> dict[str, int]:
    return ingerir_endpoint(
        "rreo",
        controle,
        _ClienteFalso(),
        _escopo(),
        _RUN,
        _DATA,
        limite=extra.pop("limite", 100),
        prazo_s=extra.pop("prazo_s", 60),
        **extra,
    )


def test_cada_unidade_vira_um_evento_com_a_revisao(bucket: _BucketFalso) -> None:
    controle = _ControleFalso([_unidade(e) for e in (1, 2, 3, 4, 5)])

    resumo = _ingerir(controle)

    eventos = {e["work_key"]: e for lote in controle.eventos for e in lote}
    assert {k: e["status"] for k, e in eventos.items()} == {
        "k1": "success",
        "k2": "no_data",
        "k3": "retry",
        "k4": "no_data",
        "k5": "permanent_error",
    }
    assert eventos["k3"]["next_attempt_at"] > datetime.now(timezone.utc)
    assert eventos["k1"]["next_attempt_at"] is None
    assert eventos["k1"]["revision_marker"] == "r1"
    assert resumo["pending"] == 5 and resumo["rows"] == 1
    assert controle.filtro == _escopo().constraints("rreo")


def test_eventos_vao_em_lotes_e_no_fim(bucket: _BucketFalso) -> None:
    controle = _ControleFalso([_unidade(2) for _ in range(5)])
    _ingerir(controle, eventos_por_gravacao=2)
    assert [len(lote) for lote in controle.eventos] == [2, 2, 1]


def test_erro_inesperado_grava_o_que_ja_foi_feito(bucket: _BucketFalso) -> None:
    """Sem o evento, a unidade continua pendente e seria refeita à toa."""
    controle = _ControleFalso([_unidade(1), _unidade(2), _unidade(6), _unidade(1)])
    with pytest.raises(RuntimeError, match="MinIO caiu"):
        _ingerir(controle)
    feitas = [e["work_key"] for lote in controle.eventos for e in lote]
    assert feitas == ["k1", "k2"]


def test_prazo_esgotado_nao_processa_nem_grava_evento(bucket: _BucketFalso) -> None:
    controle = _ControleFalso([_unidade(1)])
    resumo = _ingerir(controle, prazo_s=0)
    assert controle.eventos == [] and resumo["success"] == 0
    assert bucket.objetos == {}


def test_endpoint_fora_do_recorte_nem_consulta_a_fila(bucket: _BucketFalso) -> None:
    controle = _ControleFalso([_unidade(1)])
    resumo = ingerir_endpoint(
        "rgf",
        controle,
        _ClienteFalso(),
        _escopo(fact_endpoints=["rreo"]),
        _RUN,
        _DATA,
        limite=10,
        prazo_s=60,
    )
    assert resumo["pending"] == 0 and controle.filtro is None


# ── página acima do limite do proxy do MinIO ─────────────────────────────

_LIMITE = 4000


def _envelope(n_itens: int) -> dict[str, Any]:
    items = [{**_ITEM_RREO, "cod_ibge": i} for i in range(n_itens)]
    page = SiconfiPage(
        endpoint="rreo",
        params={"id_ente": 1, "an_exercicio": 2024},
        offset=5000,
        items=items,
        has_more=True,
        headers={"content-type": "application/json"},
        payload={"items": items, "hasMore": True, "offset": 5000, "limit": 5000},
    )
    return envelope_da_pagina(page, _RUN, _COLETA)


def test_pagina_que_cabe_nao_muda() -> None:
    envelope = _envelope(3)
    ((parte, corpo),) = partes_do_envelope(envelope, _LIMITE)
    assert parte == envelope and json.loads(corpo) == json.loads(
        json.dumps(envelope, default=str)
    )


def test_pagina_grande_vira_partes_com_os_itens_na_ordem() -> None:
    envelope = _envelope(40)
    com_corpo = partes_do_envelope(envelope, _LIMITE)
    partes = [parte for parte, _ in com_corpo]

    assert len(partes) > 1
    for parte, corpo in com_corpo:
        # O corpo devolvido é o JSON da própria parte, o que vai para o arquivo.
        assert json.loads(corpo) == json.loads(json.dumps(parte, default=str))
        assert len(corpo) <= _LIMITE
        # O resto do envelope se repete; só os itens são fatiados.
        assert {k: v for k, v in parte.items() if k not in ("payload", "_parte")} == {
            k: v for k, v in envelope.items() if k != "payload"
        }
        assert parte["payload"]["hasMore"] is True
        assert parte["_parte"]["itens"] == len(parte["payload"]["items"])
    assert [p["_parte"]["indice"] for p in partes] == list(range(1, len(partes) + 1))
    assert {p["_parte"]["total"] for p in partes} == {len(partes)}
    juntos = [item for p in partes for item in p["payload"]["items"]]
    assert juntos == envelope["payload"]["items"]
    assert sum(p["_parte"]["itens"] for p in partes) == envelope["item_count"] == 40


def test_pagina_grande_sem_itens_falha_com_o_motivo() -> None:
    envelope = {**_envelope(0), "payload": {"texto": "x" * _LIMITE}}
    with pytest.raises(ValueError, match="sem lista de itens"):
        partes_do_envelope(envelope, _LIMITE)


def test_gravar_pagina_grande_grava_cada_parte(bucket: Any) -> None:
    envelope = _envelope(40)
    with patch.object(siconfi_storage.datalakehouse, "LIMITE_OBJETO_BYTES", _LIMITE):
        keys = siconfi_storage.gravar_envelope(envelope, _DATA)

    total = len(keys)
    assert total > 1 and set(keys) == set(bucket.objetos)
    base = f"{envelope['request_hash']}_5000"
    largura = max(2, len(str(total)))
    assert [k.rsplit("/", 1)[-1] for k in keys] == [
        f"{base}_p{i:0{largura}d}de{total:0{largura}d}.json" for i in range(1, total + 1)
    ]
    lidos = [item for key in raw_keys("rreo") for item in itens_do_raw(key)]
    assert lidos == envelope["payload"]["items"]


def test_envelope_sem_itens_acima_do_limite_diz_isso() -> None:
    envelope = {**_envelope(3), "response_headers": {"x": "y" * _LIMITE}}
    with pytest.raises(ValueError, match="o envelope sem os itens já tem"):
        partes_do_envelope(envelope, _LIMITE)


def test_dividir_apaga_a_mesma_pagina_gravada_inteira(bucket: Any) -> None:
    """A conversão regrava o lote que falhou: uma página que passou inteira antes
    (entre 900 KiB e o limite real) não pode ficar também em partes."""
    envelope = _envelope(40)
    (inteira,) = siconfi_storage.gravar_envelope(envelope, _DATA)
    with patch.object(siconfi_storage.datalakehouse, "LIMITE_OBJETO_BYTES", _LIMITE):
        partes = siconfi_storage.gravar_envelope(envelope, _DATA)

    assert inteira not in bucket.objetos
    assert set(bucket.objetos) == set(partes)
    assert contar_paginas(bucket.objetos) == ContagemDePaginas(completas=1, ambiguas=0)
    lidos = [item for key in raw_keys("rreo") for item in itens_do_raw(key)]
    assert lidos == envelope["payload"]["items"]


def test_contagem_e_por_pagina_e_acusa_pagina_em_duas_formas() -> None:
    keys = [
        "raw/x/a_0.json",
        "raw/x/b_0_p01de02.json",
        "raw/x/b_0_p02de02.json",
        "raw/x/c_0_p01de03.json",  # faltam duas partes: incompleta
        "raw/x/c_0_p03de03.json",
        "raw/x/d_0.json",  # inteira e dividida
        "raw/x/d_0_p01de02.json",
        "raw/x/d_0_p02de02.json",
        "raw/x/e_0_p01de02.json",  # dividida de dois jeitos
        "raw/x/e_0_p02de02.json",
        "raw/x/e_0_p01de03.json",
        "raw/x/e_0_p02de03.json",
        "raw/x/e_0_p03de03.json",
    ]
    assert contar_paginas(keys) == ContagemDePaginas(completas=2, ambiguas=2)
