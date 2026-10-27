"""Testes das partes puras do datalakehouse: caminho, conversão e cadeia.

O que importa aqui: raw e staging de um mesmo run sempre se encontram pelo
caminho, e o Parquet de staging tem o mesmo shape que o Postgres tinha
(colunas achatadas com ``__``, em minúsculas, tudo texto) -- é o que a ponte
grava de volta nas tabelas que o dbt lê.
"""

import io
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from datalakehouse import (
    cadeia_do_gatilho,
    caminho,
    caminho_particionado,
    eventos_em_ordem,
    fatiar_por_tamanho,
    key_da_cadeia,
    nome_da_parte,
    parquet_para_registros,
    parte_do_nome,
    raw_para_staging_key,
    registros_para_parquet,
    uri_asset,
)
from extracao_por_plano_acao import (
    carregar_planos_acao,
    ids_ja_baixados,
    juntar_anexos_ao_plano,
    key_anexo_arquivo,
    politica_do_programa,
)

_DATA = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _ler(parquet: bytes) -> pd.DataFrame:
    return pd.read_parquet(io.BytesIO(parquet))


# ── caminho ──────────────────────────────────────────────────────────────


def test_caminho_particiona_por_data_e_sanitiza_run_id() -> None:
    key = caminho(
        "raw",
        "transferegov",
        "plano_acao_minc",
        "manual__2026-10-01T12:00:00+00:00",
        _DATA,
    )
    assert key == (
        "raw/transferegov/plano_acao_minc/ano=2026/mes=10/dia=01/"
        "manual__2026-10-01T12_00_00_00_00.json"
    )


def test_caminho_particionado_usa_a_particao_do_dado_e_nao_a_data() -> None:
    key = caminho_particionado(
        "staging",
        "inep",
        "ed_basica",
        {"ano_censo": 2015},
        "manual__2026-10-07T12:00:00+00:00",
        "parquet",
    )
    assert key == (
        "staging/inep/ed_basica/ano_censo=2015/"
        "manual__2026-10-07T12_00_00_00_00.parquet"
    )


def test_caminho_particionado_recusa_camada_desconhecida() -> None:
    with pytest.raises(ValueError):
        caminho_particionado("bronze", "inep", "x", {}, "r", "zip")


def test_staging_e_o_raw_com_outro_prefixo_e_extensao() -> None:
    raw = caminho("raw", "transferegov", "programa_minc", "run", _DATA)
    staging = caminho("staging", "transferegov", "programa_minc", "run", _DATA)
    assert raw_para_staging_key(raw) == staging


def test_camada_desconhecida_e_recusada() -> None:
    with pytest.raises(ValueError):
        caminho("bronze", "transferegov", "x", "run", _DATA)


def test_key_que_nao_e_raw_nao_vira_staging() -> None:
    with pytest.raises(ValueError):
        raw_para_staging_key("staging/transferegov/x/run.parquet")


# ── conversão ────────────────────────────────────────────────────────────


def test_parquet_achata_e_grava_tudo_como_texto() -> None:
    registros = [
        {"id": 1, "ente": {"uf": "DF"}, "tags": ["a", "b"], "ativo": True, "valor": None},
        {"id": 2, "ente": {"uf": "GO"}, "tags": [], "ativo": False, "valor": 1.5},
    ]
    df = _ler(registros_para_parquet(registros, dt_ingest=_DATA))

    assert set(df.columns) == {"id", "ente__uf", "tags", "ativo", "valor", "dt_ingest"}
    assert all(str(t) == "string" for t in df.dtypes)
    assert df["id"].tolist() == ["1", "2"]
    assert json.loads(df["tags"][0]) == ["a", "b"]
    assert df["ativo"].tolist() == ["true", "false"]
    assert pd.isna(df["valor"][0]) and df["valor"][1] == "1.5"
    assert set(df["dt_ingest"]) == {_DATA.isoformat()}


def test_enriquecer_entra_no_staging_sem_mexer_no_registro_de_origem() -> None:
    registros = [{"id": 7}]
    df = _ler(registros_para_parquet(registros, enriquecer=lambda r: {"sigla": "LPG"}))

    assert df["sigla"].tolist() == ["LPG"]
    assert registros == [{"id": 7}]


def test_lista_vazia_gera_parquet_legivel() -> None:
    assert len(_ler(registros_para_parquet([]))) == 0


def test_colunas_camelcase_saem_em_minusculo_como_no_postgres() -> None:
    registros = [{"id": 1, "tipoAnexo": {"auditLogin": "x", "idPrograma": 46}}]
    df = _ler(registros_para_parquet(registros, dt_ingest=_DATA))

    assert {"tipoanexo__auditlogin", "tipoanexo__idprograma"} <= set(df.columns)


# ── cadeia entre DAGs ────────────────────────────────────────────────────


def test_uri_do_asset_e_estavel_por_entidade() -> None:
    assert uri_asset("staging/transferegov/plano_acao_minc/") == (
        "s3://minc-datalakehouse/staging/transferegov/plano_acao_minc"
    )


def test_cadeia_vem_do_evento_mais_novo() -> None:
    extras = [
        {"plano_acao_minc": "staging/a.parquet"},
        {"plano_acao_minc": "staging/b.parquet"},
    ]
    assert cadeia_do_gatilho(extras, conf={"plano_acao_minc": "x"}) == {
        "plano_acao_minc": "staging/b.parquet"
    }


def test_eventos_fora_de_ordem_sao_ordenados_pelo_timestamp() -> None:
    # A ordem que o Airflow devolveu no teste do PR #58: nem crescente nem
    # decrescente.
    def evento(hora: str, key: str) -> SimpleNamespace:
        return SimpleNamespace(
            timestamp=datetime.fromisoformat(f"2026-10-06T{hora}+00:00"),
            extra={"programa_minc": key},
        )

    eventos = [
        evento("14:02:00", "c"),
        evento("14:00:50.170000", "a"),
        evento("14:00:50.380000", "b"),
        evento("14:27:00", "d"),
    ]
    em_ordem = eventos_em_ordem(eventos)
    assert [e.extra["programa_minc"] for e in em_ordem] == ["a", "b", "c", "d"]
    assert cadeia_do_gatilho(e.extra for e in em_ordem) == {"programa_minc": "d"}


def test_sem_evento_a_cadeia_vem_do_conf() -> None:
    assert cadeia_do_gatilho([], conf={"programa_minc": "staging/p.parquet"}) == {
        "programa_minc": "staging/p.parquet"
    }
    assert cadeia_do_gatilho([], conf=None) == {}


def test_entidade_fora_da_cadeia_diz_o_que_falta() -> None:
    with pytest.raises(KeyError, match="relatorios_gestao"):
        key_da_cadeia({"plano_acao_minc": "k"}, "relatorios_gestao")


# ── ponte staging -> Postgres ────────────────────────────────────────────


def test_registros_da_ponte_sem_na_e_sem_chave_repetida() -> None:
    df = pd.DataFrame({"id": ["1", "1", "2"], "nome": ["velho", "novo", pd.NA]}).astype(
        "string"
    )
    assert parquet_para_registros(df, ["id"]) == [
        {"id": "1", "nome": "novo"},
        {"id": "2", "nome": None},
    ]


def test_parquet_vazio_nao_gera_registros() -> None:
    assert parquet_para_registros(_ler(registros_para_parquet([])), ["id"]) == []


# ── leitura do staging nas DAGs encadeadas ───────────────────────────────


def test_planos_do_staging_viram_chaves_sem_na() -> None:
    planos = pd.DataFrame(
        {"id_plano_acao": ["1"], "id_programa": ["46"], "cod_ibge": [pd.NA], "x": ["y"]}
    ).astype("string")
    assert carregar_planos_acao(planos) == [
        {"id_plano_acao": "1", "id_programa": "46", "cod_ibge": None}
    ]


def test_join_de_anexos_descarta_orfaos_e_nao_devolve_na() -> None:
    anexos = pd.DataFrame(
        {
            "id": ["10", "11"],
            "nome": ["a.xlsx", "b.ods"],
            "id_relatorio_gestao": ["1", "9"],
        }
    )
    relatorios = pd.DataFrame({"id_relatorio_gestao": ["1"], "id_plano_acao": ["100"]})
    planos = pd.DataFrame(
        {"id_plano_acao": ["100"], "id_programa": ["46"], "cod_ibge": [pd.NA]}
    )
    juntos = juntar_anexos_ao_plano(anexos, relatorios, planos)

    assert juntos["id"].tolist() == ["10"]
    assert juntos["cod_ibge"].tolist() == [None]


def test_join_de_anexos_sem_campo_obrigatorio_diz_qual_entidade() -> None:
    anexos = pd.DataFrame({"id": ["10"], "id_relatorio_gestao": ["1"]})
    relatorios = pd.DataFrame({"id_relatorio_gestao": ["1"], "id_plano_acao": ["100"]})
    planos = pd.DataFrame(
        {"id_plano_acao": ["100"], "id_programa": ["46"], "cod_ibge": ["1"]}
    )

    with pytest.raises(ValueError, match=r"anexos_relatorios.*'nome'"):
        juntar_anexos_ao_plano(anexos, relatorios, planos)


@pytest.mark.parametrize(
    ("id_programa", "politica"),
    [("46", "lpg"), (61, "pnab"), ("999", "outros"), (None, "outros")],
)
def test_politica_do_programa(id_programa: object, politica: str) -> None:
    assert politica_do_programa(id_programa) == politica


def test_anexo_ja_baixado_e_reconhecido_pelo_id() -> None:
    keys = [
        key_anexo_arquivo("lpg", 10, "planilha_anexo_99.xlsx"),
        key_anexo_arquivo("pnab", 11, "outra.ods"),
        "raw/transferegov/anexos_arquivos/lpg/README",
    ]
    assert ids_ja_baixados(keys) == {"10": keys[0], "11": keys[1]}


def test_existe_objeto_pede_uma_chave_so() -> None:
    """Uma pasta de run da MSC tem centenas de milhares de arquivos; para saber se
    está vazia não se lista a pasta inteira."""
    from unittest.mock import MagicMock, patch

    import datalakehouse

    s3 = MagicMock()
    s3.list_objects_v2.side_effect = [{"KeyCount": 1}, {"KeyCount": 0}]
    hook = MagicMock()
    hook.get_conn.return_value = s3
    with patch.object(datalakehouse, "get_hook", return_value=hook):
        assert datalakehouse.existe_objeto("raw/siconfi/rreo/x/") is True
        assert datalakehouse.existe_objeto("raw/siconfi/rgf/x/") is False
    s3.list_objects_v2.assert_called_with(
        Bucket=datalakehouse.BUCKET, Prefix="raw/siconfi/rgf/x/", MaxKeys=1
    )


# ── divisão de arquivos acima do limite do proxy do MinIO ────────────────


def test_fatia_na_ordem_sem_passar_do_limite() -> None:
    grupos = fatiar_por_tamanho([4, 4, 4, 1, 9, 2], lambda n: n, 9)
    assert grupos == [[4, 4], [4, 1], [9], [2]]
    assert [n for grupo in grupos for n in grupo] == [4, 4, 4, 1, 9, 2]


def test_sem_itens_devolve_um_grupo_vazio() -> None:
    assert fatiar_por_tamanho([], len, 10) == [[]]


def test_item_maior_que_o_limite_falha_antes_do_minio() -> None:
    with pytest.raises(ValueError, match="um item sozinho tem 11 bytes"):
        fatiar_por_tamanho(["x" * 11], len, 10)


def test_nome_da_parte_ida_e_volta_e_ordenavel() -> None:
    assert nome_da_parte("abc_5000", 2, 3) == "abc_5000_p02de03"
    assert nome_da_parte("abc_0", 7, 120) == "abc_0_p007de120"
    assert parte_do_nome("abc_5000_p02de03") == ("abc_5000", 2, 3)
    assert parte_do_nome("abc_5000") is None
    nomes = [nome_da_parte("a", i, 12) for i in range(1, 13)]
    assert sorted(nomes) == nomes
    with pytest.raises(ValueError):
        nome_da_parte("a", 4, 3)
