"""Guardas da governança declarativa do OpenMetadata (`dbt/minc/governance/`).

Dois grupos:

- a DECLARAÇÃO: todo ativo do projeto dbt tem produto e camada, todo termo,
  formato e schema citado existe. É o que impede o catálogo de envelhecer em
  silêncio quando alguém cria um modelo novo;
- o CLIENTE: paginação, nome de exibição em patch próprio e convergência — os
  comportamentos que, quebrados, fazem o sync escrever a mais ou a menos sem
  erro nenhum.

Os testes da declaração precisam do `manifest.json`, gerado por `dbt parse`
(sem banco). Sem o CLI `dbt` no ambiente, eles pulam.
"""

from __future__ import annotations

import re
from typing import Any
import shutil
from pathlib import Path

import pytest
import yaml

from openmetadata.glossary import load_glossary
from openmetadata.governanca import catalogo, config
from openmetadata.governanca.catalogo import (
    RELACOES_VALIDAS,
    Formatos,
    relacoes_de_termos,
)
from openmetadata.governanca.cliente import ClienteOM, equivalente, etiqueta
from openmetadata.governanca.governanca import (
    mesclar_etiquetas,
    operacoes_de_extensao,
)

REPO = Path(__file__).resolve().parents[1]
GOV = REPO / "dbt/minc/governance"


def _yaml(nome: str) -> dict:
    return yaml.safe_load((GOV / nome).read_text(encoding="utf-8")) or {}


@pytest.fixture(scope="session")
def manifest(tmp_path_factory: pytest.TempPathFactory) -> dict:
    if shutil.which("dbt") is None:
        pytest.skip("CLI dbt ausente; os testes da declaração precisam do manifest")
    destino = tmp_path_factory.mktemp("dbt_target")
    return catalogo.carregar_manifest(catalogo.gerar_manifest(destino))


@pytest.fixture(scope="session")
def cat(manifest: dict) -> catalogo.Catalogo:
    return catalogo.montar(manifest)


# ── declaração ───────────────────────────────────────────────────────────
def test_todo_ativo_tem_produto_e_camada(cat: catalogo.Catalogo) -> None:
    """Modelo ou source novo sem dono em `dominios.yml` reprova aqui.

    Sem esta guarda, o sync o ignoraria em silêncio — fail-closed — e a tabela
    chegaria ao catálogo sem domínio, produto nem certificação.
    """
    assert not cat.erros, "\n".join(cat.erros)


def test_produto_aponta_para_dominio_declarado() -> None:
    dominios = _yaml("dominios.yml")
    fqns = {
        f"{d['parent']}.{d['name']}" if d.get("parent") else d["name"]
        for d in dominios["dominios"]
    }
    for produto in dominios["produtos"]:
        assert produto["domain"] in fqns, produto["name"]
    for fora in dominios.get("fora_de_produto") or []:
        assert fora["domain"] in fqns, fora


def test_produtos_nao_disputam_pasta_nem_source() -> None:
    """Um ativo em dois produtos teria o produto decidido pela ordem do YAML."""
    vistos: dict[str, str] = {}
    for produto in _yaml("dominios.yml")["produtos"]:
        selecao = produto.get("selecao") or {}
        for item in (selecao.get("pastas") or []) + (selecao.get("sources") or []):
            assert item not in vistos, f"{item} em {vistos.get(item)} e {produto['name']}"
            vistos[item] = produto["name"]


def test_todo_schema_tocado_esta_em_schemas_yml(cat: catalogo.Catalogo) -> None:
    declarados = {s["name"] for s in _yaml("schemas.yml")["schemas"]}
    usados = {a.schema for a in cat.ativos.values()}
    assert usados <= declarados, sorted(usados - declarados)


def test_camada_de_todo_ativo_tem_tier_e_uso() -> None:
    dominios = _yaml("dominios.yml")
    camadas = (
        set(dominios["camada_por_pasta"].values())
        | set(dominios["camada_por_prefixo_de_arquivo"].values())
        | {"raw"}
    )
    assert camadas <= set(dominios["tier_por_camada"])
    assert camadas <= set(dominios["permissao_de_uso_por_camada"])


def test_bronze_e_raw_nunca_sao_consumiveis() -> None:
    """A bronze entra no catálogo para a linhagem, não para análise."""
    uso = _yaml("dominios.yml")["permissao_de_uso_por_camada"]
    assert uso["raw"] == uso["bronze"] == "Uso.NaoConsumivel"


def test_relacoes_ligam_termos_que_existem_no_csv() -> None:
    _, termos = load_glossary(str(config.GLOSSARIO))
    fqns = {t["fullyQualifiedName"] for t in termos}
    for rel in _yaml("termos_minc.yml")["relacoes"]:
        assert rel["tipo"] in RELACOES_VALIDAS, rel
        assert rel["de"] in fqns and rel["para"] in fqns, rel
        assert rel["de"] != rel["para"], rel


def test_termos_aplicados_e_dos_produtos_existem() -> None:
    _, termos = load_glossary(str(config.GLOSSARIO))
    fqns = {t["fullyQualifiedName"] for t in termos}
    declarados = _yaml("termos_minc.yml")
    usados = set(declarados["aplica_a"]) | set(declarados["aplica_a_colunas"])
    for produto in _yaml("dominios.yml")["produtos"]:
        usados |= set(produto.get("termos") or [])
    usados |= {
        t for ts in _yaml("metricas.yml")["termos_por_metrica"].values() for t in ts
    }
    assert usados <= fqns, sorted(usados - fqns)


def test_colunas_dos_termos_existem_em_algum_ativo(cat: catalogo.Catalogo) -> None:
    """Regra em massa para coluna que não existe é regra que nunca dispara."""
    colunas = {c.nome.lower() for a in cat.ativos.values() for c in a.colunas}
    for fqn, nomes in _yaml("termos_minc.yml")["aplica_a_colunas"].items():
        for nome in nomes:
            assert nome in colunas, f"{fqn}: coluna {nome} não existe"


def test_metricas_mapeadas_existem_no_metricflow(manifest: dict) -> None:
    nomes = {m["name"] for m in manifest["metrics"].values()}
    mapeadas = set(_yaml("metricas.yml")["termos_por_metrica"])
    assert mapeadas <= nomes, sorted(mapeadas - nomes)


def test_formatos_das_regras_existem_e_regex_compila() -> None:
    formatos = _yaml("formatos.yml")
    nomes = {f["name"] for f in formatos["formatos"]}
    for f in formatos["formatos"]:
        if f.get("regex"):
            re.compile(f["regex"])
    for regra in formatos["regras"]:
        assert regra["formato"] in nomes, regra


def test_documento_cru_nunca_e_declarado_so_digitos() -> None:
    """A origem não garante máscara nem ausência dela; afirmar "só dígitos" no
    CPF do SALIC seria o catálogo mentindo com autoridade."""
    formatos = Formatos(_yaml("formatos.yml"))
    assert formatos.do("cgccpf", "text", "raw", "x", {}) == "DocumentoComoNaOrigem"
    assert formatos.do("identificador_unico", "text", "gold", "x", {}) == (
        "DocumentoSoDigitos"
    )


def test_formato_explicito_da_coluna_vence_a_regra() -> None:
    formatos = Formatos(_yaml("formatos.yml"))
    meta = {"formato": "DocumentoSoDigitos"}
    assert formatos.do("cgccpf", "text", "silver", "x", meta) == "DocumentoSoDigitos"


def test_valor_da_serie_do_bacen_nao_vira_reais() -> None:
    formatos = Formatos(_yaml("formatos.yml"))
    assert formatos.do("valor", "numeric", "bronze", "serie_sgs", {}) is None
    assert formatos.do("valor_pago_num", "numeric", "gold", "x", {}) == "MoedaBRL"


def test_csv_do_glossario_nao_carrega_relacao_nem_campo_a_mais() -> None:
    """A vírgula sem aspas já truncou a descrição de MinC.IncentivoFiscal."""
    _, termos = load_glossary(str(config.GLOSSARIO))
    incentivo = next(t for t in termos if t["name"] == "IncentivoFiscal")
    assert incentivo["description"].endswith("mecanismo de financiamento.")
    assert incentivo["synonyms"] == []


def test_database_da_governanca_segue_o_das_recipes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DB_DW_DBNAME", "dbanalytics")
    monkeypatch.setenv("OM_DATABASE", "minc")
    assert config.banco() == "dbanalytics"


# ── relações e etiquetas ─────────────────────────────────────────────────
def test_relacao_declarada_num_sentido_chega_aos_dois_termos() -> None:
    mapa = relacoes_de_termos(
        {
            "relacoes": [
                {"de": "A", "tipo": "partOf", "para": "B"},
                {"de": "A", "tipo": "seeAlso", "para": "C"},
            ]
        }
    )
    assert mapa["A"] == {("partOf", "B"), ("seeAlso", "C")}
    assert mapa["B"] == {("hasPart", "A")}
    assert mapa["C"] == {("seeAlso", "A")}


def test_mescla_preserva_pii_e_termo_pendurado_a_mao() -> None:
    atuais = [
        etiqueta("PII.Sensitive"),
        etiqueta("Tier.Tier4"),
        etiqueta("MinC.Agentes.Proponente", "Glossary"),
        etiqueta("MinC.Politicas.LPG", "Glossary"),
    ]
    desejadas = [etiqueta("Tier.Tier2")]
    resultado = {
        t["tagFQN"] for t in mesclar_etiquetas(atuais, desejadas, {"MinC.Politicas.LPG"})
    }
    assert resultado == {"PII.Sensitive", "Tier.Tier2", "MinC.Agentes.Proponente"}


def test_extensao_e_escrita_por_propriedade() -> None:
    """Substituir `/extension` inteira apagaria propriedade de outro órgão."""
    ops = operacoes_de_extensao(
        {"outraProp": "x", "mincCamada": "bronze"}, {"mincCamada": "silver"}
    )
    assert ops == [{"op": "add", "path": "/extension/mincCamada", "value": "silver"}]
    assert operacoes_de_extensao({"mincCamada": "silver"}, {"mincCamada": "silver"}) == []


def test_relacao_tipada_compara_pelo_tipo() -> None:
    termo = {"id": "1", "fullyQualifiedName": "MinC.A", "name": "A"}
    assert not equivalente(
        [{"relationType": "relatedTo", "term": termo}],
        [{"relationType": "partOf", "term": termo}],
    )
    assert equivalente(
        [{"relationType": "partOf", "term": {**termo, "href": "h"}}],
        [{"relationType": "partOf", "term": termo}],
    )


def test_descricao_com_escapada_html_e_conforme() -> None:
    assert equivalente("a &#61; b", "a = b")


# ── cliente ──────────────────────────────────────────────────────────────
class _Resposta:
    def __init__(self, status: int, corpo: dict | None = None) -> None:
        self.status_code = status
        self.ok = status < 400
        self._corpo = corpo or {}
        self.text = str(self._corpo)
        self.content = b"x"

    def json(self) -> dict:
        return self._corpo


class _Sessao:
    def __init__(self, respostas: list[_Resposta]) -> None:
        self.headers: dict = {}
        self.respostas = respostas
        self.chamadas: list[tuple] = []

    def request(self, metodo: str, url: str, **kwargs: Any) -> _Resposta:
        self.chamadas.append((metodo, url, kwargs))
        return self.respostas.pop(0)


def test_listar_segue_todas_as_paginas() -> None:
    sessao = _Sessao(
        [
            _Resposta(200, {"data": [{"n": 1}], "paging": {"after": "c1"}}),
            _Resposta(200, {"data": [{"n": 2}], "paging": {}}),
        ]
    )
    om = ClienteOM("https://om", "t", sessao=sessao)
    assert [x["n"] for x in om.listar("tables")] == [1, 2]
    assert sessao.chamadas[1][2]["params"]["after"] == "c1"


def test_nome_de_exibicao_vai_em_patch_separado() -> None:
    """A recusa do displayName não pode levar descrição e dono junto."""
    sessao = _Sessao([_Resposta(200), _Resposta(403, {"m": "DisplayName-Deny"})])
    om = ClienteOM("https://om", "t", confirmar=True, sessao=sessao)
    om.patch(
        "domains",
        "1",
        [
            {"op": "add", "path": "/description", "value": "d"},
            {"op": "add", "path": "/displayName", "value": "D"},
        ],
        "x",
    )
    primeiro, segundo = (c[2]["json"] for c in sessao.chamadas)
    assert [o["path"] for o in primeiro] == ["/description"]
    assert [o["path"] for o in segundo] == ["/displayName"]
    assert om.atualizados == 1 and len(om.falhas) == 1
    assert "DisplayName-Deny" in om.falhas[0]


def test_simulacao_nunca_escreve() -> None:
    sessao = _Sessao([])
    om = ClienteOM("https://om", "t", confirmar=False, sessao=sessao)
    om.patch("tables", "1", [{"op": "add", "path": "/tags", "value": []}], "x")
    om.criar("domains", {}, "x")
    assert sessao.chamadas == [] and om.modo == "simulacao"


def test_sem_credencial_e_offline() -> None:
    assert ClienteOM(None, None, confirmar=True).modo == "offline"


# ── qualidade, métricas e dbt test ───────────────────────────────────────
def test_teste_de_formato_so_nasce_em_coluna_textual_fora_da_bronze(
    cat: catalogo.Catalogo,
) -> None:
    """Regex sobre a bronze leria 157 M de linhas; sobre inteiro, converteria
    a coluna inteira em texto. Os dois casos ficam de fora."""
    from openmetadata.governanca.qualidade import TIPOS_TEXTUAIS, testes_nativos

    camadas = set(_yaml("qualidade.yml")["testes_de_formato"]["camadas"])
    for ativo, teste in testes_nativos(cat):
        if not teste["name"].startswith("minc_formato_"):
            continue
        coluna = next(c for c in ativo.colunas if c.nome == teste["coluna"])
        assert ativo.camada in camadas, (ativo.nome, teste["name"])
        assert coluna.tipo.lower() in TIPOS_TEXTUAIS, (ativo.nome, coluna.nome)


def test_teste_declarado_aponta_para_modelo_e_coluna_que_existem(
    cat: catalogo.Catalogo,
) -> None:
    por_nome = cat.por_nome()
    for bloco in _yaml("qualidade.yml")["testes_declarados"]:
        assert bloco["tabela"] in por_nome, bloco["tabela"]
        colunas = {c.nome for c in por_nome[bloco["tabela"]].colunas}
        for teste in bloco["testes"]:
            if teste.get("coluna"):
                assert teste["coluna"] in colunas, (bloco["tabela"], teste["coluna"])


def test_suite_de_execucao_casa_com_a_recipe() -> None:
    """Nome divergente faz a recipe procurar uma suíte que não existe."""
    recipe = yaml.safe_load(
        (REPO / "helpers/openmetadata/recipes/postgres_testsuite.yaml").read_text()
    )
    nome = _yaml("qualidade.yml")["suite_de_execucao"]["name"]
    assert recipe["source"]["serviceName"] == nome


def test_toda_metrica_vira_expressao_sql(cat: catalogo.Catalogo) -> None:
    from openmetadata.governanca.metricas import Traducao

    traducao = Traducao(cat)
    for nome, metrica in traducao.metricas.items():
        sql = traducao.expressao(metrica)
        assert sql and "SELECT" in sql, nome
        assert "{{" not in sql, f"{nome}: filtro do MetricFlow não traduzido"


def test_valor_em_reais_nao_vira_dolar(cat: catalogo.Catalogo) -> None:
    from openmetadata.governanca.metricas import Traducao

    traducao = Traducao(cat)
    corpo = traducao.tipo_e_unidade(traducao.metricas["valor_total_pago"])
    assert corpo["unitOfMeasurement"] == "OTHER"
    assert corpo["customUnitOfMeasurement"] == "BRL"


def test_dbt_test_exclui_a_bronze_do_salic_por_padrao(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openmetadata.dbt_artifacts import _dbt_test_args

    monkeypatch.delenv("OM_DBT_TEST_EXCLUDE", raising=False)
    monkeypatch.delenv("OM_DBT_RUN_TESTS", raising=False)
    args = _dbt_test_args()
    assert args and args[0] == "test"
    assert "path:models/salic_dbt/bronze" in args
    monkeypatch.setenv("OM_DBT_RUN_TESTS", "false")
    assert _dbt_test_args() is None
