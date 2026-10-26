"""Testes dos coletores estáticos de documentação do site (docs-pages/tooling).

A coleta roda offline no CI, lendo arquivos YAML e árvores AST sem subir dbt,
Airflow nem banco. Estes testes protegem a geração do acervo em `src/_data/`
contra schemas malformados, mudanças nas convenções de pastas e falhas isoladas.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Any

import pytest

DOCS_PAGES_DIR = Path(__file__).resolve().parents[1] / "docs-pages"
if str(DOCS_PAGES_DIR) not in sys.path:
    sys.path.insert(0, str(DOCS_PAGES_DIR))

from tooling import collect, common  # type: ignore[import-not-found]  # noqa: E402
from tooling.collectors import airflow_dags, dbt_models  # type: ignore[import-not-found]  # noqa: E402


def _gravar_schema(pasta: Path, conteudo: str, nome: str = "schema.yml") -> Path:
    (pasta / nome).write_text(conteudo, encoding="utf-8")
    return pasta


# ---------------------------------------------------------------------------
# 1. Metadados do dbt (_yaml_da_pasta e _metadados)
# ---------------------------------------------------------------------------


def test_metadados_pasta_sem_schema_retorna_vazio(tmp_path: Path) -> None:
    """Pasta sem schema.yml não falha: os modelos saem apenas sem descrição."""
    assert dbt_models._metadados(tmp_path) == {}


def test_metadados_schema_yaml_invalido_retorna_vazio(tmp_path: Path) -> None:
    """Um schema.yml quebrado não derruba a coleta: a pasta só fica sem metadados."""
    _gravar_schema(tmp_path, "models: [invalido: : :")
    assert dbt_models._metadados(tmp_path) == {}


@pytest.mark.parametrize(
    "conteudo_vazio",
    [
        "",
        "version: 2\n",
        "version: 2\nmodels: null\n",
        "version: 2\nmodels: []\n",
    ],
)
def test_metadados_schema_sem_modelos_retorna_vazio(
    tmp_path: Path, conteudo_vazio: str
) -> None:
    """Arquivo vazio ou com models nulo/vazio é tratado como pasta sem modelos."""
    _gravar_schema(tmp_path, conteudo_vazio)
    assert dbt_models._metadados(tmp_path) == {}


def test_metadados_extrai_modelo_completo_com_chave_tests(tmp_path: Path) -> None:
    """Limpa espaços das descrições e soma testes do modelo e das colunas."""
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - name: stg_agentes
    description: "  Agentes culturais cadastrados.  "
    tests:
      - unique
      - not_null
    columns:
      - name: id_agente
        description: " Identificador único "
        tests:
          - not_null
      - name: nome
        description: ""
""",
    )
    resultado = dbt_models._metadados(tmp_path)
    assert resultado == {
        "stg_agentes": {
            "descricao": "Agentes culturais cadastrados.",
            "colunas": [
                {"nome": "id_agente", "descricao": "Identificador único"},
                {"nome": "nome", "descricao": ""},
            ],
            "testes": 3,
        }
    }


def test_metadados_modelo_minimo_sem_colunas_e_sem_testes(tmp_path: Path) -> None:
    """Modelo só com name entra no catálogo com descrição vazia e zero testes."""
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - name: dim_uf
""",
    )
    assert dbt_models._metadados(tmp_path) == {
        "dim_uf": {
            "descricao": "",
            "colunas": [],
            "testes": 0,
        }
    }


def test_metadados_ignora_item_nao_dicionario_na_lista_models(
    tmp_path: Path,
) -> None:
    """Entrada não-dict em models é ignorada sem descartar os demais modelos."""
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - "item_invalido_string"
  - name: modelo_valido
""",
    )
    resultado = dbt_models._metadados(tmp_path)
    assert list(resultado.keys()) == ["modelo_valido"]


@pytest.mark.parametrize("campo_nome", ["", 'name: ""', "name: null"])
def test_metadados_ignora_dicionario_sem_nome_preenchido(
    tmp_path: Path, campo_nome: str
) -> None:
    """Bloco de modelo sem name preenchido não gera chave vazia no inventário."""
    _gravar_schema(
        tmp_path,
        f"""
version: 2
models:
  - description: "Modelo sem nome válido"
    {campo_nome}
""",
    )
    assert dbt_models._metadados(tmp_path) == {}


def test_metadados_contabiliza_data_tests_e_ignora_coluna_nao_dict(
    tmp_path: Path,
) -> None:
    """Conta data_tests (dbt 1.8+) e ignora coluna não-dict sem derrubar a coleta."""
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - name: fct_captacao
    data_tests:
      - expression_is_true
    columns:
      - "coluna_invalida_string"
      - name: valor_captado
        data_tests:
          - not_null
          - positive_value
""",
    )
    resultado = dbt_models._metadados(tmp_path)
    assert resultado["fct_captacao"]["testes"] == 3
    assert resultado["fct_captacao"]["colunas"] == [
        {"nome": "valor_captado", "descricao": ""}
    ]


def test_metadados_junta_schema_principal_e_schemas_por_banco(tmp_path: Path) -> None:
    """Reúne modelos de schema.yml e schema_<banco>.yml na mesma pasta (salic/)."""
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - name: silver_projeto_rouanet
    description: Modelo consolidado.
""",
        nome="schema.yml",
    )
    _gravar_schema(
        tmp_path,
        """
version: 2
models:
  - name: silver_sac_projetos
    description: Projetos do banco SAC.
    data_tests:
      - not_null
""",
        nome="schema_sac.yml",
    )
    _gravar_schema(
        tmp_path,
        "models: [yaml_quebrado: : :",
        nome="schema_tabelas.yml",
    )

    resultado = dbt_models._metadados(tmp_path)
    assert set(resultado.keys()) == {"silver_projeto_rouanet", "silver_sac_projetos"}
    assert resultado["silver_sac_projetos"]["testes"] == 1


# ---------------------------------------------------------------------------
# 2. Classificação e coleta completa do dbt (_classificar e coletar)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("caminho", "esperado"),
    [
        ("modelo_raiz.sql", ("geral", "outros")),
        (
            "data_products/cultura_em_numeros/eixo1_meta3_x.sql",
            ("cultura_em_numeros", "gold"),
        ),
        ("data_products/resumo.sql", ("geral", "gold")),
        ("salic_dbt/bronze/sac/sac__projetos.sql", ("salic", "bronze")),
        ("salic_dbt/views/vw_projetos.sql", ("salic", "views")),
        ("mapas/silver_agent.sql", ("mapas", "silver")),
        ("mapas/int_agentes.sql", ("mapas", "intermediate")),
        ("mapas/auxiliar.sql", ("mapas", "outros")),
    ],
)
def test_classificar_caminhos_dbt(caminho: str, esperado: tuple[str, str]) -> None:
    """Classifica domínio e camada tanto na árvore legada quanto no padrão ADR 0010."""
    assert dbt_models._classificar(Path(caminho)) == esperado


def test_dbt_coletar_monta_dbt_json_com_totais_meta_e_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Confere o dbt.json: totais do site, campo meta na gold e fusão de sources."""
    models_dir = tmp_path / "dbt" / "minc" / "models"
    data_dir = tmp_path / "docs-pages" / "src" / "_data"
    monkeypatch.setattr(common, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(common, "DATA_DIR", data_dir)
    monkeypatch.setattr(dbt_models, "MODELS_DIR", models_dir)

    # Sources divididas em mais de um arquivo para o mesmo par (name, schema),
    # além de um YAML inválido que deve ser ignorado sem derrubar a listagem
    (models_dir / "salic").mkdir(parents=True)
    (models_dir / "sources.yml").write_text(
        """
version: 2
sources:
  - name: mapas
    schema: raw_mapas
    description: Mapas Culturais
    tables:
      - name: agent
        description: Agentes inscritos
""",
        encoding="utf-8",
    )
    (models_dir / "salic" / "sources_agentes.yml").write_text(
        """
version: 2
sources:
  - name: salic
    schema: raw_salic
    description: Bancos de origem do SALIC
    tables:
      - name: agentes
""",
        encoding="utf-8",
    )
    (models_dir / "salic" / "sources_sac.yml").write_text(
        """
version: 2
sources:
  - name: salic
    schema: raw_salic
    description: Descrição secundária ignorada
    tables:
      - name: projetos
        description: Projetos culturais
""",
        encoding="utf-8",
    )
    (models_dir / "salic" / "sources_invalido.yml").write_text(
        "sources: [yaml_quebrado: : :",
        encoding="utf-8",
    )

    # Modelo silver com source(), materialização explícita e 1 coluna descrita de 2
    (models_dir / "salic" / "silver_sac_projetos.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "select * from {{ source('salic', 'projetos') }}\n",
        encoding="utf-8",
    )
    _gravar_schema(
        models_dir / "salic",
        """
version: 2
models:
  - name: silver_sac_projetos
    description: Projetos normalizados.
    columns:
      - name: pronac
        description: Identificador PRONAC
        data_tests:
          - unique
          - not_null
      - name: nome
        description: ""
""",
        nome="schema_sac.yml",
    )

    # Modelos gold: um ligado à Meta 3 pelo prefixo eixo<N>_meta<M>_ e outro sem meta
    gold_dir = models_dir / "data_products" / "cultura_em_numeros"
    gold_dir.mkdir(parents=True)
    (gold_dir / "eixo2_meta3_cobertura_pagamento.sql").write_text(
        "{{ config(materialized='view') }}\n"
        "select * from {{ ref('silver_sac_projetos') }}\n",
        encoding="utf-8",
    )
    (gold_dir / "resumo_executivo.sql").write_text("select 1\n", encoding="utf-8")
    _gravar_schema(
        gold_dir,
        """
version: 2
models:
  - name: eixo2_meta3_cobertura_pagamento
    description: Cobertura de pagamentos da Meta 3.
    data_tests:
      - not_null
""",
    )

    payload = dbt_models.coletar()
    assert common.read_json("dbt") == payload

    assert payload["totais"] == {
        "modelos": 3,
        "testes": 3,
        "com_descricao": 2,
        "sources": 3,
    }

    por_nome = {m["nome"]: m for m in payload["modelos"]}
    gold_meta = por_nome["eixo2_meta3_cobertura_pagamento"]
    assert gold_meta["dominio"] == "cultura_em_numeros"
    assert gold_meta["camada"] == "gold"
    assert gold_meta["meta"] == 3
    assert gold_meta["depende_de"] == ["silver_sac_projetos"]
    assert gold_meta["materializacao"] == "view"

    assert por_nome["resumo_executivo"]["meta"] is None

    silver = por_nome["silver_sac_projetos"]
    assert silver["meta"] is None
    assert silver["sources"] == ["salic.projetos"]
    assert silver["colunas_descritas"] == 1
    assert silver["materializacao"] == "table"
    assert silver["caminho"] == "dbt/minc/models/salic/silver_sac_projetos.sql"

    sources_por_nome = {s["nome"]: s for s in payload["sources"]}
    assert set(sources_por_nome.keys()) == {"mapas", "salic"}
    assert sources_por_nome["salic"]["descricao"] == "Bancos de origem do SALIC"
    assert [t["nome"] for t in sources_por_nome["salic"]["tabelas"]] == [
        "agentes",
        "projetos",
    ]


def test_dbt_coletar_sem_pasta_models_grava_acervo_vazio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ausência da pasta models/ gera dbt.json vazio sem interromper a coleta."""
    monkeypatch.setattr(common, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(common, "DATA_DIR", tmp_path / "_data")
    monkeypatch.setattr(dbt_models, "MODELS_DIR", tmp_path / "inexistente")

    esperado: dict[str, Any] = {
        "modelos": [],
        "dominios": {},
        "sources": [],
        "totais": {},
    }
    assert dbt_models.coletar() == esperado
    assert common.read_json("dbt") == esperado


# ---------------------------------------------------------------------------
# 3. Inspeção estática das DAGs (airflow_dags._argumentos_do_dag)
# ---------------------------------------------------------------------------


def _extrair_dec_e_funcao(codigo: str) -> tuple[ast.expr, ast.FunctionDef]:
    arvore = ast.parse(codigo)
    funcao = next(no for no in arvore.body if isinstance(no, ast.FunctionDef))
    return funcao.decorator_list[0], funcao


def test_argumentos_do_dag_decorador_sem_chamada() -> None:
    """Aceita @dag sem parênteses nem docstring sem falhar na listagem de DAGs."""
    dec, funcao = _extrair_dec_e_funcao(
        """
@dag
def dag_simples():
    pass
"""
    )
    assert airflow_dags._argumentos_do_dag(dec, funcao) == {
        "funcao": "dag_simples",
        "tags": [],
        "schedule": "",
        "descricao": "",
    }


def test_argumentos_do_dag_literais_e_docstring_multiparagrafo() -> None:
    """Extrai apenas o primeiro parágrafo da docstring para o resumo do catálogo."""
    dec, funcao = _extrair_dec_e_funcao(
        '''
@dag(schedule="@daily", tags=["minc", "salic"], catchup=False)
def dag_salic():
    """Primeira linha do resumo
    continuação do primeiro parágrafo.

    Segundo parágrafo ignorado.
    """
    pass
'''
    )
    assert airflow_dags._argumentos_do_dag(dec, funcao) == {
        "funcao": "dag_salic",
        "tags": ["minc", "salic"],
        "schedule": "@daily",
        "descricao": "Primeira linha do resumo continuação do primeiro parágrafo.",
    }


def test_argumentos_do_dag_schedule_dinamico_por_funcao() -> None:
    """Marca como dinâmico o schedule resolvido por função (schedule_loader)."""
    dec, funcao = _extrair_dec_e_funcao(
        """
@dag(schedule=get_dynamic_schedule("salic"))
def dag_dinamica():
    pass
"""
    )
    resultado = airflow_dags._argumentos_do_dag(dec, funcao)
    assert resultado["schedule"] == "dinâmico (schedule_loader)"


def test_argumentos_do_dag_schedule_e_tags_por_variavel_constante() -> None:
    """Variáveis não literais no decorador viram vazios sem quebrar o parse AST."""
    dec, funcao = _extrair_dec_e_funcao(
        """
@dag(schedule=CRON_VAR, tags=TAGS_VAR)
def dag_variavel():
    pass
"""
    )
    resultado = airflow_dags._argumentos_do_dag(dec, funcao)
    assert resultado["schedule"] == ""
    assert resultado["tags"] == []


# ---------------------------------------------------------------------------
# 4. Orquestração e leitura/escrita do acervo (collect.main e common)
# ---------------------------------------------------------------------------


def test_collect_main_isola_excecao_de_coletor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Falha em um coletor não impede a execução dos demais coletores do site."""
    chamados: list[str] = []

    def coletor_ok() -> None:
        chamados.append("ok")

    def coletor_falho() -> None:
        raise RuntimeError("erro simulado")

    monkeypatch.setattr(
        collect,
        "COLETORES",
        (("dbt", coletor_falho), ("airflow", coletor_ok)),
    )
    assert collect.main() == 0
    assert chamados == ["ok"]


def test_common_write_and_read_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Acervo ausente retorna dict vazio e gravação persiste o JSON em _data."""
    monkeypatch.setattr(common, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(common, "DATA_DIR", tmp_path / "_data")

    assert common.read_json("inexistente") == {}
    common.write_json("teste", {"chave": "valor"})
    assert common.read_json("teste") == {"chave": "valor"}
