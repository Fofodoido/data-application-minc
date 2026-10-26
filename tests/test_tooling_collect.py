"""Testes unitários do pipeline de coleta da documentação (docs-pages/tooling).

Avalia os coletores estáticos de metadados do dbt (`dbt_models._metadados` e
`dbt_models._classificar`), o inspetor AST de DAGs (`airflow_dags._argumentos_do_dag`)
e o orquestrador resiliente (`collect.main`), combinando técnicas Caixa-Preta
(Particionamento de Equivalência e Valor Limite) e Caixa-Branca (Branch e MC/DC).
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

DOCS_PAGES_DIR = Path(__file__).resolve().parents[1] / "docs-pages"
if str(DOCS_PAGES_DIR) not in sys.path:
    sys.path.insert(0, str(DOCS_PAGES_DIR))

from tooling import collect, common  # type: ignore[import-not-found]  # noqa: E402
from tooling.collectors import airflow_dags, dbt_models  # type: ignore[import-not-found]  # noqa: E402


def _gravar_schema(pasta: Path, conteudo: str) -> Path:
    (pasta / "schema.yml").write_text(conteudo, encoding="utf-8")
    return pasta


# ---------------------------------------------------------------------------
# 1. dbt_models._metadados (Caixa-Preta: PE + AVL | Caixa-Branca: Branch + MC/DC)
# ---------------------------------------------------------------------------


def test_metadados_pasta_sem_schema_retorna_vazio(tmp_path: Path) -> None:
    """[CT01 - CE2 / AVL] Diretório sem schema.yml devolve dicionário vazio."""
    assert dbt_models._metadados(tmp_path) == {}


def test_metadados_schema_yaml_invalido_retorna_vazio(tmp_path: Path) -> None:
    """[CT02 - CE3] Arquivo schema.yml com sintaxe YAML corrompida devolve vazio."""
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
    """[CT03 - CE5 / AVL: 0 modelos] Arquivo vazio ou lista `models` vazia."""
    _gravar_schema(tmp_path, conteudo_vazio)
    assert dbt_models._metadados(tmp_path) == {}


def test_metadados_extrai_modelo_completo_com_chave_tests(tmp_path: Path) -> None:
    """[CT04 - CE1, CE4, CE6, CE9, CE12 | MC/DC D1(F,F), D2(T,F), D3(T,F), D4=T]
    Extrai descrição com strip(), colunas válidas e soma testes de modelo e coluna.
    """
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
    """[CT05 - CE10, CE14 / AVL: 0 colunas, 0 testes | MC/DC D1(F,F), D2(F,F)]
    Modelo válido contendo apenas o nome obrigatório.
    """
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
    """[CT06 - CE7 | MC/DC D1 Par de Independência da Condição A: (T, _)]
    Elemento em `models` que não é dict faz `not isinstance(modelo, dict)` ser True.
    """
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
    """[CT07 - CE8 / AVL len(name)==0 | MC/DC D1 Par de Independência de B: (F, T)]
    Elemento é dict (A=False), mas `name` é ausente, nulo ou vazio (B=True).
    """
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
    """[CT08 - CE11, CE13 | MC/DC D2(F,T), D3(F,T) e Branch D4=False]
    Soma a sintaxe nova `data_tests` quando `tests` está ausente e filtra
    elementos inválidos dentro da lista `columns`.
    """
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


# ---------------------------------------------------------------------------
# 2. airflow_dags._argumentos_do_dag (Caixa-Branca: MC/DC + Caixa-Preta)
# ---------------------------------------------------------------------------


def _extrair_dec_e_funcao(codigo: str) -> tuple[ast.expr, ast.FunctionDef]:
    arvore = ast.parse(codigo)
    funcao = next(no for no in arvore.body if isinstance(no, ast.FunctionDef))
    return funcao.decorator_list[0], funcao


def test_argumentos_do_dag_decorador_sem_chamada() -> None:
    """[CT09 - Branch isinstance(dec, ast.Call) == False]
    Decorador `@dag` sem parênteses e função sem docstring.
    """
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
    """[CT10 - MC/DC Decisão schedule dinâmico (F, F) -> False]
    Extrai o 1º parágrafo da docstring e converte `tags` e `schedule` literais.
    """
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
    """[CT11 - MC/DC Decisão schedule dinâmico (T, T) -> True]
    Quando `schedule=get_dynamic_schedule(...)`, `_literal` é None (A=True) e o nó
    é `ast.Call` (B=True), atribuindo 'dinâmico (schedule_loader)'.
    """
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
    """[CT12 - MC/DC Decisão schedule dinâmico (T, F) -> False]
    Quando `schedule=CRON_VAR` (`ast.Name`), `_literal` é None (A=True), mas o nó
    não é `ast.Call` (B=False), resultando em `schedule=""` e `tags=[]`.
    """
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
# 3. Testes adicionais de classificação e resiliência do coletor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("caminho", "esperado"),
    [
        ("modelo_raiz.sql", ("geral", "outros")),
        ("data_products/cultura/indicadores.sql", ("cultura", "gold")),
        ("data_products/resumo.sql", ("geral", "gold")),
        ("salic_dbt/bronze/sac/sac__projetos.sql", ("salic", "bronze")),
        ("mapas/silver_agent.sql", ("mapas", "silver")),
        ("mapas/int_agentes.sql", ("mapas", "intermediate")),
        ("mapas/auxiliar.sql", ("mapas", "outros")),
    ],
)
def test_classificar_caminhos_dbt(caminho: str, esperado: tuple[str, str]) -> None:
    assert dbt_models._classificar(Path(caminho)) == esperado


def test_collect_main_isola_excecao_de_coletor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    monkeypatch.setattr(common, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(common, "DATA_DIR", tmp_path / "_data")

    assert common.read_json("inexistente") == {}
    common.write_json("teste", {"chave": "valor"})
    assert common.read_json("teste") == {"chave": "valor"}
