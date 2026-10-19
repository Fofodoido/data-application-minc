import os
from dataclasses import dataclass, field
from typing import Mapping

AIRFLOW_REPO_BASE = os.environ["AIRFLOW_REPO_BASE"]

OPENMETADATA_RECIPES_DIR = f"{AIRFLOW_REPO_BASE}/helpers/openmetadata/recipes"
# Relativo a AIRFLOW_REPO_BASE, que ja absorve o layout diferente de homolog/prod.
OM_DBT_PROJECT_DIR = os.environ.get("OM_DBT_PROJECT_DIR", "dbt/minc").strip("/")
DBT_MINC_DIR = f"{AIRFLOW_REPO_BASE}/{OM_DBT_PROJECT_DIR}"


@dataclass(frozen=True)
class RecipeDefinition:
    task_id: str
    recipe_path: str
    command: str
    replacements: Mapping[str, str]
    variaveis: Mapping[str, str] = field(default_factory=dict)
    dbt_project_dir: str = ""
    enabled: bool = True


def _flag(name: str, *, default: bool) -> bool:
    """Liga/desliga uma recipe por variavel de ambiente.

    E variavel de ambiente, e nao Variable do Airflow, porque a decisao acontece
    no parse da DAG: buscar Variable a cada parse bate no banco de metadados a
    cada poucos segundos. O custo e precisar recriar o container para mudar o
    flag, o que e aceitavel para algo que muda quando a infraestrutura muda.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "sim"}


INGEST_SUPERSET = _flag("OM_INGEST_SUPERSET", default=False)

INGEST_AIRFLOW = _flag("OM_INGEST_AIRFLOW", default=True)

# O par que constroi o catalogo de dados: postgres_metadata cria os ativos e
# dbt_metadata anexa descricao, tag e linhagem. Ligados por padrao porque sem
# eles a DAG nao faz o trabalho para o qual existe -- o flag serve para isolar
# uma das duas durante investigacao, nao para operacao normal.
INGEST_POSTGRES = _flag("OM_INGEST_POSTGRES", default=True)
INGEST_DBT = _flag("OM_INGEST_DBT", default=True)

# Desligado por padrao, e a diferenca entre os dois nao e arbitraria: o
# classifier roda com `storeSampleData: false` e nunca persiste linha bruta,
# enquanto o profiler publica min, max e distribuicao -- que sao estatisticas
# reveladoras num repositorio com CPF, CNPJ e dados de raca e deficiencia.
# O profiler so deve ser ligado depois que as exclusoes de coluna sensivel
# estiverem verificadas.
INGEST_PROFILER = _flag("OM_INGEST_PROFILER", default=False)
INGEST_CLASSIFIER = _flag("OM_INGEST_CLASSIFIER", default=True)

# O glossario e pre-requisito, nao recipe: `meta.openmetadata.glossary` nos
# schema.yml apenas REFERENCIA termo por FQN, e quem cria o termo e este sync.
# Referencia a FQN inexistente nao falha a ingestao -- ela e ignorada em
# silencio, e o modelo chega ao catalogo sem o vinculo.
INGEST_GLOSSARY = _flag("OM_INGEST_GLOSSARY", default=True)

OPENMETADATA_GLOSSARIES_DIR = f"{AIRFLOW_REPO_BASE}/helpers/openmetadata/glossaries"
GLOSSARY_DEFINITION_PATH = os.environ.get(
    "OM_GLOSSARY_PATH", f"{OPENMETADATA_GLOSSARIES_DIR}/minc.yaml"
)
GLOSSARY_TASK_ID = "sync_glossary"

# A governanca declarada em dbt/minc/governance/ -- produto, uso, certificacao,
# formato, chaves, metricas, contratos e testes nativos. Roda DEPOIS de todas
# as recipes, sempre: o conector dbt apaga a certificacao das tabelas, e e esta
# etapa que a devolve. Desligar e para investigacao, nao para operacao.
SYNC_GOVERNANCA = _flag("OM_SYNC_GOVERNANCA", default=True)
GOVERNANCA_TASK_ID = "reaplicar_governanca"

# Executa os testes nativos declarados em qualidade.yml. Le dado do banco
# (regex por coluna, contagem de linhas), por isso fica so nas camadas silver,
# intermediate e gold -- nunca na bronze de 157 M de linhas do SALIC.
INGEST_TESTSUITE = _flag("OM_INGEST_TESTSUITE", default=True)
INCIDENTES_TASK_ID = "classificar_incidentes"


COMMON_REPLACEMENTS = {
    "DB_DW_HOST": os.environ.get("DB_DW_HOST", "postgres"),
    "DB_DW_PORT": os.environ.get("DB_DW_PORT", "5432"),
    "DB_DW_USER": os.environ.get("DB_DW_USER", "postgres_dw"),
    "DB_DW_PASSWORD": os.environ.get("DB_DW_PASSWORD", "postgres_dw"),
    "DB_DW_DBNAME": os.environ.get("DB_DW_DBNAME", "data_warehouse"),
    "AIRFLOW_HOST_PORT": os.environ.get("AIRFLOW_HOST_PORT", "http://localhost:8080"),
    "AIRFLOW_DB_HOST_PORT": os.environ.get("AIRFLOW_DB_HOST_PORT", "postgres:5432"),
    "AIRFLOW_DB_USERNAME": os.environ.get("AIRFLOW_DB_USERNAME", "postgres"),
    "AIRFLOW_DB_PASSWORD": os.environ.get("AIRFLOW_DB_PASSWORD", "postgres"),
    "AIRFLOW_DB_DATABASE": os.environ.get("AIRFLOW_DB_DATABASE", "airflow"),
}

# Todas as recipes precisam do endereco do servidor.
VARIAVEIS_BASE = {"OM_HOST": "OM_HOST"}

VARIAVEIS_INGESTAO = {**VARIAVEIS_BASE, "INGESTION_TOKEN": "INGESTION_TOKEN"}
VARIAVEIS_PROFILER = {**VARIAVEIS_BASE, "PROFILER_TOKEN": "PROFILER_TOKEN"}
VARIAVEIS_CLASSIFIER = {**VARIAVEIS_BASE, "CLASSIFICATION_TOKEN": "CLASSIFICATION_TOKEN"}
VARIAVEIS_SUPERSET = {
    **VARIAVEIS_INGESTAO,
    "SUPERSET_HOST_PORT": "SUPERSET_HOST_PORT",
    "SUPERSET_USERNAME": "SUPERSET_USERNAME",
    "SUPERSET_PASSWORD": "SUPERSET_PASSWORD",
}

AIRFLOW_METADATA_RECIPE = RecipeDefinition(
    task_id="airflow_metadata",
    enabled=INGEST_AIRFLOW,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/airflow_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_INGESTAO,
)

POSTGRES_METADATA_RECIPE = RecipeDefinition(
    task_id="postgres_metadata",
    enabled=INGEST_POSTGRES,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_INGESTAO,
)

POSTGRES_PROFILER_RECIPE = RecipeDefinition(
    task_id="postgres_profiler",
    enabled=INGEST_PROFILER,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_profiler.yaml",
    command="profile",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_PROFILER,
)

POSTGRES_CLASSIFIER_RECIPE = RecipeDefinition(
    task_id="postgres_classifier",
    enabled=INGEST_CLASSIFIER,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_classifier.yaml",
    command="classify",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_CLASSIFIER,
)

SUPERSET_METADATA_RECIPE = RecipeDefinition(
    task_id="superset_metadata",
    enabled=INGEST_SUPERSET,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/superset_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_SUPERSET,
)

POSTGRES_TESTSUITE_RECIPE = RecipeDefinition(
    task_id="postgres_testsuite",
    # Sem a governanca nao ha caso de teste nativo, e a task ficaria orfa.
    enabled=INGEST_TESTSUITE and SYNC_GOVERNANCA,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/postgres_testsuite.yaml",
    command="test",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_INGESTAO,
)

DBT_METADATA_RECIPE = RecipeDefinition(
    task_id="dbt_metadata",
    enabled=INGEST_DBT,
    recipe_path=f"{OPENMETADATA_RECIPES_DIR}/dbt_metadata.yaml",
    command="ingest",
    replacements=COMMON_REPLACEMENTS,
    variaveis=VARIAVEIS_INGESTAO,
    dbt_project_dir=DBT_MINC_DIR,
)

_DEFINED_RECIPES = (
    AIRFLOW_METADATA_RECIPE,
    POSTGRES_METADATA_RECIPE,
    POSTGRES_PROFILER_RECIPE,
    POSTGRES_CLASSIFIER_RECIPE,
    SUPERSET_METADATA_RECIPE,
    DBT_METADATA_RECIPE,
    POSTGRES_TESTSUITE_RECIPE,
)

# A DAG so enxerga o que esta ligado. Uma recipe desligada nao vira task, entao
# nao aparece na UI como "skipped" -- ela simplesmente nao existe naquela DAG.
ALL_RECIPES = tuple(recipe for recipe in _DEFINED_RECIPES if recipe.enabled)

# postgres_metadata cria as tabelas no catalogo; dbt_metadata anexa descricoes
# e linhagem; profiler e classifier so tem o que medir depois disso.
RECIPE_PIPELINE = (
    "airflow_metadata",
    "postgres_metadata",
    "dbt_metadata",
    "postgres_profiler",
    "postgres_classifier",
    "superset_metadata",
)

# Depois das recipes de catalogo, a governanca cria os casos de teste nativos;
# so entao a suite deles tem o que executar. Por isso a testsuite nao esta em
# RECIPE_PIPELINE: ela vem depois da task de governanca.
RECIPES_DEPOIS_DA_GOVERNANCA = ("postgres_testsuite",)
