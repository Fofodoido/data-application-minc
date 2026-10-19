"""Guardas de empacotamento da ingestao do OpenMetadata.

Estes testes existem por causa de duas armadilhas concretas da migracao do
data-application-cidades (Airflow 2.8 + @task.virtualenv) para ca (Airflow 3.2
com o pacote instalado na imagem).
"""

import ast
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
OPENMETADATA_DIR = REPO_ROOT / "helpers/openmetadata"
RECIPES_DIR = OPENMETADATA_DIR / "recipes"

PROJETO_DBT = REPO_ROOT / "dbt/minc"
DAG_PATH = REPO_ROOT / "dags/openmetadata_ingestion_dag.py"


def _schemas_do_projeto_dbt() -> set[str]:
    """Schemas que o projeto dbt declara como source ou produz como modelo.

    Derivado, e nao escrito a mao, porque a lista mudou junto com o projeto: os
    sources do SALIC entraram e as recipes ficaram para tras sem nada acusar.
    Aqui a divergencia vira erro de teste.

    Nao ha prefixo `minc_` nos schemas produzidos porque
    dbt/minc/macros/get_custom_schema.sql usa generate_schema_name_for_env e o
    target padrao e `prod`, que aplica o custom schema literal.
    """
    schemas: set[str] = set()

    for caminho in (PROJETO_DBT / "models").rglob("*.yml"):
        conteudo = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
        for source in conteudo.get("sources") or []:
            schemas.add(source.get("schema", source["name"]))

    projeto = yaml.safe_load(
        (PROJETO_DBT / "dbt_project.yml").read_text(encoding="utf-8")
    )

    def coletar(no: object) -> None:
        if isinstance(no, dict):
            if "+schema" in no:
                schemas.add(no["+schema"])
            for valor in no.values():
                coletar(valor)

    coletar(projeto.get("models", {}))
    return {f"^{nome}$" for nome in schemas}


SCOPED_RECIPES = (
    "postgres_metadata.yaml",
    "postgres_profiler.yaml",
    "postgres_classifier.yaml",
)


def _load_recipe(name: str) -> dict:
    return yaml.safe_load((RECIPES_DIR / name).read_text(encoding="utf-8"))


def test_stubs_do_not_shadow_the_real_openmetadata_package() -> None:
    """`helpers/` vem antes do site-packages no PYTHONPATH.

    O projeto de origem carregava stubs em `helpers/metadata/` para o codigo da
    DAG conseguir anotar linhagem sem ter o openmetadata-ingestion instalado.
    Aqui o pacote real existe, entao um `helpers/metadata/` mascararia
    `metadata.ingestion...` -- e o sintoma seria um AttributeError distante, nao
    um ImportError obvio.
    """
    assert not (REPO_ROOT / "helpers/metadata").exists(), (
        "helpers/metadata/ mascara o pacote `metadata` do openmetadata-ingestion, "
        "porque PYTHONPATH poe /opt/airflow/helpers antes do site-packages."
    )


def test_virtualenv_scaffolding_did_not_come_along() -> None:
    """Nada aqui deve depender de @task.virtualenv.

    O virtualenv era necessario no Airflow 2.8, onde o openmetadata-ingestion
    1.12.1 (sqlalchemy<2) nao podia coexistir com o Airflow. Com 1.13.3.2 sobre
    Airflow 3.2.2 os dois convivem, e as sobras dessa epoca so confundem.
    """
    assert not (OPENMETADATA_DIR / "airflow_log_config.py").exists(), (
        "airflow_log_config.py so existia para o Airflow inicializar dentro do "
        "virtualenv isolado."
    )

    # Olha a arvore sintatica, nao o texto: os docstrings destes modulos citam
    # `@task.virtualenv` e `OPENMETADATA_REQUIREMENTS` de proposito, para
    # registrar o que a migracao removeu.
    for path in sorted(OPENMETADATA_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

        for node in ast.walk(tree):
            for decorator in getattr(node, "decorator_list", []):
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                assert (
                    ast.unparse(target) != "task.virtualenv"
                ), f"{path.name} ainda decora com @task.virtualenv"

            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                assert node.id != "OPENMETADATA_REQUIREMENTS", (
                    f"{path.name} ainda instala dependencias em runtime; elas "
                    "pertencem a infra/docker/airflow/requirements.lock.txt"
                )


@pytest.mark.parametrize("recipe_name", SCOPED_RECIPES)
def test_scoped_recipes_share_the_same_schema_filter(recipe_name: str) -> None:
    """Profiler e classifier so enxergam o que postgres_metadata catalogou.

    Se as tres listas divergirem, o profiler passa a medir tabela que o
    metadata nunca publicou -- e o erro aparece como tabela ausente no
    OpenMetadata, longe da causa.
    """
    config = _load_recipe(recipe_name)["source"]["sourceConfig"]["config"]
    includes = set(config["schemaFilterPattern"]["includes"])
    esperado = _schemas_do_projeto_dbt()

    faltando = esperado - includes
    sobrando = includes - esperado

    assert not faltando, (
        f"{recipe_name} nao cataloga {sorted(faltando)}, que o projeto dbt declara. "
        "As tabelas ficam de fora do OpenMetadata sem nenhum aviso."
    )
    assert (
        not sobrando
    ), f"{recipe_name} filtra {sorted(sobrando)}, que o projeto dbt nao conhece mais."


def test_recipes_do_not_carry_hardcoded_credentials() -> None:
    """Toda credencial entra por replacement, nunca literal no YAML."""
    for recipe_path in sorted(RECIPES_DIR.glob("*.yaml")):
        raw = recipe_path.read_text(encoding="utf-8")
        config = yaml.safe_load(raw)

        token = config["workflowConfig"]["openMetadataServerConfig"]["securityConfig"][
            "jwtToken"
        ]
        assert token.startswith("${") and token.endswith(
            "}"
        ), f"{recipe_path.name} tem jwtToken literal"

        for secret_key in ("password", "jwtToken"):
            for line in raw.splitlines():
                stripped = line.strip()
                if not stripped.startswith(f"{secret_key}:"):
                    continue
                value = stripped.split(":", 1)[1].strip()
                assert value.startswith(
                    "${"
                ), f"{recipe_path.name} tem {secret_key} literal: {stripped}"


def test_metadata_rest_sinks_use_bounded_batches() -> None:
    """Lote grande estoura o timeout do proxy na frente do OpenMetadata."""
    for recipe_path in sorted(RECIPES_DIR.glob("*.yaml")):
        config = yaml.safe_load(recipe_path.read_text(encoding="utf-8"))
        sink = config["sink"]

        assert sink["type"] == "metadata-rest"
        assert (
            sink["config"]["bulk_sink_batch_size"] == 10
        ), f"{recipe_path.name} nao limita o lote do sink"


def test_classifier_does_not_persist_raw_sample_rows() -> None:
    """O classifier pode ler valores para achar PII, mas nao publica linha bruta."""
    config = _load_recipe("postgres_classifier.yaml")["source"]["sourceConfig"]["config"]

    assert config["storeSampleData"] is False


def test_dbt_recipe_targets_the_same_service_as_postgres() -> None:
    """Nomes diferentes fazem o dbt anexar descricoes a um servico inexistente."""
    dbt_service = _load_recipe("dbt_metadata.yaml")["source"]["serviceName"]
    postgres_service = _load_recipe("postgres_metadata.yaml")["source"]["serviceName"]

    assert dbt_service == postgres_service


def _load_config(monkeypatch, **env: str):
    """Recarrega config.py com um ambiente controlado.

    Os flags sao lidos no import, entao trocar os.environ depois nao muda nada:
    o modulo precisa ser reimportado.
    """
    import importlib

    monkeypatch.setenv("AIRFLOW_REPO_BASE", "/opt/airflow")
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    import openmetadata.config as config_module

    return importlib.reload(config_module)


def test_superset_recipe_is_off_by_default(monkeypatch) -> None:
    """O MinC nao tem Superset; a recipe so entra quando alguem ligar."""
    monkeypatch.delenv("OM_INGEST_SUPERSET", raising=False)
    config = _load_config(monkeypatch)

    habilitadas = {recipe.task_id for recipe in config.ALL_RECIPES}

    assert "superset_metadata" not in habilitadas
    assert "postgres_metadata" in habilitadas
    assert "dbt_metadata" in habilitadas


def test_flag_turns_a_recipe_back_on(monkeypatch) -> None:
    config = _load_config(monkeypatch, OM_INGEST_SUPERSET="true")

    assert "superset_metadata" in {recipe.task_id for recipe in config.ALL_RECIPES}


def test_disabling_a_middle_recipe_keeps_the_chain_whole(monkeypatch) -> None:
    """Desligar uma recipe do meio nao pode deixar a corrente com buraco.

    Os flags sao ligados explicitamente aqui: o teste verifica o contrato entre
    ALL_RECIPES e RECIPE_PIPELINE, nao qual recipe vem ligada por padrao -- esse
    default e decisao operacional e muda.
    """
    config = _load_config(
        monkeypatch,
        OM_INGEST_AIRFLOW="true",
        OM_INGEST_PROFILER="false",
        OM_INGEST_CLASSIFIER="true",
        OM_INGEST_SUPERSET="false",
    )

    habilitadas = {recipe.task_id for recipe in config.ALL_RECIPES}
    encadeadas = [task_id for task_id in config.RECIPE_PIPELINE if task_id in habilitadas]

    assert "postgres_profiler" not in habilitadas
    assert encadeadas == [
        "airflow_metadata",
        "postgres_metadata",
        "dbt_metadata",
        "postgres_classifier",
    ]
    # Toda recipe habilitada precisa ter lugar na ordem, senao vira task solta.
    # A testsuite tem o dela: depois da governanca, que cria os casos de teste.
    assert habilitadas == set(encadeadas) | set(config.RECIPES_DEPOIS_DA_GOVERNANCA)


def test_every_defined_recipe_has_a_place_in_the_pipeline() -> None:
    """Nenhuma recipe pode existir sem posicao definida na ordem."""
    import openmetadata.config as config

    definidas = {recipe.task_id for recipe in config._DEFINED_RECIPES}

    assert definidas == set(config.RECIPE_PIPELINE) | set(
        config.RECIPES_DEPOIS_DA_GOVERNANCA
    )


def test_dbt_project_dir_comes_from_the_environment(monkeypatch) -> None:
    """O caminho do projeto dbt e configuravel, com default para o do MinC."""
    padrao = _load_config(monkeypatch)
    assert padrao.DBT_MINC_DIR == "/opt/airflow/dbt/minc"

    outro = _load_config(monkeypatch, OM_DBT_PROJECT_DIR="dbt/outro")
    assert outro.DBT_MINC_DIR == "/opt/airflow/dbt/outro"
    assert outro.DBT_METADATA_RECIPE.dbt_project_dir == "/opt/airflow/dbt/outro"


def test_metadata_recipe_never_marks_missing_tables_as_deleted() -> None:
    """`markDeletedTables` tem default `true`, e ele apaga catalogo.

    Uma carga parcial -- VPN caindo, ambiente restaurado pela metade, schema
    ainda nao materializado -- marca como deletado tudo que o catalogo tem e o
    banco nao, sem aviso. Com os 571 modelos SALIC como view, o custo de uma
    execucao contra banco incompleto e o catalogo inteiro.
    """
    config = _load_recipe("postgres_metadata.yaml")["source"]["sourceConfig"]["config"]

    assert config.get("markDeletedTables") is False, (
        "postgres_metadata.yaml precisa de `markDeletedTables: false` explicito; "
        "o default do conector e `true`."
    )


def test_only_the_metadata_recipe_declares_deletion_behaviour() -> None:
    """Profiler e classifier nao sao `DatabaseMetadata` e nao apagam nada.

    Declarar a chave neles daria a impressao de que ha tres lugares para
    proteger, e o dia em que um for esquecido ninguem saberia qual importava.
    """
    for recipe_path in sorted(RECIPES_DIR.glob("*.yaml")):
        raw = recipe_path.read_text(encoding="utf-8")
        if recipe_path.name == "postgres_metadata.yaml":
            continue
        assert "markDeletedTables" not in raw, (
            f"{recipe_path.name} declara markDeletedTables; a chave so tem efeito "
            "em sourceConfig do tipo DatabaseMetadata."
        )


def test_catalog_recipes_are_on_by_default(monkeypatch) -> None:
    """postgres_metadata e dbt_metadata sao o trabalho da DAG, nao um extra."""
    for flag in ("OM_INGEST_POSTGRES", "OM_INGEST_DBT"):
        monkeypatch.delenv(flag, raising=False)
    config = _load_config(monkeypatch)

    habilitadas = {recipe.task_id for recipe in config.ALL_RECIPES}
    assert {"postgres_metadata", "dbt_metadata"} <= habilitadas


@pytest.mark.parametrize(
    ("flag", "task_id"),
    [
        ("OM_INGEST_POSTGRES", "postgres_metadata"),
        ("OM_INGEST_DBT", "dbt_metadata"),
    ],
)
def test_catalog_recipes_can_be_isolated(monkeypatch, flag: str, task_id: str) -> None:
    """Desligar uma das duas serve para isolar problema, e precisa funcionar."""
    config = _load_config(monkeypatch, **{flag: "false"})

    assert task_id not in {recipe.task_id for recipe in config.ALL_RECIPES}


def test_profiler_is_off_and_classifier_is_on_by_default(monkeypatch) -> None:
    """A diferenca entre os dois nao e arbitraria.

    O classifier roda com `storeSampleData: false` e nunca persiste linha
    bruta. O profiler publica min, max e distribuicao -- estatistica reveladora
    num banco com CPF, CNPJ e dados de raca e deficiencia. Ele so entra depois
    que as exclusoes de coluna sensivel estiverem verificadas.
    """
    for flag in ("OM_INGEST_PROFILER", "OM_INGEST_CLASSIFIER"):
        monkeypatch.delenv(flag, raising=False)
    config = _load_config(monkeypatch)

    habilitadas = {recipe.task_id for recipe in config.ALL_RECIPES}
    assert "postgres_profiler" not in habilitadas
    assert "postgres_classifier" in habilitadas


@pytest.mark.parametrize(
    "flag",
    [
        "OM_INGEST_POSTGRES",
        "OM_INGEST_DBT",
        "OM_INGEST_GLOSSARY",
        "OM_SYNC_GOVERNANCA",
        "OM_INGEST_TESTSUITE",
        "OM_DBT_RUN_TESTS",
    ],
)
def test_declared_flags_reach_the_container(flag: str) -> None:
    """Flag que o config le mas o compose nao passa e flag morta.

    Ela e lida no parse da DAG, dentro do container: se nao estiver na
    `environment:`, o default do codigo vence e mudar o `.env` nao muda nada.
    """
    compose = (REPO_ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")

    assert f"{flag}:" in compose, f"{flag} nao chega ao container pelo compose"


def test_glossary_runs_before_every_recipe() -> None:
    """A ordem e obrigatoria, e o motivo nao aparece em nenhum erro.

    `meta.openmetadata.glossary` nos schema.yml apenas REFERENCIA termo por
    FQN. Se o termo ainda nao existe no servidor quando a recipe roda, a
    referencia e descartada em silencio e o ativo chega ao catalogo sem o
    vinculo -- nenhuma task falha.

    O teste le a arvore sintatica: importar a DAG exigiria Airflow configurado.
    """
    tree = ast.parse(DAG_PATH.read_text(encoding="utf-8"), filename=str(DAG_PATH))
    fonte = DAG_PATH.read_text(encoding="utf-8")

    tarefas = {
        no.name
        for no in ast.walk(tree)
        if isinstance(no, ast.FunctionDef)
        and any(
            "task" in ast.unparse(d.func if isinstance(d, ast.Call) else d)
            for d in no.decorator_list
        )
    }
    assert "sync_glossary" in tarefas, (
        "a DAG nao tem task de glossario; sem ela, editar glossaries/minc.csv "
        "nao chega ao servidor e os FQNs dos schema.yml apontam para o vazio."
    )

    posicao_glossario = fonte.index("previous_task = (")
    posicao_encadeamento = fonte.index("for t in RECIPE_PIPELINE")
    assert (
        posicao_glossario < posicao_encadeamento
    ), "o glossario precisa iniciar a corrente, antes das recipes."


def test_governanca_fecha_a_corrente_mesmo_com_falha_antes() -> None:
    """O conector dbt apaga a certificacao; a governanca e quem a devolve.

    Por isso ela vem depois de todas as recipes de catalogo e com
    `trigger_rule="all_done"`: presa a `all_success`, uma falha em qualquer
    recipe deixaria as tabelas sem certificacao ate a proxima execucao boa.
    """
    fonte = DAG_PATH.read_text(encoding="utf-8")

    assert fonte.index("for t in RECIPE_PIPELINE") < fonte.index(
        "[reaplicar_governanca()]"
    )
    assert fonte.index("[reaplicar_governanca()]") < fonte.index(
        "RECIPES_DEPOIS_DA_GOVERNANCA if t in recipe_tasks"
    ), "a testsuite precisa vir depois da governanca, que cria os casos de teste"
    assert 'task_id=GOVERNANCA_TASK_ID, trigger_rule="all_done"' in fonte
