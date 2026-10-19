"""DAG de ingestao de metadados do MinC no OpenMetadata.

Ordem, e por que ela nao e negociavel:

    sync_glossary          os termos precisam existir antes do conector dbt
      -> recipes           catalogo: Airflow, Postgres, dbt (com `dbt test`),
                           profiler, classifier, Superset
      -> reaplicar_governanca   produto, uso, certificacao, formato, chaves,
                                metricas, contratos e testes nativos
      -> postgres_testsuite     executa os testes nativos criados acima
      -> classificar_incidentes severidade pelo tier da tabela

O codigo de apoio vive em `helpers/openmetadata/`.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task
from schedule_loader import get_dynamic_schedule

from openmetadata.config import (
    ALL_RECIPES,
    GLOSSARY_DEFINITION_PATH,
    GLOSSARY_TASK_ID,
    GOVERNANCA_TASK_ID,
    INCIDENTES_TASK_ID,
    INGEST_GLOSSARY,
    RECIPE_PIPELINE,
    RECIPES_DEPOIS_DA_GOVERNANCA,
    SYNC_GOVERNANCA,
)


def _encadear(anterior: Any, tarefas: list[Any]) -> Any:
    """Liga as tarefas em fila, a partir de `anterior`. Devolve a ultima."""
    for tarefa in tarefas:
        if anterior is not None:
            anterior >> tarefa
        anterior = tarefa
    return anterior


@dag(
    dag_id="openmetadata_ingestion_dag",
    schedule=get_dynamic_schedule("openmetadata_ingestion_dag"),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "owner": "@arthrok",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
    tags=["openmetadata", "dbt", "postgres", "metadata", "minc"],
)
def openmetadata_ingestion_dag() -> None:
    """Executa as recipes do OpenMetadata para o escopo do MinC."""

    @task(task_id="run_openmetadata_recipe_base")
    def run_openmetadata_recipe(
        recipe_path: str,
        command: str,
        replacements: dict,
        variaveis: dict,
        dbt_project_dir: str = "",
    ) -> None:
        from airflow.sdk import Variable

        from openmetadata.runner import (
            run_openmetadata_recipe as execute_openmetadata_recipe,
        )

        resolvidos = dict(replacements)
        for marcador, nome_da_variable in variaveis.items():
            resolvidos[marcador] = Variable.get(nome_da_variable)

        execute_openmetadata_recipe(
            recipe_path=recipe_path,
            command=command,
            replacements=resolvidos,
            dbt_project_dir=dbt_project_dir,
        )

    @task(task_id=GLOSSARY_TASK_ID)
    def sync_glossary(glossary_definition_path: str) -> dict:
        """Cria os termos do glossario MinC que faltam, antes de qualquer recipe.

        Precisa vir ANTES: `meta.openmetadata.glossary` nos schema.yml apenas
        referencia termo por FQN, e uma referencia a FQN que nao existe no
        servidor e descartada em silencio -- o ativo chega ao catalogo sem o
        vinculo. Conteudo e relacoes dos termos que ja existem sao atualizados
        pela `reaplicar_governanca`, no fim.
        """
        from airflow.sdk import Variable

        from openmetadata.glossary import sync_glossary as aplicar_glossario

        return aplicar_glossario(
            glossary_definition_path=glossary_definition_path,
            host_port=Variable.get("OM_HOST"),
            jwt_token=Variable.get("INGESTION_TOKEN"),
        )

    # `all_done`: roda mesmo que uma recipe antes dela falhe, e isso e
    # deliberado. E a unica task que devolve a certificacao que o conector dbt
    # apaga; presa a `all_success`, uma falha em qualquer recipe -- inclusive
    # uma sem relacao com o catalogo, como a do Airflow -- terminaria a execucao
    # com as tabelas sem certificacao. O sync e idempotente e so aplica o que
    # esta declarado no repositorio: rodar a mais nao custa nada.
    @task(task_id=GOVERNANCA_TASK_ID, trigger_rule="all_done")
    def reaplicar_governanca() -> dict:
        """Governanca, metricas e qualidade declaradas em dbt/minc/governance/.

        Falha se o sync registrar falha, depois de tentar tudo: um ativo
        recusado nao impede os outros 1.200 de serem catalogados.
        """
        import logging
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from airflow.sdk import Variable

        from openmetadata.glossary import load_glossary
        from openmetadata.governanca import (
            catalogo,
            config,
            governanca,
            metricas,
            qualidade,
        )
        from openmetadata.governanca.cliente import ClienteOM

        with TemporaryDirectory(prefix="om_gov_") as tmp:
            cat = catalogo.montar(
                catalogo.carregar_manifest(catalogo.gerar_manifest(Path(tmp)))
            )
        if cat.erros:
            raise RuntimeError(
                "Declaracao de governanca inconsistente:\n" + "\n".join(cat.erros)
            )
        om = ClienteOM(
            Variable.get("OM_HOST"), Variable.get("INGESTION_TOKEN"), confirmar=True
        )
        _, termos = load_glossary(str(config.GLOSSARIO))
        governanca.sincronizar(om, cat, termos)
        metricas.sincronizar(om, cat)
        resumo = qualidade.sincronizar(om, cat)
        logging.info("Governanca aplicada: %s", resumo)
        if om.falhas:
            raise RuntimeError(
                f"{len(om.falhas)} falhas na governanca:\n" + "\n".join(om.falhas[:50])
            )
        return resumo

    @task(task_id=INCIDENTES_TASK_ID, trigger_rule="all_done")
    def classificar_incidentes() -> dict:
        """Severidade do tier da tabela nos incidentes que os testes abriram."""
        from pathlib import Path
        from tempfile import TemporaryDirectory

        from airflow.sdk import Variable

        from openmetadata.governanca import catalogo, config, qualidade
        from openmetadata.governanca.cliente import ClienteOM

        with TemporaryDirectory(prefix="om_gov_") as tmp:
            cat = catalogo.montar(
                catalogo.carregar_manifest(catalogo.gerar_manifest(Path(tmp)))
            )
        om = ClienteOM(
            Variable.get("OM_HOST"), Variable.get("INGESTION_TOKEN"), confirmar=True
        )
        qualidade.classificar_incidentes(om, cat, config.servico(), config.banco())
        return om.resumo()

    recipe_tasks = {
        recipe.task_id: run_openmetadata_recipe.override(task_id=recipe.task_id)(
            recipe_path=recipe.recipe_path,
            command=recipe.command,
            replacements=dict(recipe.replacements),
            variaveis=dict(recipe.variaveis),
            dbt_project_dir=recipe.dbt_project_dir,
        )
        for recipe in ALL_RECIPES
    }

    previous_task = (
        sync_glossary(glossary_definition_path=GLOSSARY_DEFINITION_PATH)
        if INGEST_GLOSSARY
        else None
    )
    previous_task = _encadear(
        previous_task, [recipe_tasks[t] for t in RECIPE_PIPELINE if t in recipe_tasks]
    )

    # Sem governanca nao ha caso de teste nativo para executar nem incidente
    # para classificar: as tres tasks so existem juntas. Instanciar e nao
    # encadear deixaria task orfa, que o Airflow roda sem dependencia nenhuma.
    if SYNC_GOVERNANCA:
        _encadear(
            previous_task,
            [reaplicar_governanca()]
            + [recipe_tasks[t] for t in RECIPES_DEPOIS_DA_GOVERNANCA if t in recipe_tasks]
            + [classificar_incidentes()],
        )


openmetadata_ingestion_dag()
