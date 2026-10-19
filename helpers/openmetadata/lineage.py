"""Linhagem das DAGs para o OpenMetadata.

Como funciona: cada task declara `inlets` e `outlets` com as tabelas que le e
escreve, e uma task no fim da DAG publica isso no OpenMetadata.

Por que operator e nao backend: o `OpenMetadataLineageBackend`, que o provider
documenta como configuracao de `airflow.cfg`, importa `airflow.lineage.backend`
-- modulo removido no Airflow 3. O operator continua funcionando.

A recipe `airflow_metadata` cataloga as DAGs por fora, lendo o banco de
metadados do Airflow. Esta via e complementar: ela publica a ligacao entre
pipeline e tabela, que a recipe sozinha nao enxerga.
"""

from __future__ import annotations

import os

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # noqa: E501
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

# Precisa casar com o serviceName/database das recipes de postgres e dbt, senao
# a linhagem aponta para tabelas que nao existem no catalogo.
OM_SERVICE = os.environ.get("OM_SERVICE", "MinC")
# O database das recipes e o DB_DW_DBNAME, entao ele vence. O default antigo,
# `minc`, era o database parado: a linhagem das DAGs apontava para 647 tabelas
# que ninguem mais atualiza, e o OpenMetadata aceitava sem erro.
OM_DATABASE = (
    os.environ.get("DB_DW_DBNAME") or os.environ.get("OM_DATABASE") or "dbanalytics"
)

# Serviço de pipeline onde as DAGs do MinC aparecem.
OM_PIPELINE_SERVICE = os.environ.get("OM_PIPELINE_SERVICE", "MinC - Airflow")


def tabela(schema: str, nome: str) -> OMEntity:
    """Referencia uma tabela do DW do MinC como no/a de linhagem.

    Use em `inlets` (o que a task le) e `outlets` (o que ela escreve):

        @task(outlets=[tabela("transferegov", "programa_minc")])
    """
    return OMEntity(
        entity=Table,
        fqn=f"{OM_SERVICE}.{OM_DATABASE}.{schema}.{nome}",
        key="default",
    )


def publicar_linhagem(task_id: str = "publicar_linhagem"):
    """Task final que envia os inlets/outlets da DAG ao OpenMetadata.

    O operator do provider monta o cliente a partir do `server_config` recebido
    no __init__, e ele nao declara template_fields. Como o parse da DAG nao tem
    contexto de task para resolver Variable -- e le-las no parse bateria no
    banco de metadados a cada poucos segundos --, a subclasse abaixo adia isso
    para a execucao.
    """
    from airflow_provider_openmetadata.lineage.operator import (
        OpenMetadataLineageOperator,
    )

    class _LinhagemComCredencialEmRuntime(OpenMetadataLineageOperator):
        def execute(self, context):
            from airflow.sdk import Variable

            self.server_config = OpenMetadataConnection(
                hostPort=Variable.get("OM_HOST"),
                authProvider="openmetadata",
                securityConfig=OpenMetadataJWTClientConfig(
                    jwtToken=Variable.get("INGESTION_TOKEN")
                ),
            )
            return super().execute(context)

    return _LinhagemComCredencialEmRuntime(
        task_id=task_id,
        # Substituido em execute(); o __init__ do operator exige um valor.
        server_config=OpenMetadataConnection(hostPort="http://placeholder/api"),
        service_name=OM_PIPELINE_SERVICE,
        # False preserva linhagem publicada por outras vias (dbt, por exemplo)
        # em vez de apagar tudo que nao veio desta DAG.
        only_keep_dag_lineage=False,
        # A DAG nao deve falhar porque o catalogo esta fora do ar: a extracao ja
        # terminou e o dado ja esta no banco quando esta task roda.
        trigger_rule="all_done",
    )
