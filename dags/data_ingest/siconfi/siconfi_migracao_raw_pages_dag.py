"""Conversão única das páginas SICONFI antigas para o datalakehouse.

Disparo manual. Lê ``siconfi_bronze.raw_pages_<endpoint>`` pela conexão
``conn_origem``, só leitura, e grava cada página no raw e no staging pelo mesmo
código da ``siconfi_ingestion_dag`` (ver ``plugins/siconfi_migracao.py``). Não
chama a API e não apaga nada: o ``siconfi_bronze`` continua lá até a bronze nova
ser conferida contra ele.

Cada task copia um endpoint até ``max_run_minutes`` e guarda o progresso em
``siconfi_control.migracao_raw_pages``. Dispare de novo até a task ``conferir``
registrar todas as tabelas como completas. Só rode com a versão nova da
``siconfi_ingestion_dag`` no ar (a que grava no MinIO): enquanto a antiga
gravar no ``siconfi_bronze``, a conversão nunca alcança o fim.

No servidor, ``conn_origem`` é a própria ``postgres_default``. Para testar no
ambiente local com dado real, crie uma conexão só de leitura com o dbanalytics,
passe o id dela em ``conn_origem`` e um ``limite_paginas_por_endpoint`` pequeno:
o progresso fica no Postgres local e os arquivos, no MinIO local.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task

from postgres_helpers import get_postgres_conn
from siconfi_migracao import (
    ENDPOINTS,
    ProgressoPostgres,
    conexao_origem,
    leitor_de_lotes,
    migrar_endpoint,
    tabela_antiga,
    tabela_existe,
    total_na_origem,
)

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "MinC", "retries": 1, "retry_delay": timedelta(minutes=5)}


def _progresso() -> ProgressoPostgres:
    progresso = ProgressoPostgres(get_postgres_conn())
    progresso.garantir_tabela()
    return progresso


@dag(
    dag_id="siconfi_migracao_raw_pages_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    doc_md=__doc__,
    params={
        "conn_origem": Param(
            "postgres_default",
            type="string",
            title="Conexão com o banco que tem o siconfi_bronze",
            description="Só é lida. No servidor, a postgres_default.",
        ),
        "endpoints": Param(
            list(ENDPOINTS),
            type="array",
            items={"type": "string", "enum": list(ENDPOINTS)},
            title="Endpoints a converter",
        ),
        "limite_paginas_por_endpoint": Param(
            0,
            type="integer",
            minimum=0,
            title="Páginas por endpoint neste run (0 = sem limite)",
            description="Para testar no ambiente local com uma amostra.",
        ),
        "lote": Param(200, type="integer", minimum=1, title="Páginas lidas por vez"),
        "paralelo": Param(
            8, type="integer", minimum=1, maximum=32, title="Gravações simultâneas"
        ),
        "max_run_minutes": Param(
            50,
            type="integer",
            minimum=1,
            maximum=110,
            title="Minutos por task antes de parar e guardar o progresso",
        ),
    },
    tags=["minc", "siconfi", "datalakehouse", "migracao"],
)
def siconfi_migracao_raw_pages_dag() -> None:
    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(hours=2))
    def migrar(endpoint: str) -> dict[str, Any]:
        params = get_current_context()["params"]
        if endpoint not in params["endpoints"]:
            return {"tabela": tabela_antiga(endpoint), "pulado": True}
        progresso = _progresso()
        with conexao_origem(get_postgres_conn(params["conn_origem"])) as conn:
            if not tabela_existe(conn, endpoint):
                logger.warning("[siconfi] %s não existe na origem", endpoint)
                return {"tabela": tabela_antiga(endpoint), "ausente": True}
            resumo = migrar_endpoint(
                leitor_de_lotes(conn, endpoint),
                progresso,
                endpoint,
                lote=int(params["lote"]),
                limite=int(params["limite_paginas_por_endpoint"]) or None,
                prazo_s=60 * float(params["max_run_minutes"]),
                paralelo=int(params["paralelo"]),
            )
        logger.info("[siconfi] conversão: %s", resumo)
        return resumo

    @task(trigger_rule="all_done")
    def conferir() -> dict[str, Any]:
        """Páginas na origem contra páginas copiadas, por tabela."""
        params = get_current_context()["params"]
        progresso = _progresso()
        relatorio: dict[str, Any] = {}
        with conexao_origem(get_postgres_conn(params["conn_origem"])) as conn:
            for endpoint in ENDPOINTS:
                tabela = tabela_antiga(endpoint)
                if not tabela_existe(conn, endpoint):
                    relatorio[tabela] = {"ausente": True}
                    continue
                total, maior_id = total_na_origem(conn, endpoint)
                ultimo, copiadas = progresso.ler(tabela)
                relatorio[tabela] = {
                    "paginas_na_origem": total,
                    "paginas_copiadas": copiadas,
                    "faltam": total - copiadas,
                    "completo": copiadas == total and ultimo >= maior_id,
                }
                logger.info("[siconfi] %s: %s", tabela, relatorio[tabela])
        pendentes = [t for t, r in relatorio.items() if r.get("completo") is False]
        logger.info(
            "[siconfi] %s",
            f"faltam {pendentes}" if pendentes else "todas as tabelas completas",
        )
        return relatorio

    copias = [
        migrar.override(task_id=f"migrar_{tabela_antiga(endpoint)}")(endpoint)
        for endpoint in ENDPOINTS
    ]
    copias >> conferir()


siconfi_migracao_raw_pages_dag()
