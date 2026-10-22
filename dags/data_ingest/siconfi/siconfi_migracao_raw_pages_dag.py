"""Conversão única do SICONFI antigo para o raw do datalakehouse e o controle novo.

Disparo manual. Lê ``siconfi_bronze.raw_pages_<endpoint>`` e
``siconfi_control.work_queue`` pela conexão ``conn_origem``, só leitura, e:

- grava cada página no raw pelo mesmo código da ``siconfi_ingestion_dag`` (ver
  ``plugins/siconfi_migracao.py``);
- copia a fila para o controle novo (``<catalogo>.control.siconfi_*``, pelo
  Trino), para a DAG nova não baixar de novo o que já foi baixado.

Não chama a API e não apaga nada: o ``siconfi_bronze`` continua lá até a bronze
nova ser conferida contra ele.

**Exceção ao ADR 0009.** A leitura do Postgres antigo usa ``psycopg2``: roda uma
vez, no ambiente atual, antes da mudança do Airflow para o Serpro.

**Ordem.** Rode antes de despausar a ``siconfi_ingestion_dag`` nova, ou pelo
menos antes que ela planeje: sem a fila convertida, ela não sabe o que já foi
baixado. E só com a versão nova da ingestão no ar: enquanto a antiga gravar no
``siconfi_bronze``, a conversão nunca alcança o fim.

Cada task para em ``max_run_minutes`` e guarda o progresso. Dispare de novo até a
task ``conferir`` registrar tudo como ``completo``. Ela compara a origem com os
arquivos de verdade no raw, por pasta de run, e falha se uma tabela dada como
copiada não tiver os arquivos esperados. A task ``falhar_se_algo_falhou`` deixa o
run vermelho quando uma cópia falha.

No servidor, ``conn_origem`` é a própria ``postgres_default``. Para testar no
ambiente local com dado real, use uma amostra num Postgres local e um
``limite_paginas_por_endpoint`` pequeno.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk import Param, TriggerRule, dag, get_current_context, task

from postgres_helpers import get_postgres_conn
from siconfi_controle import controle_do_airflow
from siconfi_migracao import (
    ENDPOINTS,
    ProgressoPostgres,
    avaliar_tabela,
    conexao_origem,
    contagem_da_fila_antiga,
    converter_fila,
    exigir_sem_duplicatas,
    fila_existe,
    leitor_da_fila,
    leitor_de_lotes,
    migrar_endpoint,
    objetos_dos_runs,
    runs_na_origem,
    status_da_fila_antiga,
    tabela_antiga,
    tabela_existe,
    total_na_origem,
)

logger = logging.getLogger(__name__)

_DEFAULT_ARGS = {"owner": "MinC", "retries": 1, "retry_delay": timedelta(minutes=5)}


def _params() -> dict[str, Any]:
    return dict(get_current_context()["params"])


def _progresso() -> ProgressoPostgres:
    return ProgressoPostgres(get_postgres_conn())


def _origem() -> Any:
    return conexao_origem(get_postgres_conn(_params()["conn_origem"]))


def _migrar(endpoint: str) -> dict[str, Any]:
    params = _params()
    if endpoint not in params["endpoints"]:
        return {"tabela": tabela_antiga(endpoint), "pulado": True}
    progresso = _progresso()
    with _origem() as conn:
        if not tabela_existe(conn, endpoint):
            logger.warning("[siconfi] %s não existe na origem", endpoint)
            return {"tabela": tabela_antiga(endpoint), "ausente": True}
        if progresso.ler(tabela_antiga(endpoint))[0] == 0:
            exigir_sem_duplicatas(conn, endpoint)
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


def _migrar_fila() -> dict[str, Any]:
    params = _params()
    if not params["converter_fila"]:
        return {"pulado": True}
    with _origem() as conn:
        if not fila_existe(conn):
            logger.warning("[siconfi] siconfi_control.work_queue não existe na origem")
            return {"ausente": True}
        resumo = converter_fila(
            leitor_da_fila(conn),
            controle_do_airflow(),
            _progresso(),
            prazo_s=60 * float(params["max_run_minutes"]),
        )
    logger.info("[siconfi] fila: %s", resumo)
    return resumo


def _conferir() -> dict[str, Any]:
    """Origem contra arquivos no raw, por tabela, e fila antiga contra a nova."""
    progresso = _progresso()
    relatorio: dict[str, Any] = {}
    with _origem() as conn:
        for endpoint in ENDPOINTS:
            tabela = tabela_antiga(endpoint)
            if not tabela_existe(conn, endpoint):
                relatorio[tabela] = {"situacao": "ausente"}
                continue
            total, _ = total_na_origem(conn, endpoint)
            runs = runs_na_origem(conn, endpoint)
            relatorio[tabela] = avaliar_tabela(
                paginas_na_origem=total,
                distintas_na_origem=sum(n for _, _, n in runs),
                copiadas=progresso.ler(tabela)[1],
                objetos_no_raw=objetos_dos_runs(endpoint, runs),
            )
        if fila_existe(conn):
            relatorio["fila"] = {
                "antiga": status_da_fila_antiga(contagem_da_fila_antiga(conn)),
                "nova": controle_do_airflow().latest_status_counts(),
                "convertidas": progresso.ler_fila()[2],
            }
    for nome, linha in relatorio.items():
        logger.info("[siconfi] %s: %s", nome, linha)
    inconsistentes = [
        t for t, r in relatorio.items() if r.get("situacao") == "inconsistente"
    ]
    if inconsistentes:
        raise AirflowFailException(
            f"cópia sem os arquivos esperados no raw: {inconsistentes}"
        )
    return relatorio


@dag(
    dag_id="siconfi_migracao_raw_pages_dag",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # No máximo 3 tasks ao mesmo tempo no run: cada cópia abre uma conexão longa
    # na origem e grava com ``paralelo`` threads no MinIO.
    max_active_tasks=3,
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
        "converter_fila": Param(
            True, type="boolean", title="Converter a fila antiga para o controle novo"
        ),
        "limite_paginas_por_endpoint": Param(
            0,
            type="integer",
            minimum=0,
            title="Páginas por endpoint neste run (0 = sem limite)",
            description="Para testar no ambiente local com uma amostra.",
        ),
        # Uma página MSC chega a 2,5 MB em texto (medido em 21/10/2026) e ocupa
        # várias vezes isso como objeto Python: o lote fica pequeno.
        "lote": Param(20, type="integer", minimum=1, title="Páginas lidas por vez"),
        "paralelo": Param(
            4, type="integer", minimum=1, maximum=16, title="Gravações simultâneas"
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
    @task
    def preparar() -> None:
        """Cria as tabelas uma vez, antes das cópias, e não em cada uma delas."""
        _progresso().garantir_tabelas()
        controle_do_airflow()

    @task(execution_timeout=timedelta(hours=2))
    def migrar(endpoint: str) -> dict[str, Any]:
        return _migrar(endpoint)

    @task(execution_timeout=timedelta(hours=2))
    def migrar_fila() -> dict[str, Any]:
        return _migrar_fila()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def conferir() -> dict[str, Any]:
        return _conferir()

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def falhar_se_algo_falhou() -> None:
        """Deixa o run vermelho quando uma cópia falha: ``conferir`` roda
        sempre e, sozinha, esconderia a falha."""
        raise AirflowFailException("uma ou mais tasks da conversão falharam")

    copias = [
        migrar.override(task_id=f"migrar_{tabela_antiga(endpoint)}")(endpoint)
        for endpoint in ENDPOINTS
    ]
    copias.append(migrar_fila())
    conferencia = conferir()
    preparar() >> copias
    copias >> conferencia
    [*copias, conferencia] >> falhar_se_algo_falhou()


siconfi_migracao_raw_pages_dag()
