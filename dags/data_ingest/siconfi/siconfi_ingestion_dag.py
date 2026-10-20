"""Ingestão retomável dos nove endpoints da API pública SICONFI.

O escopo é deliberadamente dirigido pela Variable ``siconfi_config``. O
padrão cobre só o exercício corrente e todos os demonstrativos. Para um
backfill, estreite o recorte antes de ampliar os anos: a API aceita 1 req/s e
cada combinação de parâmetros é uma requisição — a MSC completa de um ente
num ano são 312. Chaves de recorte (ausente ou ``null`` = sem filtro):

- ``fact_endpoints``: quais de ``rreo``, ``rgf``, ``dca`` e ``msc_*`` buscar;
- ``rgf_poderes``: E, L, J, M, D;
- ``msc_months``, ``msc_classes``, ``msc_value_types``, ``msc_matrix_types``;
- ``rreo_periods`` (bimestres 1 a 6) e ``rreo_anexos`` (por exemplo
  ``"RREO-Anexo 03"``). Com anexos, cada unidade pede só aqueles anexos em vez do
  RREO inteiro.

A DCA tem intervalo de anos próprio, ``dca_start_year`` e ``dca_end_year``, e sem
ele fica desligada. Ela não vem do extrato: é planejada direto, uma requisição por
ente e exercício (a API só tem DCA a partir de 2013, e o balanço de um exercício
só existe no ano seguinte). Isso evita buscar o extrato de cada ano antigo.

Exemplo — o saldo final de dezembro da execução orçamentária, mais o DCA::

    {"start_year": 2019, "end_year": 2025,
     "fact_endpoints": ["msc_orcamentaria", "dca"],
     "dca_start_year": 2014, "dca_end_year": 2025,
     "msc_months": [12], "msc_classes": [6],
     "msc_value_types": ["ending_balance"], "msc_matrix_types": ["MSCC"]}

Exemplo — Eixo 1 (gasto empenhado da MSC orçamentária e a RCL, que é o Anexo 03
do RREO no bimestre 6)::

    {"start_year": 2019, "end_year": 2025,
     "fact_endpoints": ["msc_orcamentaria", "rreo"],
     "msc_months": [12], "msc_classes": [6],
     "msc_value_types": ["ending_balance"], "msc_matrix_types": ["MSCC", "MSCE"],
     "rreo_periods": [6], "rreo_anexos": ["RREO-Anexo 03"]}

O recorte vale também para o que já está na fila: estreitá-lo para de buscar o
que ficou de fora, sem apagar nada. Cada task de ingestão trabalha até
``max_run_minutes`` e devolve à fila o que não processou.

Destino (ADR 0009 e 0011): cada página vai para o datalakehouse, em
``raw/siconfi/<entidade>/`` como chegou e em ``staging/siconfi/<entidade>/``
como Parquet de texto, uma pasta por run (ver ``siconfi_storage``). Nenhuma
camada é gravada no Postgres; lá fica só o controle, em ``siconfi_control``.
Cada task emite o Asset do staging da sua entidade com a pasta do run no
``extra``, e é ele que dispara a ponte para ``siconfi.bronze_<entidade>``. Precisa
da conexão ``minio_datalakehouse`` no Airflow.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import Variable, dag, task

import datalakehouse
from cliente_siconfi import (
    FACT_ENDPOINTS,
    PlanScope,
    SiconfiClient,
    SiconfiPermanentError,
    SiconfiRetryableError,
)
from postgres_helpers import get_postgres_conn
from schedule_loader import get_dynamic_schedule
from siconfi_storage import (
    CLAIM_LEASE_MINUTES,
    FONTE,
    SiconfiStorage,
    entidade,
    entity_ids_do_raw,
    persist_page,
    plan_from_extrato,
    prefixo_raw_do_run,
    prefixo_staging_do_run,
)

logger = logging.getLogger(__name__)

# Não há pool do Airflow aqui de propósito: o contrato de 1 req/s é garantido
# pelo SiconfiRateLimiter, que coordena via PostgreSQL e por isso vale entre
# tasks, workers e DAGs. Um pool seria redundante — e, se não existisse na
# instância, o scheduler deixaria as tasks em `scheduled` para sempre, sem log.
_DEFAULT_CONFIG: dict[str, Any] = {
    "start_year": datetime.now().year,
    "end_year": datetime.now().year,
    "entity_ids": [],
    "page_limit": 5000,
    # O limite real de cada task é o tempo; este teto só evita reservar demais.
    "max_work_units_per_run": 2000,
    "max_run_minutes": 45,
    "reference_refresh_hours": 168,
    # Itens do extrato lidos por execução do planejamento, sempre em arquivos
    # inteiros do raw que ainda não foram planejados com o recorte atual.
    "max_manifest_rows_for_planning": 100000,
    "fact_endpoints": None,
    "rreo_periods": None,
    "rreo_anexos": None,
    "dca_start_year": None,
    "dca_end_year": None,
    "rgf_poderes": None,
    "msc_months": None,
    "msc_classes": None,
    "msc_value_types": None,
    "msc_matrix_types": None,
}
_DEFAULT_ARGS = {"owner": "MinC", "retries": 2, "retry_delay": timedelta(minutes=10)}
# Unidades de trabalho acumuladas antes de cada gravação em lote no planejamento.
_PLAN_FLUSH_SIZE = 5000
# Tabelas de referência, atualizadas a cada ``reference_refresh_hours``.
_REFERENCE_ENDPOINTS = ("anexos-relatorios", "entes")


def _config() -> dict[str, Any]:
    configured = Variable.get("siconfi_config", default={}, deserialize_json=True)
    config = {**_DEFAULT_CONFIG, **configured}
    if int(config["start_year"]) > int(config["end_year"]):
        raise ValueError("siconfi_config: start_year não pode ser maior que end_year")
    if int(config["page_limit"]) < 1 or int(config["max_work_units_per_run"]) < 1:
        raise ValueError(
            "siconfi_config: page_limit e max_work_units_per_run devem ser positivos"
        )
    if not 0 < float(config["max_run_minutes"]) < CLAIM_LEASE_MINUTES:
        raise ValueError(
            f"siconfi_config: max_run_minutes deve ficar entre 0 e {CLAIM_LEASE_MINUTES}"
        )
    PlanScope.from_config(config)  # valida as chaves de recorte
    return config


def _run() -> tuple[str, datetime]:
    """``run_id`` e a data que dá nome à pasta do run no datalakehouse."""
    return datalakehouse.run_atual()


def _asset(endpoint: str) -> Any:
    return datalakehouse.asset_staging(FONTE, entidade(endpoint))


def _publicar_run(endpoint: str, run_id: str, data: datetime) -> None:
    """Avisa a ponte de que a pasta deste run, no staging, está completa."""
    datalakehouse.publicar_cadeia(
        _asset(endpoint),
        {entidade(endpoint): prefixo_staging_do_run(endpoint, run_id, data)},
    )


def _storage(conn_str: str) -> SiconfiStorage:
    storage = SiconfiStorage(conn_str)
    storage.ensure_tables()
    return storage


def _refresh_reference(
    conn_str: str, config: dict[str, Any], run_id: str, data: datetime
) -> dict[str, int | None]:
    """Itens gravados por tabela de referência; ``None`` quando não venceu."""
    storage = _storage(conn_str)
    client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
    result: dict[str, int | None] = {}
    try:
        for endpoint in _REFERENCE_ENDPOINTS:
            if not storage.reference_due(
                endpoint, int(config["reference_refresh_hours"])
            ):
                result[endpoint] = None
                continue
            result[endpoint] = sum(
                persist_page(page, run_id, data)
                for page in client.iter_pages(endpoint, {})
            )
            # Só depois da última página: o planejamento lê os entes desta
            # pasta, e uma pasta pela metade deixaria entes de fora.
            storage.mark_reference_refreshed(
                endpoint, prefixo_raw_do_run(endpoint, run_id, data)
            )
    finally:
        client.close()
    return result


def _entity_ids(storage: SiconfiStorage, config: dict[str, Any]) -> list[int]:
    return [int(value) for value in config["entity_ids"]] or entity_ids_do_raw(
        storage.reference_prefix("entes")
    )


def _plan_extrato(conn_str: str, config: dict[str, Any]) -> int:
    storage = _storage(conn_str)
    entity_ids = _entity_ids(storage, config)
    if not entity_ids:
        raise ValueError(
            "Nenhum ente disponível; aguarde a carga de /entes " "ou configure entity_ids"
        )
    years = range(int(config["start_year"]), int(config["end_year"]) + 1)
    created = storage.enqueue_many(
        "extrato_entregas",
        (
            ({"id_ente": entity_id, "an_referencia": year}, None)
            for entity_id in entity_ids
            for year in years
        ),
    )
    logger.info(
        "[siconfi] %s unidades de extrato novas, %s entes", created, len(entity_ids)
    )
    return created


def _plan_dca(storage: SiconfiStorage, scope: PlanScope, config: dict[str, Any]) -> int:
    """Enfileira a DCA por ente e exercício, sem consultar o extrato."""
    if not scope.enabled("dca"):
        return 0
    entity_ids = _entity_ids(storage, config)
    if not entity_ids:
        raise ValueError(
            "Nenhum ente disponível; aguarde a carga de /entes ou configure entity_ids"
        )
    created = storage.enqueue_many("dca", scope.dca_units(entity_ids))
    logger.info(
        "[siconfi] %s unidades de dca novas, %s entes, exercícios %s a %s",
        created,
        len(entity_ids),
        scope.dca_start_year,
        scope.dca_end_year,
    )
    return created


def _plan_facts(conn_str: str, config: dict[str, Any]) -> dict[str, int]:
    scope = PlanScope.from_config(config)
    storage = _storage(conn_str)
    created = plan_from_extrato(
        storage,
        scope,
        budget=int(config["max_manifest_rows_for_planning"]),
        flush_size=_PLAN_FLUSH_SIZE,
    )
    created["dca"] = _plan_dca(storage, scope, config)
    logger.info("[siconfi] unidades novas: %s", created)
    return created


def _ingest_work(
    endpoint: str, conn_str: str, config: dict[str, Any], run_id: str, data: datetime
) -> dict[str, int]:
    scope = PlanScope.from_config(config)
    summary = dict.fromkeys(
        ("claimed", "success", "no_data", "retry", "permanent_error", "released", "rows"),
        0,
    )
    if not scope.enabled(endpoint):
        logger.info("[siconfi] %s fora do recorte; nada a fazer", endpoint)
        return summary
    storage = _storage(conn_str)
    claimed = storage.claim(
        endpoint,
        int(config["max_work_units_per_run"]),
        run_id,
        param_filter=scope.constraints(endpoint),
    )
    summary["claimed"] = len(claimed)
    deadline = time.monotonic() + 60 * float(config["max_run_minutes"])
    done = 0
    client = SiconfiClient(conn_str, page_limit=int(config["page_limit"]))
    try:
        for unit in claimed:
            if time.monotonic() >= deadline:
                break
            try:
                rows = sum(
                    persist_page(page, run_id, data)
                    for page in client.iter_pages(endpoint, unit["params"])
                )
                status, error = ("success" if rows else "no_data"), None
                summary["rows"] += rows
            except SiconfiRetryableError as exc:
                status, error = "retry", str(exc)
                logger.warning("[siconfi] %s será repetido: %s", endpoint, exc)
            except SiconfiPermanentError as exc:
                status = "no_data" if exc.status_code == 404 else "permanent_error"
                error = str(exc)
                logger.warning("[siconfi] %s não recuperável: %s", endpoint, exc)
            storage.complete(endpoint, unit["work_key"], status, error)
            summary[status] += 1
            done += 1
    finally:
        client.close()
        # Por tempo esgotado ou por erro inesperado: o que não foi processado
        # volta para a fila agora, não quando o lease vencer.
        remaining = [unit["work_key"] for unit in claimed[done:]]
        storage.release(endpoint, remaining, run_id)
        summary["released"] = len(remaining)
    logger.info("[siconfi] %s: %s", endpoint, summary)
    return summary


@dag(
    dag_id="siconfi_ingestion_dag",
    schedule=get_dynamic_schedule("siconfi_ingestion_dag", default="@hourly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["minc", "siconfi", "tesouro", "datalakehouse", "raw"],
)
def siconfi_ingestion_dag() -> None:
    @task(outlets=[_asset(endpoint) for endpoint in _REFERENCE_ENDPOINTS])
    def refresh_reference_data() -> dict[str, int | None]:
        run_id, data = _run()
        result = _refresh_reference(get_postgres_conn(), _config(), run_id, data)
        for endpoint, rows in result.items():
            if rows is not None:
                _publicar_run(endpoint, run_id, data)
        return result

    @task
    def plan_extrato() -> int:
        return _plan_extrato(get_postgres_conn(), _config())

    @task
    def plan_facts() -> dict[str, int]:
        return _plan_facts(get_postgres_conn(), _config())

    @task
    def ingest(endpoint: str) -> dict[str, int]:
        run_id, data = _run()
        summary = _ingest_work(endpoint, get_postgres_conn(), _config(), run_id, data)
        _publicar_run(endpoint, run_id, data)
        return summary

    planned_facts = plan_facts()
    (
        refresh_reference_data()
        >> plan_extrato()
        >> ingest.override(
            task_id="ingest_extrato", outlets=[_asset("extrato_entregas")]
        )("extrato_entregas")
        >> planned_facts
    )
    for endpoint in FACT_ENDPOINTS:
        planned_facts >> ingest.override(
            task_id=f"ingest_{endpoint}", outlets=[_asset(endpoint)]
        )(endpoint)


siconfi_ingestion_dag()
