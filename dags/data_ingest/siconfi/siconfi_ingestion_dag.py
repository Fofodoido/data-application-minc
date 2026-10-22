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
``max_run_minutes``; o que não processou continua pendente para o próximo run.

Destino (ADR 0009): cada página vai para o raw do datalakehouse, em
``raw/siconfi/<entidade>/``, como chegou, uma pasta por run (ver
``siconfi_storage``). Não há staging: a carga da bronze
(``siconfi.bronze_<entidade>``) lê o raw.

Controle (ADR 0005 e 0009): o Airflow não fala com banco, só com o Trino. A fila,
o refresh das tabelas de referência e o que o planejamento já leu ficam em
``<catalogo>.control.siconfi_*``, escritos só por ``INSERT`` pelo Trino, como os
logs do SALIC v2 e do Mapas (ver ``siconfi_controle``). Sem trava de linha, uma
unidade nunca é processada por duas tasks porque a DAG roda um run por vez e uma
task por vez (``max_active_tasks=1``), o que também garante o limite de 1 req/s
da API com um limitador em memória. Catálogo e schema vêm das Variables
``siconfi_trino_target_catalog`` (padrão ``dw``) e ``siconfi_trino_control_schema``
(padrão ``control``).

Cada entidade tem uma task ``anunciar_<entidade>`` que roda mesmo se a ingestão
falhou e emite o Asset ``raw/siconfi/<entidade>`` com a pasta do run no
``extra``, mas só quando a pasta tem arquivo. É o que vai disparar a carga da
bronze. A pasta de um run tem só o que aquele run processou: quem consome precisa
ler todos os eventos recebidos, não só o mais novo. A task
``falhar_se_algo_falhou`` deixa o run vermelho quando qualquer task falha.

Agendamento: o ``@hourly`` não é uma coleta por hora. Com um run por vez e uma
task por vez, as 7 tasks de ingestão (extrato e 6 fatos) rodam em série, até
``max_run_minutes`` cada: com fila cheia, um run leva de 5 a 6 horas. Com
``max_active_runs=1`` e ``catchup=False``, o scheduler não acumula runs: quando um
termina, ele cria logo o do intervalo mais recente. Na prática a DAG roda sem
parar enquanto há fila, e de hora em hora quando a fila está vazia (aí o run
termina em minutos). A vazão é a mesma de rodar em paralelo, porque a API limita
tudo a 1 req/s.

Precisa das conexões ``minio_datalakehouse`` e ``trino_default`` no Airflow.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import TriggerRule, Variable, dag, task

import datalakehouse
from cliente_siconfi import FACT_ENDPOINTS, PlanScope, SiconfiClient
from schedule_loader import get_dynamic_schedule
from siconfi_controle import SiconfiControle, controle_do_airflow
from siconfi_storage import (
    FONTE,
    entidade,
    entity_ids_do_raw,
    ingerir_endpoint,
    persist_page,
    plan_from_extrato,
    prefixo_raw_do_run,
    raw_keys,
)

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG: dict[str, Any] = {
    "start_year": datetime.now().year,
    "end_year": datetime.now().year,
    "entity_ids": [],
    "page_limit": 5000,
    # O limite real de cada task é o tempo; este teto só evita ler demais da fila.
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
# Teto do ``max_run_minutes``: abaixo do ``execution_timeout`` das tasks.
_MAX_RUN_MINUTES = 110


def _config() -> dict[str, Any]:
    configured = Variable.get("siconfi_config", default={}, deserialize_json=True)
    config = {**_DEFAULT_CONFIG, **configured}
    if int(config["start_year"]) > int(config["end_year"]):
        raise ValueError("siconfi_config: start_year não pode ser maior que end_year")
    if int(config["page_limit"]) < 1 or int(config["max_work_units_per_run"]) < 1:
        raise ValueError(
            "siconfi_config: page_limit e max_work_units_per_run devem ser positivos"
        )
    if not 0 < float(config["max_run_minutes"]) <= _MAX_RUN_MINUTES:
        raise ValueError(
            f"siconfi_config: max_run_minutes deve ficar entre 0 e {_MAX_RUN_MINUTES}"
        )
    PlanScope.from_config(config)  # valida as chaves de recorte
    return config


def _controle() -> SiconfiControle:
    return controle_do_airflow()


def _client(config: dict[str, Any]) -> SiconfiClient:
    return SiconfiClient(page_limit=int(config["page_limit"]))


def _run() -> tuple[str, datetime]:
    """``run_id`` e a data que dá nome à pasta do run no datalakehouse."""
    return datalakehouse.run_atual()


def _asset(endpoint: str) -> Any:
    return datalakehouse.asset(
        datalakehouse.prefixo(datalakehouse.CAMADA_RAW, FONTE, entidade(endpoint))
    )


def _refresh_reference(
    controle: SiconfiControle, config: dict[str, Any], run_id: str, data: datetime
) -> dict[str, int | None]:
    """Itens gravados por tabela de referência; ``None`` quando não venceu."""
    client = _client(config)
    result: dict[str, int | None] = {}
    try:
        for endpoint in _REFERENCE_ENDPOINTS:
            if not controle.reference_due(
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
            controle.mark_reference_refreshed(
                endpoint, prefixo_raw_do_run(endpoint, run_id, data), run_id
            )
    finally:
        client.close()
    return result


def _entity_ids(controle: SiconfiControle, config: dict[str, Any]) -> list[int]:
    return [int(value) for value in config["entity_ids"]] or entity_ids_do_raw(
        controle.reference_prefix("entes")
    )


def _plan_extrato(controle: SiconfiControle, config: dict[str, Any]) -> int:
    entity_ids = _entity_ids(controle, config)
    if not entity_ids:
        raise ValueError(
            "Nenhum ente disponível; aguarde a carga de /entes ou configure entity_ids"
        )
    years = range(int(config["start_year"]), int(config["end_year"]) + 1)
    created = controle.enqueue_many(
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


def _plan_dca(controle: SiconfiControle, scope: PlanScope, config: dict[str, Any]) -> int:
    """Enfileira a DCA por ente e exercício, sem consultar o extrato."""
    if not scope.enabled("dca"):
        return 0
    entity_ids = _entity_ids(controle, config)
    if not entity_ids:
        raise ValueError(
            "Nenhum ente disponível; aguarde a carga de /entes ou configure entity_ids"
        )
    created = controle.enqueue_many("dca", scope.dca_units(entity_ids))
    logger.info(
        "[siconfi] %s unidades de dca novas, %s entes, exercícios %s a %s",
        created,
        len(entity_ids),
        scope.dca_start_year,
        scope.dca_end_year,
    )
    return created


def _plan_facts(controle: SiconfiControle, config: dict[str, Any]) -> dict[str, int]:
    scope = PlanScope.from_config(config)
    created = plan_from_extrato(
        controle,
        scope,
        budget=int(config["max_manifest_rows_for_planning"]),
        flush_size=_PLAN_FLUSH_SIZE,
    )
    created["dca"] = _plan_dca(controle, scope, config)
    logger.info("[siconfi] unidades novas: %s", created)
    return created


def _ingerir(endpoint: str) -> dict[str, int]:
    run_id, data = _run()
    config = _config()
    client = _client(config)
    try:
        summary = ingerir_endpoint(
            endpoint,
            _controle(),
            client,
            PlanScope.from_config(config),
            run_id,
            data,
            limite=int(config["max_work_units_per_run"]),
            prazo_s=60 * float(config["max_run_minutes"]),
        )
    finally:
        client.close()
    logger.info("[siconfi] %s: %s", endpoint, summary)
    return summary


def _anunciar(endpoint: str) -> str:
    """Emite o Asset da entidade se a pasta deste run tem arquivo.

    Roda mesmo se a ingestão falhou no meio: o que foi gravado antes da falha
    também precisa chegar à bronze. Sem arquivo, a task é pulada e nenhum evento
    sai.
    """
    run_id, data = _run()
    prefixo = prefixo_raw_do_run(endpoint, run_id, data)
    if not raw_keys(endpoint, prefixo):
        raise AirflowSkipException(f"nada gravado em {prefixo}")
    datalakehouse.publicar_cadeia(_asset(endpoint), {entidade(endpoint): prefixo})
    return prefixo


@dag(
    dag_id="siconfi_ingestion_dag",
    # Não é uma coleta por hora: com fila cheia cada run leva horas, e o próximo
    # sai assim que ele termina. Ver "Agendamento" no topo.
    schedule=get_dynamic_schedule("siconfi_ingestion_dag", default="@hourly"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Uma task por vez: sem trava de linha no controle, é o que impede duas tasks
    # na mesma fila, e o que faz o limitador em memória valer para a API toda.
    max_active_tasks=1,
    default_args=_DEFAULT_ARGS,
    tags=["minc", "siconfi", "tesouro", "datalakehouse", "raw"],
)
def siconfi_ingestion_dag() -> None:
    @task
    def refresh_reference_data() -> dict[str, int | None]:
        run_id, data = _run()
        return _refresh_reference(_controle(), _config(), run_id, data)

    @task
    def plan_extrato() -> int:
        return _plan_extrato(_controle(), _config())

    @task
    def plan_facts() -> dict[str, int]:
        return _plan_facts(_controle(), _config())

    @task(execution_timeout=timedelta(hours=2))
    def ingest(endpoint: str) -> dict[str, int]:
        return _ingerir(endpoint)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def anunciar(endpoint: str) -> str:
        return _anunciar(endpoint)

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def falhar_se_algo_falhou() -> None:
        """Deixa o run vermelho: as tasks ``anunciar_*`` terminam bem mesmo
        quando a ingestão de antes falhou, e esconderiam a falha."""
        raise AirflowFailException("uma ou mais tasks da ingestão SICONFI falharam")

    def anuncio(endpoint: str) -> Any:
        return anunciar.override(
            task_id=f"anunciar_{entidade(endpoint)}", outlets=[_asset(endpoint)]
        )(endpoint)

    refresh = refresh_reference_data()
    extrato = plan_extrato()
    ingest_extrato = ingest.override(task_id="ingest_extrato")("extrato_entregas")
    planned_facts = plan_facts()
    tarefas: list[Any] = [refresh, extrato, ingest_extrato, planned_facts]
    refresh >> extrato >> ingest_extrato >> planned_facts
    for endpoint in _REFERENCE_ENDPOINTS:
        tarefas.append(refresh >> anuncio(endpoint))
    tarefas.append(ingest_extrato >> anuncio("extrato_entregas"))
    for endpoint in FACT_ENDPOINTS:
        ingest_fato = ingest.override(task_id=f"ingest_{endpoint}")(endpoint)
        tarefas.extend([planned_facts >> ingest_fato, ingest_fato >> anuncio(endpoint)])
    tarefas >> falhar_se_algo_falhou()


siconfi_ingestion_dag()
