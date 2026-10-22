"""Porta única de escrita e leitura no datalakehouse do MinC (MinIO).

Dado de API não vai mais direto para o Postgres: cada extração grava o que
recebeu em ``raw/`` e uma cópia tabular em Parquet em ``staging/``, no bucket
``minc-datalakehouse``::

    raw/<fonte>/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.json
    staging/<fonte>/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>.parquet

As duas camadas têm o mesmo caminho, só muda o prefixo e a extensão -- dado
um arquivo de staging, o raw de onde ele saiu é sempre recuperável.

As funções de caminho e de conversão são puras (não tocam Airflow nem rede)
para serem testáveis sem subir o ambiente; o ``S3Hook`` é importado só dentro
das funções que falam com o MinIO.

O encadeamento entre DAGs é por Asset, um por entidade: quem grava o staging
publica um evento com a key exata do arquivo no ``extra``, e quem consome lê
essa key do evento que o disparou. O ``extra`` carrega a *cadeia* inteira
(``entidade -> key``) desde o programa, para que um consumidor que precise de
várias entidades -- anexos, relatórios e planos -- leia todas do mesmo run.

Fonte que grava muitos arquivos por run (SICONFI: um por página da API) usa
``prefixo_do_run``/``caminho_no_run`` e publica no ``extra`` a **pasta** do run,
não a key de um arquivo: quem consome lista a pasta. E cada pasta só tem o que
aquele run processou, então o consumidor precisa ler todos os eventos recebidos
(``eventos_em_ordem``), não só o mais novo.
"""

from __future__ import annotations

import functools
import io
import json
import logging
import math
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping

import pandas as pd

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.sdk import Asset

CONN_ID = "minio_datalakehouse"
BUCKET = "minc-datalakehouse"

CAMADA_RAW = "raw"
CAMADA_STAGING = "staging"
_EXTENSAO = {CAMADA_RAW: "json", CAMADA_STAGING: "parquet"}

# O run_id do Airflow carrega ':' e '+' (manual__2026-10-01T12:00:00+00:00),
# que viram escape em URL de S3 e atrapalham quem lista o bucket pelo console.
_CARACTERE_INSEGURO = re.compile(r"[^A-Za-z0-9._=-]")


# ── caminhos ─────────────────────────────────────────────────────────────


def sanitizar_run_id(run_id: str) -> str:
    return _CARACTERE_INSEGURO.sub("_", run_id)


def prefixo(camada: str, fonte: str, entidade: str) -> str:
    """Prefixo de uma entidade numa camada, terminado em barra."""
    if camada not in _EXTENSAO:
        raise ValueError(f"camada inválida: {camada}")
    return f"{camada}/{fonte}/{entidade}/"


def caminho(camada: str, fonte: str, entidade: str, run_id: str, data: datetime) -> str:
    """Key completa de um arquivo de ``camada`` para o run ``run_id``."""
    return (
        f"{prefixo(camada, fonte, entidade)}"
        f"ano={data:%Y}/mes={data:%m}/dia={data:%d}/"
        f"{sanitizar_run_id(run_id)}.{_EXTENSAO[camada]}"
    )


def caminho_particionado(
    camada: str,
    fonte: str,
    entidade: str,
    particao: Mapping[str, Any],
    run_id: str,
    extensao: str,
) -> str:
    """Key de um arquivo particionado pelo que identifica o dado, não pela data do run.

    Para fonte estática (um arquivo por ano de referência, como os censos do
    INEP) a partição útil é o ano do dado: recarregar um ano é achar o prefixo
    dele. Cada run grava a própria key, então o raw continua imutável.
    """
    particoes = "".join(f"{nome}={valor}/" for nome, valor in particao.items())
    return (
        f"{prefixo(camada, fonte, entidade)}{particoes}"
        f"{sanitizar_run_id(run_id)}.{extensao}"
    )


def prefixo_do_run(
    camada: str, fonte: str, entidade: str, run_id: str, data: datetime
) -> str:
    """Pasta de um run que grava muitos arquivos da mesma entidade.

    O ``caminho`` dá um arquivo por run; uma fonte paginada (SICONFI) grava
    milhares de páginas por run, e cada uma vira um arquivo dentro desta pasta.
    Quem consome o run lê a pasta inteira -- nunca "o arquivo mais recente".
    """
    return (
        f"{prefixo(camada, fonte, entidade)}"
        f"ano={data:%Y}/mes={data:%m}/dia={data:%d}/{sanitizar_run_id(run_id)}/"
    )


def caminho_no_run(
    camada: str, fonte: str, entidade: str, run_id: str, data: datetime, nome: str
) -> str:
    """Key de um arquivo ``nome`` dentro da pasta do run (ver ``prefixo_do_run``)."""
    return (
        f"{prefixo_do_run(camada, fonte, entidade, run_id, data)}"
        f"{sanitizar_run_id(nome)}.{_EXTENSAO[camada]}"
    )


def raw_para_staging_key(key_raw: str) -> str:
    """Key de staging equivalente a uma key de raw."""
    if not key_raw.startswith(f"{CAMADA_RAW}/") or not key_raw.endswith(".json"):
        raise ValueError(f"key de raw inválida: {key_raw}")
    miolo = key_raw[len(CAMADA_RAW) + 1 : -len(".json")]
    return f"{CAMADA_STAGING}/{miolo}.parquet"


# ── cadeia entre DAGs ────────────────────────────────────────────────────


def uri_asset(key_prefixo: str) -> str:
    """URI do Asset que representa tudo o que existe sob ``key_prefixo``.

    É estático por entidade, sem ``run_id``: a identidade do Asset é o que o
    scheduler usa para casar produtor e consumidor. A key do run vai no
    ``extra`` do evento.
    """
    return f"s3://{BUCKET}/{key_prefixo.rstrip('/')}"


def eventos_em_ordem(eventos: Iterable[Any]) -> list[Any]:
    """Eventos de Asset do mais antigo para o mais novo, pelo ``timestamp``.

    O Airflow não garante a ordem de ``triggering_asset_events``: os eventos
    voltam pela relação ``consumed_asset_events``, sem ``ORDER BY``. Quem lê
    mais de um evento -- a produtora rodou de novo antes da consumidora --
    precisa ordenar antes de escolher o mais novo ou de aplicar em sequência.
    """
    return sorted(eventos, key=lambda evento: evento.timestamp)


def cadeia_do_gatilho(
    extras: Iterable[Mapping[str, Any]], conf: Mapping[str, Any] | None = None
) -> dict[str, str]:
    """Cadeia ``entidade -> key`` do evento que disparou o run.

    ``extras`` são os ``extra`` dos eventos do Asset de gatilho, do mais
    antigo para o mais novo (ver ``eventos_em_ordem``); vale o mais novo. Sem evento (run manual), vale
    o ``conf`` do run -- é por ele que se reprocessa um arquivo específico.
    """
    ultimo: Mapping[str, Any] = {}
    for extra in extras:
        ultimo = extra
    origem = ultimo or conf or {}
    return {str(k): str(v) for k, v in origem.items()}


def key_da_cadeia(cadeia: Mapping[str, str], entidade: str) -> str:
    try:
        return cadeia[entidade]
    except KeyError:
        raise KeyError(
            f"a cadeia que disparou este run não traz '{entidade}' "
            f"(tem: {sorted(cadeia)}). Para rodar à mão, passe a key no conf: "
            f'{{"{entidade}": "staging/..."}}'
        ) from None


# ── conversão JSON → Parquet ─────────────────────────────────────────────


def _como_texto(valor: Any) -> str | None:
    """Toda coluna de staging é texto, como era no Postgres; a tipagem é do dbt.

    Listas e dicts viram JSON (o ``insert_data`` gravava o ``repr`` do Python,
    que nenhum parser lê de volta).
    """
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (list, dict)):
        return json.dumps(valor, ensure_ascii=False, default=str)
    return str(valor)


def registros_para_parquet(
    registros: list[dict[str, Any]],
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    dt_ingest: datetime | None = None,
) -> bytes:
    """Achata ``registros`` (``__`` entre níveis) e devolve o Parquet em bytes.

    ``enriquecer`` recebe cada registro e devolve os campos a acrescentar --
    é onde entra o que não vem da origem (território, política pública), para
    o raw continuar sendo exatamente o que a API respondeu.
    """
    if enriquecer is not None:
        registros = [{**r, **enriquecer(r)} for r in registros]

    df = pd.json_normalize(registros, sep="__") if registros else pd.DataFrame()
    # O insert_data baixava as chaves para minúsculo, e é com esses nomes que
    # as tabelas e a bronze existem: o backend do TransfereGov responde em
    # camelCase (tipoAnexo.auditLogin), e a coluna é tipoanexo__auditlogin.
    df.columns = [str(coluna).lower() for coluna in df.columns]
    df["dt_ingest"] = (dt_ingest or datetime.now(timezone.utc)).isoformat()
    df = df.astype(object).apply(lambda coluna: coluna.map(_como_texto))
    df = df.astype("string")

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    return buffer.getvalue()


def parquet_para_registros(df: pd.DataFrame, chave: list[str]) -> list[dict[str, Any]]:
    """Linhas de um Parquet de staging prontas para ``insert_data`` com upsert.

    ``pd.NA`` vira ``None`` (o psycopg2 não sabe adaptar ``NA``), e linhas
    com a mesma ``chave`` ficam só na última ocorrência: duas no mesmo
    INSERT fariam o ``ON CONFLICT DO UPDATE`` recusar o lote inteiro.
    """
    if df.empty:
        return []
    unicas = df.drop_duplicates(subset=chave, keep="last")
    if len(unicas) < len(df):
        logging.warning(
            "[datalakehouse] %d linhas com %s repetido -- fica a última",
            len(df) - len(unicas),
            chave,
        )
    registros: list[dict[str, Any]] = (
        unicas.astype(object).where(unicas.notna(), None).to_dict(orient="records")  # type: ignore[assignment]
    )
    return registros


# ── MinIO ────────────────────────────────────────────────────────────────


@functools.cache
def get_hook() -> S3Hook:
    """``S3Hook`` da conexão ``minio_datalakehouse``, com o bucket garantido.

    Em cache: sem isso, cada leitura e cada escrita pagava um HeadBucket --
    3000 a mais numa rodada da ``download_anexos_transferegov_dag``.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=CONN_ID)
    if not hook.check_for_bucket(BUCKET):
        hook.create_bucket(bucket_name=BUCKET)
    return hook


def run_atual() -> tuple[str, datetime]:
    """``run_id`` e data de partição da execução corrente.

    Em run manual no Airflow 3 o ``logical_date`` pode vir vazio; aí vale o
    ``run_after``, que sempre existe.
    """
    from airflow.sdk import get_current_context

    contexto = get_current_context()
    dag_run = contexto["dag_run"]
    data = contexto.get("logical_date") or dag_run.run_after
    return dag_run.run_id, data


def gravar_raw(registros: list[dict[str, Any]], fonte: str, entidade: str) -> str:
    """Grava ``registros`` como JSON em ``raw/`` e devolve a key.

    Só a key deve trafegar por XCom -- a lista inteira estoura o IPC do
    Airflow 3 com alguns milhares de registros.
    """
    run_id, data = run_atual()
    key = caminho(CAMADA_RAW, fonte, entidade, run_id, data)
    get_hook().load_string(
        string_data=json.dumps(registros, ensure_ascii=False, default=str),
        key=key,
        bucket_name=BUCKET,
        replace=True,
    )
    logging.info(
        "[datalakehouse] %d registros gravados em s3://%s/%s",
        len(registros),
        BUCKET,
        key,
    )
    return key


def ler_json(key: str) -> Any:
    """Conteúdo de um JSON do bucket, com os tipos como foram gravados."""
    return json.loads(get_hook().read_key(key=key, bucket_name=BUCKET))


def ler_raw(key: str) -> list[dict[str, Any]]:
    registros: list[dict[str, Any]] = json.loads(
        get_hook().read_key(key=key, bucket_name=BUCKET)
    )
    return registros


def raw_para_staging(
    key_raw: str,
    enriquecer: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> str:
    """Converte o JSON de ``key_raw`` em Parquet no staging e devolve a key."""
    registros = ler_raw(key_raw)
    key_staging = raw_para_staging_key(key_raw)
    get_hook().load_bytes(
        bytes_data=registros_para_parquet(registros, enriquecer=enriquecer),
        key=key_staging,
        bucket_name=BUCKET,
        replace=True,
    )
    logging.info(
        "[datalakehouse] %d registros convertidos em s3://%s/%s",
        len(registros),
        BUCKET,
        key_staging,
    )
    return key_staging


def listar_objetos(prefixo_busca: str) -> list[dict[str, Any]]:
    """Objetos (``Key``, ``LastModified``...) sob ``prefixo_busca``."""
    paginador = get_hook().get_conn().get_paginator("list_objects_v2")
    objetos: list[dict[str, Any]] = []
    for pagina in paginador.paginate(Bucket=BUCKET, Prefix=prefixo_busca):
        objetos.extend(pagina.get("Contents", []))
    return objetos


def ler_parquet(key: str) -> pd.DataFrame:
    corpo = get_hook().get_key(key=key, bucket_name=BUCKET).get()["Body"].read()
    return pd.read_parquet(io.BytesIO(corpo))


def gravar_bytes(conteudo: bytes, key: str) -> str:
    """Grava um binário (anexo, por exemplo) em ``key`` e devolve a key."""
    get_hook().load_bytes(bytes_data=conteudo, key=key, bucket_name=BUCKET, replace=True)
    return key


def gravar_arquivo(caminho_local: str, key: str) -> str:
    """Envia um arquivo local para ``key`` sem carregá-lo em memória.

    Para o que não cabe com folga num ``bytes`` -- o ZIP de 2025 do Censo
    Escolar tem 537 MB.
    """
    get_hook().load_file(
        filename=caminho_local, key=key, bucket_name=BUCKET, replace=True
    )
    logging.info("[datalakehouse] %s enviado para s3://%s/%s", caminho_local, BUCKET, key)
    return key


def baixar_arquivo(key: str, caminho_local: str) -> str:
    """Baixa ``key`` para ``caminho_local`` em streaming e devolve o caminho."""
    get_hook().get_key(key=key, bucket_name=BUCKET).download_file(caminho_local)
    return caminho_local


# ── Assets ───────────────────────────────────────────────────────────────


def asset(key_prefixo: str) -> Asset:
    """Asset de um prefixo do bucket (ver ``uri_asset``)."""
    from airflow.sdk import Asset

    return Asset(uri=uri_asset(key_prefixo))


def asset_staging(fonte: str, entidade: str) -> Asset:
    return asset(prefixo(CAMADA_STAGING, fonte, entidade))


def ler_cadeia(asset_gatilho: Asset) -> dict[str, str]:
    """Cadeia ``entidade -> key`` do evento de ``asset_gatilho`` que disparou o run."""
    from airflow.sdk import get_current_context

    contexto = get_current_context()
    eventos = eventos_em_ordem(contexto["triggering_asset_events"].get(asset_gatilho, []))
    return cadeia_do_gatilho(
        (evento.extra for evento in eventos), contexto["dag_run"].conf
    )


def publicar_cadeia(asset_saida: Asset, cadeia: Mapping[str, str]) -> None:
    """Põe ``cadeia`` no ``extra`` do evento que a task vai emitir em ``asset_saida``.

    A task precisa declarar ``asset_saida`` em ``outlets``; o evento só sai se
    ela terminar com sucesso.
    """
    from airflow.sdk import get_current_context

    get_current_context()["outlet_events"][asset_saida].extra.update(cadeia)
    logging.info("[datalakehouse] Evento em %s: %s", asset_saida.uri, dict(cadeia))
