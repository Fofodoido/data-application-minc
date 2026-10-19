"""Caminhos e nomes que a governança usa, resolvidos do ambiente.

Nada de nome de instância escrito em YAML: o serviço e o database vêm do
mesmo ambiente que as recipes e o `lineage.py` leem, para os três apontarem
sempre para o mesmo lugar.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

#: `helpers/openmetadata/governanca/config.py` -> raiz do repositório. No
#: container é o mesmo que `AIRFLOW_REPO_BASE`; localmente, o clone.
RAIZ = Path(os.environ.get("AIRFLOW_REPO_BASE") or Path(__file__).resolve().parents[3])

PROJETO_DBT = RAIZ / os.environ.get("OM_DBT_PROJECT_DIR", "dbt/minc").strip("/")
GOVERNANCA = PROJETO_DBT / "governance"
GLOSSARIO = RAIZ / "helpers" / "openmetadata" / "glossaries" / "minc.yaml"
#: Saída local: payloads de simulação e o manifest gerado por `dbt parse`.
BUILD = RAIZ / "build" / "openmetadata"


def servico() -> str:
    """Serviço de banco no OpenMetadata — o `serviceName` das recipes."""
    return os.environ.get("OM_SERVICE", "MinC")


def banco() -> str:
    """Database no OpenMetadata.

    É o `database` da conexão das recipes, que sai de `DB_DW_DBNAME` — e por
    isso `DB_DW_DBNAME` vence. `OM_DATABASE` fica só como recurso para quando
    ele não existe: até 2026-10 o `local.env` trazia `OM_DATABASE=minc`, o
    database antigo, e quem copiou o modelo mirava 647 tabelas paradas
    enquanto o catálogo vivo estava em `dbanalytics`.
    """
    return (
        os.environ.get("DB_DW_DBNAME") or os.environ.get("OM_DATABASE") or "dbanalytics"
    )


def servico_de_pipeline() -> str:
    return os.environ.get("OM_PIPELINE_SERVICE", "MinC - Airflow")


@dataclass(frozen=True)
class Acesso:
    url: str | None
    token: str | None


#: Nomes aceitos para URL e token, na ordem de preferência. O primeiro par é o
#: das Variables do Airflow (a DAG passa os valores explicitamente); o segundo,
#: o que o `.env` local usa.
VARS_URL = ("OM_HOST", "OPENMETADATA_URL")
VARS_TOKEN = ("INGESTION_TOKEN", "OM_INGESTION_TOKEN", "OPENMETADATA_JWT_TOKEN")


def carregar_env(arquivo: Path | None = None) -> None:
    """Carrega o `.env` da raiz sem sobrescrever o que já veio do ambiente."""
    arquivo = arquivo or RAIZ / ".env"
    if not arquivo.exists():
        return
    for linha in arquivo.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, _, valor = linha.partition("=")
        os.environ.setdefault(chave.strip(), valor.strip().strip('"').strip("'"))


def acesso() -> Acesso:
    """URL e token da instância, se houver. Sem eles o modo é offline."""
    carregar_env()
    url = next((os.environ[n] for n in VARS_URL if os.environ.get(n)), None)
    token = next((os.environ[n] for n in VARS_TOKEN if os.environ.get(n)), None)
    return Acesso(url=url, token=token)


def api_base(url: str) -> str:
    """Normaliza `https://host`, `https://host/api` e `https://host/api/v1`."""
    base = url.strip().rstrip("/")
    if base.endswith("/api/v1"):
        return base
    if base.endswith("/api"):
        return f"{base}/v1"
    return f"{base}/api/v1"
