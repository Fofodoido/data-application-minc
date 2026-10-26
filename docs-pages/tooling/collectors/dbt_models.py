"""
dbt_models.py: inventário e linhagem dos modelos dbt.

Lê a árvore de arquivos, não o `manifest.json`. A convenção de pastas do
repositório — `models/<dominio>_dbt/<camada>/<modelo>.sql`, ou, na estrutura do
ADR 0010, `models/<fonte>/<camada>_<entidade>.sql` — já carrega domínio e
camada, e os `ref()`/`source()` no SQL dão a linhagem. Assim a coleta roda
offline: sem dbt instalado, sem VPN e sem conexão com o Postgres.

O preço dessa escolha é acoplamento à convenção de pastas. Se alguém reorganizar
`models/`, este coletor para de classificar direito — e é por isso que a
convenção está escrita no CLAUDE.md.

Saída: docs-pages/src/_data/dbt.json
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from tooling.common import MODELS_DIR, log, write_json

RE_REF = re.compile(r"""ref\(\s*['"]([^'"]+)['"]\s*\)""")
RE_SOURCE = re.compile(r"""source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")
RE_MATERIALIZED = re.compile(r"""materialized\s*=\s*['"](\w+)['"]""")

CAMADAS = ("bronze", "silver", "gold", "views")

# Camada pelo prefixo do nome do arquivo, para a estrutura plana do ADR 0010.
PREFIXOS_CAMADA = {"bronze_": "bronze", "silver_": "silver", "int_": "intermediate"}

# Produtos de dados (Gold, ADR 0010) moram em `data_products/<produto>/`, e o
# arquivo não leva prefixo de camada. O produto é a subpasta: não há lista a
# manter em dia.
DATA_PRODUCTS = "data_products"

# Convenção de nome do Cultura em Números: eixo<N>_meta<M>_<nome>. A meta é o
# que liga a gold ao domínio do site (`meta:` em dominios.yml).
RE_META = re.compile(r"^eixo\d+_meta(\d+)_")


def _yaml_da_pasta(pasta: Path) -> dict[str, Any]:
    """Junta os modelos do schema.yml e dos schema_*.yml da pasta.

    A pasta salic/ tem um schema_<banco>.yml por banco de origem do SALIC,
    além do schema.yml. Arquivo ausente ou inválido não entra.
    """
    modelos: list[Any] = []
    for schema in sorted([pasta / "schema.yml", *pasta.glob("schema_*.yml")]):
        if not schema.exists():
            continue
        try:
            dados = yaml.safe_load(schema.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as erro:
            log.warning("%s inválido em %s: %s", schema.name, pasta, erro)
            continue
        modelos += dados.get("models") or []
    return {"models": modelos}


def _metadados(pasta: Path) -> dict[str, dict[str, Any]]:
    """Extrai descrição, colunas descritas e contagem de testes por modelo."""
    dados = _yaml_da_pasta(pasta)
    saida: dict[str, dict[str, Any]] = {}
    for modelo in dados.get("models") or []:
        if not isinstance(modelo, dict) or not modelo.get("name"):
            continue
        colunas = [c for c in modelo.get("columns") or [] if isinstance(c, dict)]
        testes = len(modelo.get("tests") or modelo.get("data_tests") or [])
        for coluna in colunas:
            testes += len(coluna.get("tests") or coluna.get("data_tests") or [])
        saida[modelo["name"]] = {
            "descricao": (modelo.get("description") or "").strip(),
            "colunas": [
                {
                    "nome": c.get("name", ""),
                    "descricao": (c.get("description") or "").strip(),
                }
                for c in colunas
            ],
            "testes": testes,
        }
    return saida


def _classificar(relativo: Path) -> tuple[str, str]:
    """Devolve (domínio, camada) a partir do caminho relativo a models/."""
    partes = relativo.parts[:-1]
    if not partes:
        return "geral", "outros"
    if partes[0] == DATA_PRODUCTS:
        return (partes[1] if len(partes) > 1 else "geral"), "gold"
    dominio = partes[0].removesuffix("_dbt")
    camada = next((p for p in partes if p in CAMADAS), None)
    if camada is None:
        # Estrutura do ADR 0010: arquivos planos na pasta da fonte, e a camada
        # está no prefixo do nome (`silver_agent.sql`), não numa subpasta.
        camada = next(
            (
                c
                for prefixo, c in PREFIXOS_CAMADA.items()
                if relativo.stem.startswith(prefixo)
            ),
            "outros",
        )
    return dominio, camada


def coletar() -> dict[str, Any]:
    if not MODELS_DIR.exists():
        log.warning("pasta de modelos não encontrada: %s", MODELS_DIR)
        vazio: dict[str, Any] = {
            "modelos": [],
            "dominios": {},
            "sources": [],
            "totais": {},
        }
        write_json("dbt", vazio)
        return vazio

    modelos: list[dict[str, Any]] = []
    cache_meta: dict[Path, dict[str, dict[str, Any]]] = {}

    for arquivo in sorted(MODELS_DIR.rglob("*.sql")):
        relativo = arquivo.relative_to(MODELS_DIR)
        dominio, camada = _classificar(relativo)
        sql = arquivo.read_text(encoding="utf-8", errors="replace")

        if arquivo.parent not in cache_meta:
            cache_meta[arquivo.parent] = _metadados(arquivo.parent)
        meta = cache_meta[arquivo.parent].get(arquivo.stem, {})

        materializacao = RE_MATERIALIZED.search(sql)
        meta_produto = RE_META.match(arquivo.stem) if camada == "gold" else None
        modelos.append(
            {
                "nome": arquivo.stem,
                "dominio": dominio,
                "camada": camada,
                "meta": int(meta_produto.group(1)) if meta_produto else None,
                "caminho": str(arquivo.relative_to(MODELS_DIR.parents[2])),
                "descricao": meta.get("descricao", ""),
                "colunas": meta.get("colunas", []),
                "colunas_descritas": sum(
                    1 for c in meta.get("colunas", []) if c["descricao"]
                ),
                "testes": meta.get("testes", 0),
                "materializacao": materializacao.group(1) if materializacao else "",
                "depende_de": sorted(set(RE_REF.findall(sql))),
                "sources": sorted({f"{a}.{b}" for a, b in RE_SOURCE.findall(sql)}),
                "linhas_sql": sql.count("\n") + 1,
            }
        )

    # sources declaradas — a lista de origem, que o site mostra como "de onde vem".
    # Vêm do sources.yml da raiz e dos sources*.yml das pastas: a source `salic`
    # é uma só, declarada num sources_<banco>.yml por banco. Mesmo nome e schema
    # em vários arquivos viram uma entrada, com as tabelas somadas.
    por_chave: dict[tuple[str, str], dict[str, Any]] = {}
    for arquivo_sources in sorted(MODELS_DIR.rglob("sources*.yml")):
        try:
            dados = yaml.safe_load(arquivo_sources.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as erro:
            log.warning("%s inválido: %s", arquivo_sources.name, erro)
            continue
        for grupo in dados.get("sources") or []:
            nome = grupo.get("name", "")
            schema = grupo.get("schema", nome)
            entrada = por_chave.setdefault(
                (nome, schema),
                {"nome": nome, "schema": schema, "descricao": "", "tabelas": []},
            )
            if not entrada["descricao"]:
                entrada["descricao"] = (grupo.get("description") or "").strip()
            entrada["tabelas"] += [
                {
                    "nome": t.get("name", ""),
                    "descricao": (t.get("description") or "").strip(),
                }
                for t in grupo.get("tables") or []
            ]
    sources: list[dict[str, Any]] = list(por_chave.values())

    dominios: dict[str, Any] = {}
    for m in modelos:
        d = dominios.setdefault(m["dominio"], {"modelos": 0, "testes": 0, "camadas": {}})
        d["modelos"] += 1
        d["testes"] += m["testes"]
        d["camadas"][m["camada"]] = d["camadas"].get(m["camada"], 0) + 1

    payload = {
        "modelos": modelos,
        "dominios": dominios,
        "sources": sources,
        "totais": {
            "modelos": len(modelos),
            "testes": sum(m["testes"] for m in modelos),
            "com_descricao": sum(1 for m in modelos if m["descricao"]),
            "sources": sum(len(s["tabelas"]) for s in sources),
        },
    }
    write_json("dbt", payload)
    return payload
