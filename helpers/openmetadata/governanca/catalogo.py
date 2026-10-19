"""O que o repositório declara sobre cada ativo, resolvido do projeto dbt.

Calculado a cada execução a partir do `manifest.json` e dos YAMLs de
`dbt/minc/governance/`, e nunca gravado no repositório: o manifest deixou de
ser versionado (ADR 0007), e um catálogo commitado descreveria o projeto da
última vez que alguém lembrou de regenerá-lo.

**Fail-closed.** Ativo sem produto, sem camada ou com domínio que contradiz o
produto vai para `Catalogo.erros` e NÃO é catalogado com default. Uma tabela
nova não sai certificada como Gold porque ninguém escreveu nada sobre ela.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import yaml

from openmetadata.governanca import config

log = logging.getLogger(__name__)

#: Testes do dbt que viram restrição de tabela no catálogo.
TESTE_UNICO = "unique"
TESTE_NAO_NULO = "not_null"
TESTE_RELACAO = "relationships"

#: Inverso de cada tipo de relação entre termos (padrão do OpenMetadata 1.13).
#: Tipo ausente daqui é simétrico: a relação vale igual nos dois sentidos.
INVERSO_DA_RELACAO = {
    "broader": "narrower",
    "narrower": "broader",
    "partOf": "hasPart",
    "hasPart": "partOf",
    "calculatedFrom": "usedToCalculate",
    "usedToCalculate": "calculatedFrom",
}
RELACOES_SIMETRICAS = {"relatedTo", "seeAlso", "synonym", "antonym"}
RELACOES_VALIDAS = RELACOES_SIMETRICAS | set(INVERSO_DA_RELACAO)

#: "Grão: 1 linha por X." na descrição do modelo. É a convenção que os
#: schema.yml do MinC já seguem; o texto vira a propriedade `mincTableGrain`.
GRAO = re.compile(r"(?:Gr[aã]o:\s*|^)(1 linha por [^.;]+)", re.IGNORECASE | re.MULTILINE)


@dataclass
class Coluna:
    nome: str
    tipo: str
    descricao: str
    formato: str | None = None
    termos: list[str] = field(default_factory=list)
    #: termo pendurado pelo `meta.openmetadata.glossary` (o conector aplica)
    termos_do_dbt: list[str] = field(default_factory=list)


@dataclass
class Ativo:
    unique_id: str
    tipo: str  # "model" | "source"
    nome: str
    schema: str
    caminho: str
    camada: str | None
    produto: str | None
    dominio: str | None
    tier: str | None
    certificacao: str | None
    uso: str | None
    descricao: str
    materializacao: str | None
    colunas: list[Coluna]
    depends_on: list[str]
    termos: list[str] = field(default_factory=list)
    grao: str | None = None
    #: (colunas, tipo) — PRIMARY_KEY ou UNIQUE
    chaves: list[tuple[tuple[str, ...], str]] = field(default_factory=list)
    #: (coluna, unique_id do alvo, coluna do alvo)
    referencias: list[tuple[str, str, str]] = field(default_factory=list)

    def fqn(self, servico: str, banco: str) -> str:
        return f"{servico}.{banco}.{self.schema}.{self.nome}"


@dataclass
class Catalogo:
    ativos: dict[str, Ativo]
    metricas: list[dict]
    semantic_models: dict[str, dict]
    governanca: dict[str, dict]
    erros: list[str]
    avisos: list[str]

    def por_nome(self) -> dict[str, Ativo]:
        return {a.nome: a for a in self.ativos.values() if a.tipo == "model"}

    def do_produto(self, produto: str) -> list[Ativo]:
        return [a for a in self.ativos.values() if a.produto == produto]


# ── entrada ──────────────────────────────────────────────────────────────
def carregar_yaml(nome: str, pasta: Path | None = None) -> dict:
    caminho = (pasta or config.GOVERNANCA) / nome
    if not caminho.exists():
        raise FileNotFoundError(f"Arquivo de governança ausente: {caminho}")
    return yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}


ARQUIVOS = (
    "dominios.yml",
    "schemas.yml",
    "servicos.yml",
    "termos_minc.yml",
    "formatos.yml",
    "qualidade.yml",
    "contratos.yml",
    "metricas.yml",
)


def carregar_governanca(pasta: Path | None = None) -> dict[str, dict]:
    return {nome.removesuffix(".yml"): carregar_yaml(nome, pasta) for nome in ARQUIVOS}


def gerar_manifest(destino: Path | None = None) -> Path:
    """Roda `dbt parse` e devolve o caminho do manifest gerado.

    `parse` não abre conexão com o banco: funciona sem VPN, ao contrário do
    `docs generate`. Target e log vão para `destino`, nunca para dentro do
    projeto dbt, que no container é montado do repositório.
    """
    destino = destino or config.BUILD / "dbt_target"
    dbt = shutil.which("dbt")
    if dbt is None:
        raise FileNotFoundError("CLI `dbt` não encontrado no PATH.")
    subprocess.run(
        [
            dbt,
            "parse",
            "--project-dir",
            str(config.PROJETO_DBT),
            "--profiles-dir",
            str(config.PROJETO_DBT),
            "--target-path",
            str(destino),
            "--log-path",
            str(destino / "logs"),
            "--quiet",
        ],
        check=True,
    )
    return destino / "manifest.json"


def carregar_manifest(caminho: Path | str | None = None) -> dict:
    caminho = Path(caminho) if caminho else gerar_manifest()
    manifest: dict = json.loads(Path(caminho).read_text(encoding="utf-8"))
    return manifest


# ── resolução ────────────────────────────────────────────────────────────
def caminho_relativo(no: dict) -> str:
    """Caminho do arquivo dentro de `models/`, com `/` como separador."""
    bruto = no.get("original_file_path") or ""
    return bruto.split("models/", 1)[-1] if "models/" in bruto else bruto


def _meta_om(no: dict) -> dict:
    meta = (no.get("config") or {}).get("meta") or no.get("meta") or {}
    return meta.get("openmetadata") or {}


def resolver_camada(no: dict, tipo: str, nome: str, dominios: dict) -> str | None:
    prefixos = dominios.get("camada_por_prefixo_de_arquivo") or {}
    base = Path(caminho_relativo(no)).stem if tipo == "model" else nome
    for prefixo, camada in prefixos.items():
        if base.startswith(prefixo):
            return str(camada)
    if tipo == "source":
        return "raw"
    caminho = caminho_relativo(no)
    pastas = dominios.get("camada_por_pasta") or {}
    # o prefixo mais longo vence: `salic_dbt/core` antes de `salic_dbt`
    for pasta in sorted(pastas, key=len, reverse=True):
        if caminho == pasta or caminho.startswith(pasta.rstrip("/") + "/"):
            return str(pastas[pasta])
    return None


def resolver_produto(
    no: dict, tipo: str, dominios: dict
) -> tuple[str | None, str | None, bool]:
    """(produto, domínio, declarado fora de produto)."""
    caminho = caminho_relativo(no)
    fonte = no.get("source_name")
    for produto in dominios.get("produtos") or []:
        selecao = produto.get("selecao") or {}
        if tipo == "model" and any(
            caminho.startswith(p) for p in selecao.get("pastas") or []
        ):
            return produto["name"], produto["domain"], False
        if tipo == "source" and fonte in (selecao.get("sources") or []):
            return produto["name"], produto["domain"], False
    for fora in dominios.get("fora_de_produto") or []:
        if tipo == "model" and fora.get("pasta") and caminho.startswith(fora["pasta"]):
            return None, fora["domain"], True
        if tipo == "source" and fora.get("source") == fonte:
            return None, fora["domain"], True
    return None, None, False


class Formatos:
    """Formato de cada coluna: `meta.formato` da coluna, ou a primeira regra."""

    def __init__(self, declarado: dict) -> None:
        self.formatos = {f["name"]: f for f in declarado.get("formatos") or []}
        self.regras = []
        for regra in declarado.get("regras") or []:
            padrao = regra.get("padrao")
            self.regras.append(
                {
                    **regra,
                    "_padrao": re.compile(padrao) if padrao else None,
                    "_colunas": {c.lower() for c in regra.get("colunas") or []},
                    "_tipos": {t.lower() for t in regra.get("tipos") or []},
                }
            )

    def do(
        self, coluna: str, tipo: str, camada: str | None, ativo: str, meta: dict
    ) -> str | None:
        explicito = meta.get("formato")
        if explicito:
            return str(explicito)
        nome, tipo = coluna.lower(), (tipo or "").lower()
        for regra in self.regras:
            if regra["_colunas"] and nome not in regra["_colunas"]:
                continue
            if regra["_padrao"] and not regra["_padrao"].search(nome):
                continue
            if regra["_tipos"] and tipo not in regra["_tipos"]:
                continue
            if regra.get("camadas") and camada not in regra["camadas"]:
                continue
            if ativo in (regra.get("exceto") or []):
                continue
            return str(regra["formato"])
        return None


def _termos_por_coluna(termos: dict) -> dict[str, list[str]]:
    mapa: dict[str, list[str]] = {}
    for fqn, colunas in (termos.get("aplica_a_colunas") or {}).items():
        for coluna in colunas:
            mapa.setdefault(coluna.lower(), []).append(fqn)
    return mapa


def _termos_por_ativo(termos: dict) -> dict[str, list[str]]:
    mapa: dict[str, list[str]] = {}
    for fqn, ativos in (termos.get("aplica_a") or {}).items():
        for ativo in ativos:
            mapa.setdefault(ativo, []).append(fqn)
    return mapa


def _testes_por_no(manifest: dict) -> dict[str, list[dict]]:
    """Testes genéricos agrupados pelo nó que testam."""
    mapa: dict[str, list[dict]] = {}
    for no in manifest.get("nodes", {}).values():
        if no.get("resource_type") != "test" or not no.get("test_metadata"):
            continue
        alvo = no.get("attached_node") or next(
            (
                d
                for d in no.get("depends_on", {}).get("nodes", [])
                if not d.startswith("test.")
            ),
            None,
        )
        if alvo:
            mapa.setdefault(alvo, []).append(no)
    return mapa


def _chaves(testes: Iterable[dict]) -> tuple[list, list]:
    """`unique` + `not_null` na mesma coluna é chave primária; só `unique`, única.

    O dbt não tem `primary key`, tem teste. Teste com `where` não entra: ele
    vale para um recorte, e uma restrição publicada sem o recorte mentiria.
    """
    unicos, nao_nulos, referencias = set(), set(), []
    for teste in testes:
        meta = teste["test_metadata"]
        if (teste.get("config") or {}).get("where"):
            continue
        coluna = teste.get("column_name") or (meta.get("kwargs") or {}).get("column_name")
        if not coluna:
            continue
        if meta["name"] == TESTE_UNICO:
            unicos.add(coluna)
        elif meta["name"] == TESTE_NAO_NULO:
            nao_nulos.add(coluna)
        elif meta["name"] == TESTE_RELACAO:
            kwargs = meta.get("kwargs") or {}
            alvo = next(
                (
                    d
                    for d in teste["depends_on"]["nodes"]
                    if d != teste.get("attached_node")
                ),
                None,
            )
            if alvo and kwargs.get("field"):
                referencias.append((coluna, alvo, kwargs["field"]))
    chaves = [
        ((c,), "PRIMARY_KEY" if c in nao_nulos else "UNIQUE") for c in sorted(unicos)
    ]
    # Mais de uma "PK" numa tabela é contradição: a primeira em ordem alfabética
    # fica primária e as outras viram únicas.
    vistas = False
    resultado = []
    for colunas, tipo in chaves:
        if tipo == "PRIMARY_KEY" and vistas:
            tipo = "UNIQUE"
        vistas = vistas or tipo == "PRIMARY_KEY"
        resultado.append((colunas, tipo))
    return resultado, referencias


def montar(manifest: dict, governanca: dict[str, dict] | None = None) -> Catalogo:
    gov = governanca or carregar_governanca()
    dominios, termos = gov["dominios"], gov["termos_minc"]
    formatos = Formatos(gov["formatos"])
    tier_padrao = dominios.get("tier_por_camada") or {}
    cert_padrao = dominios.get("certificacao_por_camada") or {}
    uso_padrao = dominios.get("permissao_de_uso_por_camada") or {}
    por_coluna = _termos_por_coluna(termos)
    por_ativo = _termos_por_ativo(termos)
    testes = _testes_por_no(manifest)
    erros: list[str] = []
    avisos: list[str] = []
    ativos: dict[str, Ativo] = {}

    nos = [
        ("model", n)
        for n in manifest.get("nodes", {}).values()
        if n.get("resource_type") == "model"
    ] + [("source", s) for s in manifest.get("sources", {}).values()]

    for tipo, no in nos:
        nome = (
            no.get("alias") or no["name"]
            if tipo == "model"
            else no.get("identifier") or no["name"]
        )
        prefixo = f"{no['source_name']}." if tipo == "source" else ""
        rotulo = f"{tipo} {prefixo}{no['name']}"
        camada = resolver_camada(no, tipo, nome, dominios)
        produto, dominio, fora = resolver_produto(no, tipo, dominios)
        meta = _meta_om(no)
        if camada is None:
            erros.append(
                f"{rotulo}: camada não resolvida (dominios.yml, camada_por_pasta)"
            )
        if produto is None and not fora:
            erros.append(
                f"{rotulo}: sem produto de dados nem `fora_de_produto` (dominios.yml)"
            )
        if meta.get("domain") and dominio and meta["domain"] != dominio:
            erros.append(
                f"{rotulo}: meta.openmetadata.domain={meta['domain']} contradiz o "
                f"domínio do produto ({dominio})"
            )
        tier = meta.get("tier") or (
            f"Tier.{tier_padrao[camada]}" if camada in tier_padrao else None
        )
        certificacao = next(
            (t for t in meta.get("tags") or [] if str(t).startswith("Certification.")),
            cert_padrao.get(camada),
        )
        colunas = []
        for col in (no.get("columns") or {}).values():
            col_meta = col.get("meta") or {}
            termos_dbt = list((col_meta.get("openmetadata") or {}).get("glossary") or [])
            formato = formatos.do(
                col["name"], col.get("data_type") or "", camada, no["name"], col_meta
            )
            if formato and formato not in formatos.formatos:
                erros.append(f"{rotulo}.{col['name']}: formato '{formato}' não declarado")
            colunas.append(
                Coluna(
                    nome=col["name"],
                    tipo=col.get("data_type") or "",
                    descricao=col.get("description") or "",
                    formato=formato,
                    termos=sorted(
                        set(por_coluna.get(col["name"].lower(), [])) | set(termos_dbt)
                    ),
                    termos_do_dbt=termos_dbt,
                )
            )
        chaves, referencias = _chaves(testes.get(no["unique_id"], []))
        grao = GRAO.search(no.get("description") or "")
        ativos[no["unique_id"]] = Ativo(
            unique_id=no["unique_id"],
            tipo=tipo,
            nome=nome,
            schema=no["schema"],
            caminho=no.get("original_file_path") or "",
            camada=camada,
            produto=produto,
            dominio=dominio,
            tier=tier,
            certificacao=certificacao,
            uso=uso_padrao.get(camada) if camada else None,
            descricao=no.get("description") or "",
            materializacao=(no.get("config") or {}).get("materialized"),
            colunas=colunas,
            depends_on=list((no.get("depends_on") or {}).get("nodes") or []),
            # O termo que o `meta.openmetadata.glossary` do modelo pendura entra no
            # desejado: fora dele, o sync o tiraria e o conector o devolveria na
            # execução seguinte, alternando para sempre.
            termos=sorted(
                set(por_ativo.get(no["name"], [])) | set(meta.get("glossary") or [])
            ),
            grao=" ".join(grao.group(1).split()) if grao else None,
            chaves=chaves,
            referencias=referencias,
        )

    for fqn, alvos in (termos.get("aplica_a") or {}).items():
        conhecidos = {a.nome for a in ativos.values()} | {n["name"] for _, n in nos}
        for alvo in alvos:
            if alvo not in conhecidos:
                erros.append(
                    f"termos_minc.yml: {fqn} aplica-se a '{alvo}', que não existe"
                )

    semantic = {s["name"]: s for s in manifest.get("semantic_models", {}).values()}
    return Catalogo(
        ativos=ativos,
        metricas=list(manifest.get("metrics", {}).values()),
        semantic_models=semantic,
        governanca=gov,
        erros=erros,
        avisos=avisos,
    )


def relacoes_de_termos(termos: dict) -> dict[str, set[tuple[str, str]]]:
    """Relações desejadas de cada termo, já com o inverso aplicado.

    A relação é declarada num sentido só no YAML. Como o patch de
    `relatedTerms` SUBSTITUI a lista, cada termo precisa receber a lista
    completa — inclusive o que veio do outro lado —, senão o segundo patch
    apagaria a aresta que o primeiro criou.
    """
    mapa: dict[str, set[tuple[str, str]]] = {}
    for rel in termos.get("relacoes") or []:
        de, tipo, para = rel["de"], rel["tipo"], rel["para"]
        mapa.setdefault(de, set()).add((tipo, para))
        inverso = tipo if tipo in RELACOES_SIMETRICAS else INVERSO_DA_RELACAO[tipo]
        mapa.setdefault(para, set()).add((inverso, de))
    return mapa
