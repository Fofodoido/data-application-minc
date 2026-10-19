"""Aplica no OpenMetadata a governança declarada em `dbt/minc/governance/`.

Cuida de tudo que diz de QUEM é o dado, o QUE ele significa, COMO ele está
escrito e SE pode ser usado:

    servicos.yml     descrição, dono e domínio do serviço, do database e do
                     serviço de pipeline
    schemas.yml      descrição e camada de cada schema
    dominios.yml     domínio, produto de dados, dono, tier, certificação,
                     permissão de uso, etiquetas e propriedades customizadas
    formatos.yml     a etiqueta `FormatoMinC` de cada coluna
    termos_minc.yml  relações tipadas entre termos e onde cada um se aplica

Roda DEPOIS dos conectores, sempre: o conector dbt escreve a tabela sem
`certification` e o `createOrUpdate` do OpenMetadata a substitui por nulo. Esta
é a etapa que a devolve.

É idempotente: duas execuções seguidas terminam com `atualizados=0`. Etiqueta
de classificação que não é nossa (PII, por exemplo, que o classifier aplica) é
preservada; termo de glossário pendurado à mão também.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Any

from openmetadata.governanca import config
from openmetadata.governanca.catalogo import Ativo, Catalogo, relacoes_de_termos
from openmetadata.governanca.cliente import (
    ClienteOM,
    Proprietarios,
    equivalente,
    etiqueta,
    operacoes_de_diferenca,
    referencia,
    texto,
)

log = logging.getLogger(__name__)

#: Classificações cujo conteúdo numa tabela é inteiramente nosso: as etiquetas
#: delas são SUBSTITUÍDAS pelo declarado. As de qualquer outra são preservadas.
CLASSIFICACAO_DE_FORMATO = "FormatoMinC"
CLASSIFICACAO_DE_DOCUMENTACAO = "DocumentacaoMinC"
CLASSIFICACOES_DA_TABELA = {"Uso", "Tier", CLASSIFICACAO_DE_DOCUMENTACAO}
CLASSIFICACOES_DA_COLUNA = {CLASSIFICACAO_DE_FORMATO, CLASSIFICACAO_DE_DOCUMENTACAO}

#: Camadas que recebem a vizinhança semântica. Bronze e raw ficam de fora: são
#: cópia da origem, e o que se publica delas é a topologia, não o significado.
CAMADAS_COM_RELACOES = {"silver", "intermediate", "gold"}

CAMPOS_DA_TABELA = (
    "tags,domains,owners,columns,certification,dataProducts,extension,tableConstraints"
)


# ── classificações e propriedades ────────────────────────────────────────
def sincronizar_classificacoes(om: ClienteOM, dominios: dict, formatos: dict) -> None:
    """Cria classificações e etiquetas antes de qualquer coisa usá-las.

    Classificação `compartilhada` é de outros órgãos também: só acrescentamos
    a etiqueta que falta, nunca alteramos o que existe.
    """
    log.info("Classificações")
    for declarada in dominios.get("classificacoes") or []:
        etiquetas = list(declarada.get("etiquetas") or [])
        if declarada["name"] == CLASSIFICACAO_DE_FORMATO:
            # as etiquetas de formato vivem em formatos.yml, junto das regras
            etiquetas = [
                {
                    "name": f["name"],
                    "display_name": f.get("display_name"),
                    "description": f.get("description"),
                }
                for f in formatos.get("formatos") or []
            ]
        if _sincronizar_classificacao(om, declarada):
            _sincronizar_etiquetas(
                om, declarada["name"], etiquetas, bool(declarada.get("compartilhada"))
            )

    for nativa in dominios.get("classificacoes_nativas") or []:
        if om.offline:
            continue
        if om.existe("classifications", nativa):
            om.conformes += 1
        else:
            om.falhas.append(f"classificação nativa {nativa} não existe na instância")


def _sincronizar_classificacao(om: ClienteOM, declarada: dict) -> bool:
    """Cria ou ajusta a classificação. Falso quando não dá para seguir."""
    nome = declarada["name"]
    compartilhada = bool(declarada.get("compartilhada"))
    atual = om.existe("classifications", nome)
    if atual and compartilhada:
        om.conformes += 1
    elif atual:
        # `mutuallyExclusive` é imutável depois de criada; no patch, a API
        # recusaria a atualização inteira.
        om.patch(
            "classifications",
            atual["id"],
            operacoes_de_diferenca(
                atual,
                {
                    "displayName": declarada.get("display_name", nome),
                    "description": texto(declarada.get("description")),
                },
            ),
            f"classificação {nome}",
        )
    elif compartilhada:
        if om.offline:
            om._registrar(f"conferiria a classificação compartilhada {nome}")
        else:
            om.falhas.append(
                f"classificação compartilhada {nome} não existe na instância"
            )
        return False
    else:
        om.criar(
            "classifications",
            {
                "name": nome,
                "displayName": declarada.get("display_name", nome),
                "description": texto(declarada.get("description")),
                "mutuallyExclusive": bool(declarada.get("mutuamente_exclusivas")),
            },
            nome,
        )
    return True


def _sincronizar_etiquetas(
    om: ClienteOM, classificacao: str, etiquetas: list[dict], compartilhada: bool
) -> None:
    for tag in etiquetas:
        fqn = f"{classificacao}.{tag['name']}"
        existente = om.existe("tags", fqn)
        desejada = {
            "displayName": tag.get("display_name") or tag["name"],
            "description": texto(tag.get("description")),
        }
        if existente and compartilhada:
            om.conformes += 1
        elif existente:
            om.patch(
                "tags",
                existente["id"],
                operacoes_de_diferenca(existente, desejada),
                f"etiqueta {fqn}",
            )
        else:
            om.criar(
                "tags",
                {"name": tag["name"], "classification": classificacao, **desejada},
                fqn,
            )


def sincronizar_propriedades(om: ClienteOM, dominios: dict) -> None:
    """Declara as propriedades customizadas nos tipos de entidade.

    O tipo de entidade é GLOBAL na instância: só criamos o que falta e nunca
    removemos, porque as outras podem ser de outro órgão.
    """
    declaradas = dominios.get("propriedades_customizadas") or []
    if not declaradas:
        return
    log.info("Propriedades customizadas")
    tipos = {
        t["name"]: t["id"] for t in om.listar("metadata/types", {"category": "field"})
    }
    for prop in declaradas:
        # `customProperties` só vem se for pedido; sem isso a lista chega vazia
        # e a propriedade seria recriada a cada execução.
        entidade = om.existe(
            "metadata/types", prop["entidade"], campos="customProperties"
        )
        if not entidade:
            if om.offline:
                om._registrar(
                    f"conferiria a propriedade {prop['entidade']}.{prop['name']}"
                )
            else:
                om.falhas.append(f"tipo de entidade {prop['entidade']} não existe")
            continue
        if prop["name"] in {c["name"] for c in entidade.get("customProperties") or []}:
            om.conformes += 1
            continue
        if prop["tipo"] not in tipos:
            om.falhas.append(f"tipo de campo '{prop['tipo']}' não existe")
            continue
        om.criar(
            f"metadata/types/{entidade['id']}",
            {
                "name": prop["name"],
                "description": texto(prop.get("description")),
                "propertyType": {"id": tipos[prop["tipo"]], "type": "type"},
            },
            f"{prop['entidade']}.{prop['name']}",
        )


# ── domínios, serviços e produtos ────────────────────────────────────────
def sincronizar_dominios(om: ClienteOM, donos: Proprietarios, dominios: dict) -> None:
    log.info("Domínios")
    for d in dominios.get("dominios") or []:
        # o FQN do subdomínio é `Pai.Filho`; o nome curto devolve 404
        fqn = f"{d['parent']}.{d['name']}" if d.get("parent") else d["name"]
        desejado: dict[str, Any] = {
            "displayName": d.get("display_name", d["name"]),
            "description": texto(d.get("description")),
            "owners": donos.resolver(d.get("owner_key")),
        }
        atual = om.existe("domains", fqn, campos="owners")
        if atual:
            om.patch("domains", atual["id"], operacoes_de_diferenca(atual, desejado), fqn)
            continue
        corpo = {
            "name": d["name"],
            "domainType": d.get("domain_type", "Aggregate"),
            **{k: v for k, v in desejado.items() if v is not None},
        }
        if d.get("parent"):
            corpo["parent"] = d["parent"]
        om.criar("domains", corpo, fqn)
        om.esquecer("domains", fqn)


def _documentar(
    om: ClienteOM, donos: Proprietarios, rota: str, fqn: str, decl: dict
) -> None:
    """Descrição, dono, domínio e etiqueta de uma entidade que já existe.

    Nunca cria e nunca toca em conexão: serviço e database nascem das recipes.
    """
    atual = om.existe(rota, fqn, campos="owners,domains,tags")
    if not atual:
        if om.offline:
            om._registrar(f"documentaria {rota}: {fqn}")
        else:
            om.falhas.append(f"{rota} '{fqn}' não existe; ele nasce da recipe, não daqui")
        return
    desejado: dict[str, Any] = {
        "description": texto(decl.get("description")),
        "owners": donos.resolver(decl.get("owner_key")),
    }
    dominio = om.existe("domains", decl["domain"]) if decl.get("domain") else None
    if dominio:
        desejado["domains"] = [referencia(dominio, "domain")]
    if decl.get("tags"):
        desejado["tags"] = mesclar_etiquetas(
            atual.get("tags") or [], [etiqueta(t) for t in decl["tags"]], set()
        )
    om.patch(rota, atual["id"], operacoes_de_diferenca(atual, desejado), fqn)


def sincronizar_servicos(
    om: ClienteOM, donos: Proprietarios, servicos: dict, servico: str, banco: str
) -> None:
    log.info("Serviços e database")
    alvos = [
        # o serviço vive sob `services/`; procurá-lo na raiz devolve 404
        ("services/databaseServices", servico, servicos.get("servico") or {}),
        ("databases", f"{servico}.{banco}", servicos.get("banco") or {}),
    ]
    orquestracao = servicos.get("orquestracao") or {}
    if orquestracao:
        alvos.append(("services/pipelineServices", orquestracao["name"], orquestracao))
    for rota, fqn, decl in alvos:
        if decl:
            _documentar(om, donos, rota, fqn, decl)


def etiquetas_do_produto(dominios: dict, produto: str) -> list[str]:
    automaticas = dominios.get("etiquetas_automaticas") or {}
    return [
        f
        for f in (
            automaticas.get("organizacao"),
            (automaticas.get("por_produto") or {}).get(produto),
        )
        if f
    ]


def sincronizar_produtos(om: ClienteOM, donos: Proprietarios, dominios: dict) -> None:
    log.info("Produtos de dados")
    for p in dominios.get("produtos") or []:
        dominio = om.existe("domains", p["domain"])
        desejado: dict[str, Any] = {
            "displayName": p.get("display_name", p["name"]),
            "description": texto(p.get("description")),
            "owners": donos.resolver(p.get("owner_key")),
            "tags": [etiqueta(t) for t in etiquetas_do_produto(dominios, p["name"])]
            + [etiqueta(t, "Glossary") for t in p.get("termos") or []],
        }
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
        peritos = [
            referencia(u, "user")
            for u in (om.existe("users", nome) for nome in p.get("experts") or [])
            if u
        ]
        if peritos:
            desejado["experts"] = peritos
        if p.get("meta"):
            desejado["extension"] = {"mincMetas": f"Meta {p['meta']}"}
        atual = om.existe(
            "dataProducts", p["name"], campos="owners,domains,tags,experts,extension"
        )
        if atual:
            outro = {d["fullyQualifiedName"] for d in atual.get("domains") or []}
            if outro and p["domain"] not in outro:
                # nome de produto é global: outro órgão pode ter um igual
                om.falhas.append(
                    f"produto {p['name']} já existe no domínio {sorted(outro)}; "
                    "renomeie em dominios.yml em vez de tomá-lo"
                )
                continue
            om.patch(
                "dataProducts",
                atual["id"],
                operacoes_de_diferenca(atual, desejado),
                f"produto {p['name']}",
            )
        else:
            om.criar(
                "dataProducts",
                {
                    "name": p["name"],
                    **{k: v for k, v in desejado.items() if k != "domains" and v},
                    "domains": [p["domain"]],
                },
                p["name"],
            )
            om.esquecer("dataProducts", p["name"])


# ── glossário ────────────────────────────────────────────────────────────
def sincronizar_termos(
    om: ClienteOM, donos: Proprietarios, termos_declarados: list[dict], termos: dict
) -> None:
    """Dono do glossário, conteúdo dos termos e relações tipadas entre eles.

    O conteúdo vem do CSV do glossário; a CRIAÇÃO de termo é da primeira task
    da DAG (`glossary.sync_glossary`), que precisa rodar antes do conector dbt.
    Aqui se atualiza o que mudou e se aplicam as relações, num segundo passe,
    quando todos os termos já existem.
    """
    log.info("Glossário")
    _sincronizar_glossario(om, donos, termos.get("glossario") or {})
    for t in termos_declarados:
        atual = om.existe("glossaryTerms", t["fullyQualifiedName"])
        if not atual:
            if not om.offline:
                om.falhas.append(
                    f"termo {t['fullyQualifiedName']} não existe — a task sync_glossary "
                    "da DAG é quem cria"
                )
            continue
        desejado = {
            "displayName": t["displayName"],
            "description": t["description"],
            "synonyms": t["synonyms"] or None,
            "references": t["references"] or None,
        }
        om.patch(
            "glossaryTerms",
            atual["id"],
            operacoes_de_diferenca(atual, desejado),
            f"termo {t['fullyQualifiedName']}",
        )
    for fqn, relacoes in sorted(relacoes_de_termos(termos).items()):
        _relacionar_termo(om, fqn, relacoes)


def _sincronizar_glossario(om: ClienteOM, donos: Proprietarios, declarado: dict) -> None:
    """Dono e revisores do glossário — a entidade, não os termos dele."""
    if not declarado:
        return
    glossario = om.existe("glossaries", declarado["name"], campos="owners,reviewers")
    if not glossario:
        if not om.offline:
            om.falhas.append(f"glossário {declarado['name']} não existe na instância")
        return
    desejado: dict[str, Any] = {"owners": donos.resolver(declarado.get("owner_key"))}
    revisores = [
        referencia(u, "user")
        for u in (om.existe("users", n) for n in declarado.get("revisores") or [])
        if u
    ]
    if revisores:
        desejado["reviewers"] = revisores
    om.patch(
        "glossaries",
        glossario["id"],
        operacoes_de_diferenca(glossario, desejado),
        declarado["name"],
    )


def _relacionar_termo(om: ClienteOM, fqn: str, relacoes: set[tuple[str, str]]) -> None:
    """Substitui as relações do termo pela lista completa declarada.

    `relatedTerms` aceita `TermRelation` com tipo só por PATCH; o tipo é o que
    o Explorador de Ontologia usa para rotular e colorir a aresta.
    """
    atual = om.existe("glossaryTerms", fqn, campos="relatedTerms")
    if not atual:
        if om.offline:
            om._registrar(f"relacionaria {fqn} a {len(relacoes)} termos")
        return
    desejadas = []
    for tipo, outro in sorted(relacoes):
        alvo = om.existe("glossaryTerms", outro)
        if not alvo:
            om.falhas.append(f"termo relacionado inexistente: {outro} (em {fqn})")
            continue
        desejadas.append({"relationType": tipo, "term": referencia(alvo, "glossaryTerm")})
    if equivalente(atual.get("relatedTerms") or [], desejadas):
        om.conformes += 1
        return
    om.patch(
        "glossaryTerms",
        atual["id"],
        [{"op": "add", "path": "/relatedTerms", "value": desejadas}],
        f"relações de {fqn}",
    )


# ── etiquetas ────────────────────────────────────────────────────────────
def mesclar_etiquetas(
    atuais: list[dict],
    desejadas: list[dict],
    glossario_nosso: set[str],
    classificacoes_nossas: set[str] = CLASSIFICACOES_DA_TABELA,
) -> list[dict]:
    """Substitui o que governamos e preserva o que não é nosso.

    Trocar a lista inteira apagaria PII do classifier e etiqueta de outro
    órgão; só somar deixaria etiqueta obsoleta para sempre. Termo de glossário
    só sai se for um dos que este repositório aplica por regra — o pendurado à
    mão na tela não é nosso para tirar.
    """
    preservadas = [
        tag
        for tag in atuais
        if str(tag.get("tagFQN", "")).split(".")[0] not in classificacoes_nossas
        and not (tag.get("source") == "Glossary" and tag.get("tagFQN") in glossario_nosso)
    ]
    vistas: set[str] = set()
    resultado = []
    for tag in preservadas + desejadas:
        if tag["tagFQN"] not in vistas:
            vistas.add(tag["tagFQN"])
            resultado.append(tag)
    return resultado


def etiquetas_do_ativo(dominios: dict, ativo: Ativo) -> list[dict]:
    """Organização, produto, camada, tier e permissão de uso."""
    automaticas = dominios.get("etiquetas_automaticas") or {}
    fqns = etiquetas_do_produto(dominios, ativo.produto or "")
    camada = (automaticas.get("por_camada") or {}).get(ativo.camada)
    if camada:
        fqns.append(camada)
    if ativo.tier:
        fqns.append(ativo.tier)
    if ativo.uso:
        fqns.append(ativo.uso)
    fqns += marcas_de_verificacao(dominios, ativo)
    return [etiqueta(f) for f in fqns] + [etiqueta(t, "Glossary") for t in ativo.termos]


def nao_verificada(dominios: dict, descricao: str | None) -> bool:
    """A descrição carrega a marca de documentação não confirmada."""
    padrao = (dominios.get("documentacao_nao_verificada") or {}).get("padrao")
    return bool(padrao and re.search(padrao, descricao or "", re.IGNORECASE))


def marcas_de_verificacao(dominios: dict, ativo: Ativo) -> list[str]:
    """Etiquetas de documentação não verificada da tabela.

    Duas perguntas diferentes: a descrição DA TABELA é confiável? e alguma
    COLUNA dela não é? A segunda é a que interessa a quem vai consumir uma
    silver de descrição limpa com uma coluna de significado ainda inferido.
    """
    regra = dominios.get("documentacao_nao_verificada") or {}
    marcas = []
    if regra.get("etiqueta") and nao_verificada(dominios, ativo.descricao):
        marcas.append(regra["etiqueta"])
    if regra.get("etiqueta_de_colunas") and any(
        nao_verificada(dominios, c.descricao) for c in ativo.colunas
    ):
        marcas.append(regra["etiqueta_de_colunas"])
    return marcas


def vocabulario_por_regra(termos: dict) -> set[str]:
    """Termos que este repositório aplica por regra — e portanto pode tirar."""
    return set(termos.get("aplica_a") or {}) | set(termos.get("aplica_a_colunas") or {})


# ── schemas ──────────────────────────────────────────────────────────────
def catalogar_schemas(
    om: ClienteOM, donos: Proprietarios, cat: Catalogo, servico: str, banco: str
) -> None:
    """Descrição, dono, domínio e etiqueta de camada nos schemas.

    Schema que guarda ativos de mais de um domínio (não há hoje) recebe o
    domínio raiz `Cultura`, para não afirmar uma titularidade parcial.
    """
    log.info("Schemas")
    declarados = {s["name"]: s for s in cat.governanca["schemas"].get("schemas") or []}
    dominios_por_schema: dict[str, set[str]] = defaultdict(set)
    for ativo in cat.ativos.values():
        if ativo.dominio:
            dominios_por_schema[ativo.schema].add(ativo.dominio)
    automaticas = cat.governanca["dominios"].get("etiquetas_automaticas") or {}
    for nome in sorted(dominios_por_schema):
        decl = declarados.get(nome)
        if not decl:
            om.falhas.append(f"schema '{nome}' sem entrada em schemas.yml")
            continue
        fqn = f"{servico}.{banco}.{nome}"
        atual = om.existe("databaseSchemas", fqn, campos="owners,domains,tags")
        if not atual:
            if om.offline:
                om._registrar(f"catalogaria schema {nome}")
            continue
        dominios = dominios_por_schema[nome]
        dominio = om.existe(
            "domains", next(iter(dominios)) if len(dominios) == 1 else "Cultura"
        )
        marcas = [automaticas["organizacao"]] if automaticas.get("organizacao") else []
        camada = (automaticas.get("por_camada") or {}).get(decl.get("layer"))
        if camada:
            marcas.append(camada)
        desejado: dict[str, Any] = {
            "description": texto(decl.get("description")),
            "owners": donos.resolver(decl.get("owner_key")),
            "tags": mesclar_etiquetas(
                atual.get("tags") or [], [etiqueta(m) for m in marcas], set()
            ),
        }
        if dominio:
            desejado["domains"] = [referencia(dominio, "domain")]
        om.patch(
            "databaseSchemas", atual["id"], operacoes_de_diferenca(atual, desejado), fqn
        )


# ── tabelas e colunas ────────────────────────────────────────────────────
def markdown_semantico(ativo: Ativo, cat: Catalogo, leitores: list[str]) -> str:
    """Com o que a tabela se relaciona, e por qual chave.

    Sai do grafo do dbt e dos testes — nada é inferido por semelhança de nome.
    Não cria chave estrangeira nem aresta de linhagem: é auxílio de descoberta.
    """
    linhas = [f"**Camada:** {ativo.camada}"]
    if ativo.grao:
        linhas.append(f"**Grão:** {ativo.grao}")
    pks = [c for colunas, tipo in ativo.chaves if tipo == "PRIMARY_KEY" for c in colunas]
    if pks:
        linhas.append("**Chave:** " + ", ".join(f"`{c}`" for c in pks))
    if ativo.referencias:
        linhas.append(
            "**Cruza com:** "
            + "; ".join(
                f"`{col}` → `{cat.ativos[alvo].nome}.{campo}`"
                for col, alvo, campo in ativo.referencias
                if alvo in cat.ativos
            )
        )
    origem = sorted(cat.ativos[d].nome for d in ativo.depends_on if d in cat.ativos)
    if origem:
        linhas.append("**Lê de:** " + ", ".join(f"`{n}`" for n in origem))
    if leitores:
        linhas.append("**É lido por:** " + ", ".join(f"`{n}`" for n in sorted(leitores)))
    linhas.append(
        "_Derivado do grafo do dbt e dos testes declarados. Não constitui chave "
        "estrangeira nem aresta de linhagem._"
    )
    return "\n\n".join(linhas)


def extensao_desejada(
    ativo: Ativo, cat: Catalogo, leitores: list[str], referencias: list[dict]
) -> dict[str, Any]:
    desejada: dict[str, Any] = {
        "mincCamada": ativo.camada,
        "mincModeloDbt": ativo.caminho,
    }
    if ativo.grao:
        desejada["mincTableGrain"] = ativo.grao
    if ativo.camada in CAMADAS_COM_RELACOES:
        desejada["mincSemanticRelationships"] = markdown_semantico(ativo, cat, leitores)
        if referencias:
            desejada["mincRelatedTables"] = referencias
    return {k: v for k, v in desejada.items() if v}


#: Propriedades que já podem estar preenchidas por outra fonte, e que por isso o
#: sync só PREENCHE, nunca sobrescreve. Em 2026-10 cinco tabelas de `agentes`
#: tinham nelas a cardinalidade medida via Trino e relações candidatas com o
#: SALIC (catálogo `minc-semantic-relationships` de 2026-09-08), evidência mais
#: rica que o grafo do dbt e que não existe no repositório para ser refeita.
SO_PREENCHER = {"mincSemanticRelationships", "mincRelatedTables", "mincTableGrain"}


def operacoes_de_extensao(atual: dict | None, desejada: dict[str, Any]) -> list[dict]:
    """Patch por propriedade, preservando as que não são nossas.

    Substituir `/extension` inteira apagaria propriedade de outro órgão ou
    preenchida à mão. As de `SO_PREENCHER` só entram onde estão vazias.
    """
    if not desejada:
        return []
    if not atual:
        return [{"op": "add", "path": "/extension", "value": desejada}]
    return [
        {"op": "add", "path": f"/extension/{chave}", "value": valor}
        for chave, valor in desejada.items()
        if not equivalente(atual.get(chave), valor)
        and not (chave in SO_PREENCHER and atual.get(chave) not in (None, "", []))
    ]


def restricoes_desejadas(
    om: ClienteOM, ativo: Ativo, cat: Catalogo, tabela: dict, servico: str, banco: str
) -> list[dict] | None:
    """Chaves dos testes do dbt somadas às que o banco já declara.

    O conector Postgres publica a chave física (as 5 que existem hoje). Elas
    ficam; as dos testes entram ao lado. Coluna que a tabela não tem é
    ignorada: o teste pode estar desatualizado, e a API recusaria o patch.
    Pelo mesmo motivo, a FK só entra quando a tabela referenciada já está no
    catálogo: uma dimensão ainda não materializada derrubaria o patch inteiro
    (`Table not found`), levando etiquetas e produto junto.
    """
    colunas = {c["name"] for c in tabela.get("columns") or []}
    atuais = list(tabela.get("tableConstraints") or [])
    novas = [
        {"constraintType": tipo, "columns": list(cols)}
        for cols, tipo in ativo.chaves
        if set(cols) <= colunas
    ]
    for coluna, alvo_id, campo in ativo.referencias:
        alvo = cat.ativos.get(alvo_id)
        if alvo and coluna in colunas and om.existe("tables", alvo.fqn(servico, banco)):
            novas.append(
                {
                    "constraintType": "FOREIGN_KEY",
                    "columns": [coluna],
                    "referredColumns": [f"{alvo.fqn(servico, banco)}.{campo}"],
                }
            )
    # uma coluna já com PK física não recebe uma UNIQUE de teste por cima
    com_chave = {tuple(c.get("columns") or ()) for c in atuais}
    desejadas = atuais + [
        n
        for n in novas
        if n["constraintType"] == "FOREIGN_KEY" or tuple(n["columns"]) not in com_chave
    ]
    unicas: dict[tuple, dict] = {}
    for restricao in desejadas:
        chave = (
            restricao["constraintType"],
            tuple(restricao.get("columns") or ()),
            tuple(restricao.get("referredColumns") or ()),
        )
        unicas.setdefault(chave, restricao)
    desejadas = list(unicas.values())
    if equivalente(atuais, desejadas):
        return None
    return desejadas


def operacoes_de_colunas(
    ativo: Ativo, tabela: dict, glossario_nosso: set[str], dominios: dict
) -> list[dict]:
    """Formato, verificação da descrição e termos de glossário em cada coluna.

    O índice sai da resposta da API (`/columns/<i>/tags`), nunca de uma ordem
    presumida: a ordem do catálogo é a do banco, não a do YAML.
    """
    declaradas = {c.nome: c for c in ativo.colunas}
    marca = (dominios.get("documentacao_nao_verificada") or {}).get("etiqueta")
    operacoes = []
    for indice, coluna in enumerate(tabela.get("columns") or []):
        decl = declaradas.get(coluna["name"])
        if not decl:
            continue
        marcas = []
        if decl.formato:
            marcas.append(etiqueta(f"{CLASSIFICACAO_DE_FORMATO}.{decl.formato}"))
        if marca and nao_verificada(dominios, decl.descricao):
            marcas.append(etiqueta(marca))
        marcas += [etiqueta(t, "Glossary") for t in decl.termos]
        desejadas = mesclar_etiquetas(
            coluna.get("tags") or [], marcas, glossario_nosso, CLASSIFICACOES_DA_COLUNA
        )
        if not equivalente(coluna.get("tags") or [], desejadas):
            operacoes.append(
                {"op": "add", "path": f"/columns/{indice}/tags", "value": desejadas}
            )
    return operacoes


def catalogar_ativos(
    om: ClienteOM, donos: Proprietarios, cat: Catalogo, servico: str, banco: str
) -> dict[str, int]:
    """Domínio, produto, dono, etiquetas, certificação, propriedades, chaves e
    colunas de cada tabela que o projeto dbt declara.

    A ordem importa: o OpenMetadata recusa vincular tabela a produto se ela
    ainda não está no domínio do produto (`Data Product Domain Validation`).
    Domínio num patch, depois o resto.
    """
    log.info("Tabelas e colunas")
    dominios = cat.governanca["dominios"]
    glossario_nosso = vocabulario_por_regra(cat.governanca["termos_minc"])
    leitores: dict[str, list[str]] = defaultdict(list)
    for ativo in cat.ativos.values():
        for d in ativo.depends_on:
            leitores[d].append(ativo.nome)
    por_schema: dict[str, dict[str, Ativo]] = defaultdict(dict)
    for ativo in cat.ativos.values():
        por_schema[ativo.schema][ativo.nome] = ativo

    produtos = {
        p["name"]: om.existe("dataProducts", p["name"])
        for p in dominios.get("produtos") or []
    }
    contagem = {"tabelas": 0, "ausentes": 0, "fora_do_dbt": 0}
    for schema, ativos in sorted(por_schema.items()):
        fqn_schema = f"{servico}.{banco}.{schema}"
        if om.offline:
            om._registrar(f"catalogaria {len(ativos)} ativos de {schema}")
            continue
        tabelas = {
            t["name"]: t
            for t in om.listar(
                "tables", {"databaseSchema": fqn_schema, "fields": CAMPOS_DA_TABELA}
            )
        }
        contagem["fora_do_dbt"] += len(set(tabelas) - set(ativos))
        for nome, ativo in sorted(ativos.items()):
            tabela = tabelas.get(nome)
            if not tabela:
                contagem["ausentes"] += 1
                continue
            contagem["tabelas"] += 1
            catalogar_tabela(
                om,
                donos,
                cat,
                ativo,
                tabela,
                produtos,
                glossario_nosso,
                leitores.get(ativo.unique_id, []),
                servico,
                banco,
            )
    log.info(
        "  %(tabelas)s tabelas catalogadas; %(ausentes)s declaradas no dbt e ainda "
        "não materializadas no catálogo; %(fora_do_dbt)s no catálogo sem declaração "
        "no dbt (não tocadas)",
        contagem,
    )
    return contagem


def dono_do_ativo(dominios: dict, ativo: Ativo) -> str | None:
    """O dono do produto do ativo; fora de produto, o único proprietário declarado."""
    for produto in dominios.get("produtos") or []:
        if produto["name"] == ativo.produto:
            return str(produto.get("owner_key"))
    proprietarios = list(dominios.get("proprietarios") or {})
    return proprietarios[0] if len(proprietarios) == 1 else None


def catalogar_tabela(
    om: ClienteOM,
    donos: Proprietarios,
    cat: Catalogo,
    ativo: Ativo,
    tabela: dict,
    produtos: dict[str, dict | None],
    glossario_nosso: set[str],
    leitores: list[str],
    servico: str,
    banco: str,
) -> None:
    rotulo = f"{ativo.schema}.{ativo.nome}"
    dominio = om.existe("domains", ativo.dominio) if ativo.dominio else None
    if dominio:
        om.patch(
            "tables",
            tabela["id"],
            operacoes_de_diferenca(tabela, {"domains": [referencia(dominio, "domain")]}),
            f"domínio de {rotulo}",
        )

    desejado: dict[str, Any] = {
        "owners": donos.resolver(dono_do_ativo(cat.governanca["dominios"], ativo)),
        "tags": mesclar_etiquetas(
            tabela.get("tags") or [],
            etiquetas_do_ativo(cat.governanca["dominios"], ativo),
            glossario_nosso,
        ),
    }
    produto = produtos.get(ativo.produto or "")
    if produto:
        desejado["dataProducts"] = [referencia(produto, "dataProduct")]
    operacoes = operacoes_de_diferenca(tabela, desejado)

    vizinhos = []
    for outro_id in ativo.depends_on:
        outro = cat.ativos.get(outro_id)
        entidade = om.existe("tables", outro.fqn(servico, banco)) if outro else None
        if entidade:
            vizinhos.append(referencia(entidade, "table"))
    operacoes += operacoes_de_extensao(
        tabela.get("extension"), extensao_desejada(ativo, cat, leitores, vizinhos)
    )
    restricoes = restricoes_desejadas(om, ativo, cat, tabela, servico, banco)
    if restricoes is not None:
        operacoes.append({"op": "add", "path": "/tableConstraints", "value": restricoes})
    operacoes += operacoes_de_colunas(
        ativo, tabela, glossario_nosso, cat.governanca["dominios"]
    )
    om.patch("tables", tabela["id"], operacoes, rotulo)
    aplicar_certificacao(om, tabela, ativo.certificacao, rotulo)


def aplicar_certificacao(
    om: ClienteOM, entidade: dict, fqn: str | None, rotulo: str
) -> None:
    """A certificação no campo próprio, se ainda não for a declarada.

    Vai em patch separado: é o campo que o conector dbt apaga, e ele não pode
    depender do sucesso do resto. O OpenMetadata preenche a validade sozinho;
    é o sync recorrente que renova.
    """
    if not fqn:
        return
    atual = ((entidade.get("certification") or {}).get("tagLabel") or {}).get("tagFQN")
    if atual == fqn:
        om.conformes += 1
        return
    om.patch(
        "tables",
        entidade["id"],
        [{"op": "add", "path": "/certification", "value": {"tagLabel": etiqueta(fqn)}}],
        f"certificação de {rotulo}",
    )


# ── orquestração ─────────────────────────────────────────────────────────
def sincronizar(
    om: ClienteOM, cat: Catalogo, termos_do_glossario: list[dict]
) -> dict[str, int]:
    """Tudo, na ordem em que uma etapa depende da outra.

    Classificação e domínio antes de quem os usa; produto antes da tabela que
    entra nele; termos antes das relações; tabela por último.
    """
    servico, banco = config.servico(), config.banco()
    gov = cat.governanca
    donos = Proprietarios(om, gov["dominios"])
    sincronizar_classificacoes(om, gov["dominios"], gov["formatos"])
    sincronizar_propriedades(om, gov["dominios"])
    sincronizar_dominios(om, donos, gov["dominios"])
    sincronizar_servicos(om, donos, gov["servicos"], servico, banco)
    sincronizar_produtos(om, donos, gov["dominios"])
    sincronizar_termos(om, donos, termos_do_glossario, gov["termos_minc"])
    catalogar_schemas(om, donos, cat, servico, banco)
    catalogar_ativos(om, donos, cat, servico, banco)
    return om.resumo()
