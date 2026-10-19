"""Qualidade e observabilidade: testes nativos, suítes, contratos e incidentes.

Declarado em `dbt/minc/governance/qualidade.yml`, `formatos.yml` e
`contratos.yml`. Três entregas:

- **testes nativos** — o que o dbt não expressa bem: o formato declarado de
  cada coluna (regex), a tabela de consumo que esvaziou e a coluna que entrou
  ou saiu sem o `schema.yml` acompanhar. Ficam na suíte lógica de execução, que
  a recipe `postgres_testsuite` roda;
- **suítes por produto** — todos os testes (dbt e nativos) das tabelas de um
  produto, para a aba Data Quality mostrar a saúde por produto;
- **contratos** — schema, testes e termos de uso de cada tabela Gold.

Os incidentes abrem sozinhos quando um teste falha. O que se faz aqui é dar a
cada um a severidade do tier da tabela, para o Incident Manager ordenar pelo
que chega a painel.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable

from openmetadata.governanca import config
from openmetadata.governanca.catalogo import Ativo, Catalogo
from openmetadata.governanca.cliente import (
    ClienteOM,
    Proprietarios,
    operacoes_de_diferenca,
    referencia,
    texto,
)
from openmetadata.governanca.governanca import dono_do_ativo

log = logging.getLogger(__name__)

#: Tipos de coluna em que um teste de regex faz sentido. Regex sobre inteiro
#: obrigaria o banco a converter a coluna inteira em texto a cada execução.
TIPOS_TEXTUAIS = {"text", "varchar", "character varying", "char", "character", "bpchar"}

COLUNAS_DOCUMENTADAS = "@colunas_documentadas"


def link_da_tabela(fqn: str, coluna: str | None = None) -> str:
    """O `entityLink` do caso de teste: a tabela, ou uma coluna dela."""
    return f"<#E::table::{fqn}::columns::{coluna}>" if coluna else f"<#E::table::{fqn}>"


# ── testes nativos ───────────────────────────────────────────────────────
def _parametros(parametros: dict, ativo: Ativo) -> list[dict]:
    valores = []
    for nome, valor in (parametros or {}).items():
        if valor == COLUNAS_DOCUMENTADAS:
            valor = ",".join(c.nome for c in ativo.colunas)
        valores.append({"name": nome, "value": str(valor)})
    return valores


def testes_nativos(cat: Catalogo) -> list[tuple[Ativo, dict]]:
    """Os casos de teste nativos declarados, já resolvidos por ativo.

    Ativo da camada certa sem coluna documentada não recebe o teste de
    colunas: a lista vazia faria o teste reprovar sempre.
    """
    qualidade = cat.governanca["qualidade"]
    formatos = {f["name"]: f for f in cat.governanca["formatos"].get("formatos") or []}
    camadas_de_formato = set(
        (qualidade.get("testes_de_formato") or {}).get("camadas") or []
    )
    definicao_de_formato = (qualidade.get("testes_de_formato") or {}).get(
        "definicao", "columnValuesToMatchRegex"
    )
    por_camada = qualidade.get("testes_por_camada") or {}
    declarados = {
        d["tabela"]: d["testes"] for d in qualidade.get("testes_declarados") or []
    }
    resultado: list[tuple[Ativo, dict]] = []
    for ativo in sorted(cat.ativos.values(), key=lambda a: a.unique_id):
        if ativo.tipo != "model":
            continue
        for teste in por_camada.get(ativo.camada or "", []):
            if COLUNAS_DOCUMENTADAS in (teste.get("parametros") or {}).values() and not (
                ativo.colunas
            ):
                continue
            resultado.append((ativo, teste))
        if ativo.camada in camadas_de_formato:
            for coluna in ativo.colunas:
                regex = (formatos.get(coluna.formato or "") or {}).get("regex")
                if regex and coluna.tipo.lower() in TIPOS_TEXTUAIS:
                    resultado.append(
                        (
                            ativo,
                            {
                                "name": f"minc_formato_{coluna.nome}",
                                "definicao": definicao_de_formato,
                                "coluna": coluna.nome,
                                "parametros": {"regex": regex},
                                "description": (
                                    f"Os valores de `{coluna.nome}` seguem o formato "
                                    f"declarado FormatoMinC.{coluna.formato}."
                                ),
                            },
                        )
                    )
        for teste in declarados.get(ativo.nome, []):
            resultado.append((ativo, teste))
    return resultado


def sincronizar_testes_nativos(
    om: ClienteOM, donos: Proprietarios, cat: Catalogo, servico: str, banco: str
) -> list[dict]:
    """Cria ou atualiza cada caso de teste nativo. Devolve os que existem."""
    log.info("Testes nativos")
    existentes = []
    for ativo, teste in testes_nativos(cat):
        fqn_tabela = ativo.fqn(servico, banco)
        if not om.offline and not om.existe("tables", fqn_tabela):
            continue  # tabela ainda não materializada: o teste viria órfão
        coluna = teste.get("coluna")
        fqn = (
            f"{fqn_tabela}.{coluna}.{teste['name']}"
            if coluna
            else f"{fqn_tabela}.{teste['name']}"
        )
        corpo = {
            "name": teste["name"],
            "description": texto(teste.get("description")),
            "testDefinition": teste["definicao"],
            "entityLink": link_da_tabela(fqn_tabela, coluna),
            "parameterValues": _parametros(teste.get("parametros") or {}, ativo),
            "owners": donos.resolver(dono_do_ativo(cat.governanca["dominios"], ativo)),
        }
        corpo = {k: v for k, v in corpo.items() if v is not None}
        atual = om.existe("dataQuality/testCases", fqn, campos="owners")
        if atual:
            om.patch(
                "dataQuality/testCases",
                atual["id"],
                _diferenca_do_teste(atual, corpo),
                f"teste {fqn}",
            )
            existentes.append(atual)
            continue
        criado = om.criar("dataQuality/testCases", corpo, fqn)
        if criado:
            existentes.append(criado)
    return existentes


def _diferenca_do_teste(atual: dict, corpo: dict) -> list[dict]:
    """Descrição, dono e parâmetros que mudaram.

    Parâmetro não tem `id` nem `fullyQualifiedName`: a comparação genérica o
    reduziria ao `name`, e trocar o regex passaria despercebido. Por isso o par
    nome-valor é comparado aqui.
    """
    operacoes = operacoes_de_diferenca(
        atual, {k: corpo.get(k) for k in ("description", "owners")}
    )

    def pares(itens: Iterable[dict]) -> set[tuple[str, str]]:
        return {(p["name"], str(p.get("value"))) for p in itens or []}

    if pares(atual.get("parameterValues") or []) != pares(
        corpo.get("parameterValues") or []
    ):
        operacoes.append(
            {"op": "add", "path": "/parameterValues", "value": corpo["parameterValues"]}
        )
    return operacoes


# ── suítes lógicas ───────────────────────────────────────────────────────
def _suite(
    om: ClienteOM, donos: Proprietarios, nome: str, decl: dict, rotulo: str
) -> dict | None:
    atual = om.existe("dataQuality/testSuites", nome, campos="owners,tests")
    desejado = {
        "displayName": decl.get("display_name") or nome,
        "description": texto(decl.get("description")),
        "owners": donos.resolver(decl.get("owner_key", "minc_data_engineering")),
    }
    if atual:
        om.patch(
            "dataQuality/testSuites",
            atual["id"],
            operacoes_de_diferenca(atual, desejado),
            rotulo,
        )
        return atual
    om.criar(
        "dataQuality/testSuites",
        {"name": nome, **{k: v for k, v in desejado.items() if v}},
        rotulo,
    )
    om.esquecer("dataQuality/testSuites", nome)
    return om.existe("dataQuality/testSuites", nome, campos="owners,tests")


def _incluir(om: ClienteOM, suite: dict | None, casos: list[dict], rotulo: str) -> None:
    """Acrescenta à suíte lógica os casos que ainda não estão nela."""
    if not suite:
        if casos:
            om._registrar(f"incluiria {len(casos)} casos de teste em {rotulo}")
        return
    presentes = {t["id"] for t in suite.get("tests") or []}
    faltando = sorted({c["id"] for c in casos} - presentes)
    if not faltando:
        om.conformes += 1
        return
    om.enviar(
        "PUT",
        "dataQuality/testCases/logicalTestCases",
        {"testSuiteId": suite["id"], "testCaseIds": faltando},
        f"incluiria {len(faltando)} casos de teste em {rotulo}",
    )


def casos_da_tabela(om: ClienteOM, fqn: str) -> list[dict]:
    """Todos os casos de teste de uma tabela, de dbt e nativos, colunas inclusive."""
    return list(
        om.listar(
            "dataQuality/testCases",
            {"entityLink": link_da_tabela(fqn), "includeAllTests": "true"},
        )
    )


def sincronizar_suites(
    om: ClienteOM,
    donos: Proprietarios,
    cat: Catalogo,
    nativos: list[dict],
    servico: str,
    banco: str,
) -> None:
    """A suíte que executa (só nativos) e uma suíte de observação por produto."""
    log.info("Suítes de teste")
    qualidade = cat.governanca["qualidade"]
    execucao = qualidade["suite_de_execucao"]
    suite = _suite(om, donos, execucao["name"], execucao, f"suíte {execucao['name']}")
    _incluir(om, suite, nativos, f"suíte {execucao['name']}")

    por_produto = qualidade.get("suites_por_produto") or {}
    camadas = set(por_produto.get("camadas") or ["silver", "intermediate", "gold"])
    for produto in cat.governanca["dominios"].get("produtos") or []:
        nome = f"{por_produto.get('prefixo', 'minc_')}{produto['name']}"
        decl = {
            "display_name": f"MinC — {produto.get('display_name', produto['name'])}",
            "description": por_produto.get("description"),
            "owner_key": produto.get("owner_key"),
        }
        suite = _suite(om, donos, nome, decl, f"suíte {nome}")
        casos: list[dict] = []
        for ativo in cat.do_produto(produto["name"]):
            if ativo.camada in camadas and not om.offline:
                casos += casos_da_tabela(om, ativo.fqn(servico, banco))
        _incluir(om, suite, casos, f"suíte {nome}")


# ── contratos ────────────────────────────────────────────────────────────
def sincronizar_contratos(
    om: ClienteOM, donos: Proprietarios, cat: Catalogo, servico: str, banco: str
) -> None:
    """Um contrato por tabela Gold: schema e testes do catálogo, termos do YAML.

    O schema e a lista de testes saem da tabela como ela está no catálogo, para
    o contrato nunca descrever colunas que a tabela já não tem. O status só é
    escrito na criação (`Draft`): aprovar é decisão de quem responde pelo
    produto, e o sync não desfaz a aprovação.
    """
    log.info("Contratos de dados")
    contratos = cat.governanca["contratos"]
    padrao = contratos.get("padrao") or {}
    termos_de_uso = contratos.get("termos_de_uso") or {}
    for ativo in sorted(cat.ativos.values(), key=lambda a: a.nome):
        if ativo.camada not in set(padrao.get("camadas") or ["gold"]):
            continue
        fqn = ativo.fqn(servico, banco)
        tabela = om.existe("tables", fqn, campos="columns")
        if not tabela:
            if om.offline:
                om._registrar(f"contrataria {fqn}")
            continue
        casos = casos_da_tabela(om, fqn)
        uso = texto(termos_de_uso.get(ativo.produto or ""))
        desejado: dict[str, Any] = {
            "description": texto(ativo.descricao),
            "schema": [
                {"name": c["name"], "dataType": c["dataType"]}
                for c in tabela.get("columns") or []
            ],
            "qualityExpectations": [referencia(c, "testCase") for c in casos],
            # A instância guarda os termos como objeto `{content}`. Texto puro é
            # aceito e convertido, mas a comparação nunca bateria e o contrato
            # seria reescrito a cada execução.
            "termsOfUse": {"content": uso} if uso else None,
            "owners": donos.resolver(padrao.get("owner_key")),
        }
        nome = f"minc_contrato_{ativo.nome}"
        atual = om.obter(
            "dataContracts/entity",
            {"entityId": tabela["id"], "entityType": "table", "fields": "owners"},
        )
        if atual and atual.get("id"):
            om.patch(
                "dataContracts",
                atual["id"],
                _diferenca_do_contrato(atual, desejado),
                f"contrato de {ativo.nome}",
            )
            continue
        om.criar(
            "dataContracts",
            {
                "name": nome,
                "entity": referencia(tabela, "table"),
                "entityStatus": padrao.get("status", "Draft"),
                **{k: v for k, v in desejado.items() if v},
            },
            f"contrato de {ativo.nome}",
        )


def _diferenca_do_contrato(atual: dict, desejado: dict) -> list[dict]:
    """O schema do contrato não tem `id`: compara por nome e tipo."""
    schema = desejado.pop("schema")
    operacoes = operacoes_de_diferenca(atual, desejado)
    if {(c["name"], c["dataType"]) for c in atual.get("schema") or []} != {
        (c["name"], c["dataType"]) for c in schema
    }:
        operacoes.append({"op": "add", "path": "/schema", "value": schema})
    return operacoes


# ── incidentes ───────────────────────────────────────────────────────────
def classificar_incidentes(
    om: ClienteOM, cat: Catalogo, servico: str, banco: str, dias: int = 30
) -> None:
    """Severidade do tier da tabela em cada incidente aberto sem severidade.

    O OpenMetadata abre o incidente sozinho na falha, sem severidade. Sem ela,
    o Incident Manager mistura a gold que alimenta painel com a bronze que
    ninguém consome.
    """
    log.info("Incidentes")
    severidade = (cat.governanca["qualidade"].get("incidentes") or {}).get(
        "severidade_por_tier"
    ) or {}
    if om.offline:
        om._registrar("classificaria os incidentes abertos pelo tier da tabela")
        return
    tier_por_fqn = {a.fqn(servico, banco): a.tier for a in cat.ativos.values()}
    fim = int(time.time() * 1000)
    abertos = om.listar(
        "dataQuality/testCases/testCaseIncidentStatus",
        {
            "latest": "true",
            "startTs": fim - dias * 86_400_000,
            "endTs": fim,
            "testCaseResolutionStatusType": "New",
            "fields": "testCaseReference",
        },
    )
    prefixo = f"{servico}.{banco}."
    for incidente in abertos:
        caso = (incidente.get("testCaseReference") or {}).get("fullyQualifiedName") or ""
        if not caso.startswith(prefixo) or incidente.get("severity"):
            continue
        tabela = ".".join(caso.split(".")[:4])
        alvo = severidade.get(str(tier_por_fqn.get(tabela) or "").removeprefix("Tier."))
        if not alvo:
            continue
        om.patch(
            "dataQuality/testCases/testCaseIncidentStatus",
            incidente["id"],
            [{"op": "add", "path": "/severity", "value": alvo}],
            f"severidade do incidente de {caso}",
        )


def sincronizar(om: ClienteOM, cat: Catalogo) -> dict[str, int]:
    servico, banco = config.servico(), config.banco()
    donos = Proprietarios(om, cat.governanca["dominios"])
    nativos = sincronizar_testes_nativos(om, donos, cat, servico, banco)
    sincronizar_suites(om, donos, cat, nativos, servico, banco)
    sincronizar_contratos(om, donos, cat, servico, banco)
    return om.resumo()
