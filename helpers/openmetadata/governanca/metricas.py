"""Métricas de negócio do MetricFlow publicadas como `Metric` no OpenMetadata.

A definição continua no projeto dbt (`metrics.yml` e `semantic_models.yml`);
daqui sai só a tradução: nome, rótulo, descrição, tipo, unidade,
granularidade, a expressão SQL equivalente e a tabela de origem — que vira
aresta de linhagem tabela -> métrica. O que o MetricFlow não sabe, o conceito
do glossário a que cada métrica responde, vem de `governance/metricas.yml`.

Por que publicar: a métrica é o que a pergunta de política pública cita
("a reserva racial foi cumprida?"). No catálogo ela fica ao lado do termo que
a define, no Explorador de Ontologia, e da tabela de onde sai, na linhagem.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from openmetadata.governanca import config
from openmetadata.governanca.catalogo import Catalogo
from openmetadata.governanca.cliente import (
    ClienteOM,
    Proprietarios,
    etiqueta,
    operacoes_de_diferenca,
    referencia,
    texto,
)
from openmetadata.governanca.governanca import etiquetas_do_produto

log = logging.getLogger(__name__)

TIPO_POR_AGREGACAO = {
    "sum": "SUM",
    "count": "COUNT",
    "count_distinct": "COUNT",
    "average": "AVERAGE",
    "min": "MIN",
    "max": "MAX",
    "median": "MEDIAN",
}
GRANULARIDADE = {
    "day": "DAY",
    "week": "WEEK",
    "month": "MONTH",
    "quarter": "QUARTER",
    "year": "YEAR",
}
AGREGACAO_SQL = {
    "sum": "SUM({})",
    "count": "COUNT({})",
    "count_distinct": "COUNT(DISTINCT {})",
    "average": "AVG({})",
    "min": "MIN({})",
    "max": "MAX({})",
}
DIMENSAO = re.compile(r"\{\{\s*Dimension\('(?:\w+__)?(\w+)'\)\s*\}\}")


class Traducao:
    """Lê o MetricFlow do manifest e traduz cada métrica."""

    def __init__(self, cat: Catalogo) -> None:
        self.metricas = {m["name"]: m for m in cat.metricas}
        self.medidas: dict[str, tuple[dict, dict]] = {}
        for sm in cat.semantic_models.values():
            for medida in sm.get("measures") or []:
                self.medidas[medida["name"]] = (sm, medida)
        self.gov = cat.governanca["metricas"]

    def modelo_semantico(self, metrica: dict) -> dict | None:
        """O modelo semântico de onde a métrica sai (pela primeira medida)."""
        medidas = (metrica.get("type_params") or {}).get("input_measures") or []
        if not medidas or medidas[0]["name"] not in self.medidas:
            return None
        return self.medidas[medidas[0]["name"]][0]

    def _coluna(self, sm: dict, dimensao: str) -> str:
        for d in sm.get("dimensions") or []:
            if d["name"] == dimensao:
                return str(d.get("expr") or d["name"])
        return dimensao

    def _filtro(self, sm: dict, metrica: dict) -> str:
        filtros = [
            f["where_sql_template"]
            for f in ((metrica.get("filter") or {}).get("where_filters") or [])
        ]
        return " AND ".join(
            DIMENSAO.sub(lambda m: self._coluna(sm, m.group(1)), f) for f in filtros
        )

    def expressao(self, metrica: dict) -> str | None:
        """SQL equivalente, para quem lê o catálogo sem conhecer MetricFlow."""
        params = metrica.get("type_params") or {}
        if metrica["type"] == "ratio":
            num, den = params["numerator"]["name"], params["denominator"]["name"]
            a, b = self.expressao(self.metricas[num]), self.expressao(self.metricas[den])
            return f"-- {num} / {den}\n({a})\n/ NULLIF(({b}), 0)" if a and b else None
        medida = (params.get("measure") or {}).get("name")
        if medida not in self.medidas:
            return None
        sm, decl = self.medidas[medida]
        coluna = decl.get("expr") or decl["name"]
        agregacao = AGREGACAO_SQL.get(decl["agg"], decl["agg"].upper() + "({})")
        relacao = sm["node_relation"]
        origem = f"{relacao['schema_name']}.{relacao['alias']}"
        sql = f"SELECT {agregacao.format(coluna)}\nFROM {origem}"
        filtro = self._filtro(sm, metrica)
        return f"{sql}\nWHERE {filtro}" if filtro else sql

    def tipo_e_unidade(self, metrica: dict) -> dict[str, Any]:
        if metrica["type"] == "ratio":
            unidade = self.gov.get("unidade_de_razao") or {}
            tipo = "RATIO"
        else:
            medida = ((metrica.get("type_params") or {}).get("measure") or {}).get("name")
            agg = self.medidas[medida][1]["agg"] if medida in self.medidas else ""
            unidade = (self.gov.get("unidade_por_medida") or {}).get(agg) or {}
            tipo = TIPO_POR_AGREGACAO.get(agg, "OTHER")
        corpo = {"metricType": tipo, "unitOfMeasurement": unidade.get("unidade")}
        if unidade.get("unidade_customizada"):
            corpo["customUnitOfMeasurement"] = unidade["unidade_customizada"]
        return corpo

    def granularidade(self, sm: dict | None) -> str | None:
        if not sm:
            return None
        tempo = (sm.get("defaults") or {}).get("agg_time_dimension")
        for d in sm.get("dimensions") or []:
            if d["name"] == tempo:
                grao = (d.get("type_params") or {}).get("time_granularity")
                return GRANULARIDADE.get(str(grao))
        return None


def nome_no_catalogo(cat: Catalogo, nome: str) -> str:
    return f"{cat.governanca['metricas'].get('prefixo_de_nome', '')}{nome}"


def sincronizar(om: ClienteOM, cat: Catalogo) -> dict[str, int]:
    """Métricas em dois passes, e a linhagem tabela -> métrica.

    `relatedMetrics` referencia outra métrica por id: a razão só pode apontar
    para o numerador e o denominador depois que os dois existem.
    """
    log.info("Métricas")
    servico, banco = config.servico(), config.banco()
    traducao = Traducao(cat)
    donos = Proprietarios(om, cat.governanca["dominios"])
    termos = cat.governanca["metricas"].get("termos_por_metrica") or {}
    por_modelo = {a.unique_id: a for a in cat.ativos.values()}
    produtos = {p["name"]: p for p in cat.governanca["dominios"].get("produtos") or []}

    tabelas: dict[str, dict | None] = {}
    for metrica in sorted(
        traducao.metricas.values(), key=lambda m: m["type"] != "simple"
    ):
        sm = traducao.modelo_semantico(metrica)
        ativo = por_modelo.get(((sm or {}).get("depends_on") or {}).get("nodes", [""])[0])
        produto = produtos.get(ativo.produto or "") if ativo else None
        desejado: dict[str, Any] = {
            "displayName": metrica.get("label") or metrica["name"],
            "description": texto(metrica.get("description")),
            "metricExpression": (
                {"language": "SQL", "code": codigo}
                if (codigo := traducao.expressao(metrica))
                else None
            ),
            **traducao.tipo_e_unidade(metrica),
            "granularity": traducao.granularidade(sm),
            "owners": donos.resolver((produto or {}).get("owner_key")),
            "tags": [
                etiqueta(t)
                for t in etiquetas_do_produto(
                    cat.governanca["dominios"], (ativo.produto if ativo else None) or ""
                )
            ]
            + [etiqueta(t, "Glossary") for t in termos.get(metrica["name"], [])],
        }
        if produto:
            dominio = om.existe("domains", produto["domain"])
            if dominio:
                desejado["domains"] = [referencia(dominio, "domain")]
        nome = nome_no_catalogo(cat, metrica["name"])
        atual = om.existe("metrics", nome, campos="owners,tags,domains,relatedMetrics")
        if atual:
            om.patch(
                "metrics",
                atual["id"],
                operacoes_de_diferenca(atual, desejado),
                f"métrica {nome}",
            )
        else:
            om.criar(
                "metrics",
                {
                    "name": nome,
                    **{k: v for k, v in desejado.items() if v and k != "domains"},
                    **({"domains": [produto["domain"]]} if produto else {}),
                },
                nome,
            )
            om.esquecer("metrics", nome)
        if ativo:
            tabelas[nome] = om.existe("tables", ativo.fqn(servico, banco))

    for metrica in traducao.metricas.values():
        nome = nome_no_catalogo(cat, metrica["name"])
        _relacionar(om, cat, metrica, nome)
        _ligar_tabela(om, nome, tabelas.get(nome))
    return om.resumo()


def _relacionar(om: ClienteOM, cat: Catalogo, metrica: dict, nome: str) -> None:
    """Razão aponta para numerador e denominador."""
    params = metrica.get("type_params") or {}
    partes = [
        params.get(p, {}) and params[p]["name"] for p in ("numerator", "denominator")
    ]
    partes = [p for p in partes if p]
    if not partes:
        return
    atual = om.existe("metrics", nome, campos="relatedMetrics")
    if not atual:
        if om.offline:
            om._registrar(f"relacionaria {nome} a {', '.join(partes)}")
        return
    refs = [
        referencia(m, "metric")
        for m in (om.existe("metrics", nome_no_catalogo(cat, p)) for p in partes)
        if m
    ]
    om.patch(
        "metrics",
        atual["id"],
        operacoes_de_diferenca(atual, {"relatedMetrics": refs}),
        f"relações de {nome}",
    )


def _ligar_tabela(om: ClienteOM, nome: str, tabela: dict | None) -> None:
    """Aresta de linhagem da tabela de origem até a métrica."""
    metrica = om.existe("metrics", nome)
    if not (metrica and tabela):
        if om.offline:
            om._registrar(f"ligaria a tabela de origem a {nome}")
        return
    linhagem = om.obter(
        f"lineage/metric/{metrica['id']}", {"upstreamDepth": 1, "downstreamDepth": 0}
    )
    if tabela["id"] in {n["id"] for n in (linhagem or {}).get("nodes") or []}:
        om.conformes += 1
        return
    om.enviar(
        "PUT",
        "lineage",
        {
            "edge": {
                "fromEntity": {"id": tabela["id"], "type": "table"},
                "toEntity": {"id": metrica["id"], "type": "metric"},
                "description": "Métrica do MetricFlow calculada a partir desta tabela.",
            }
        },
        f"ligaria {tabela['fullyQualifiedName']} -> {nome}",
    )
