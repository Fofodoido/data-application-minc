"""Confere o que está NA INSTÂNCIA contra o que o repositório declara.

Os testes de `tests/test_governanca_openmetadata.py` respondem se a
declaração está completa. Esta auditoria responde se ela chegou ao catálogo —
pergunta diferente, e a distância entre as duas já foi grande: o glossário foi
corrigido no CSV e a instância seguiu com a versão antiga por meses, sem erro
em lugar nenhum.

O que ela pega e a conferência manual não pega de graça:

- campo que a API aceita com 200 e descarta em silêncio (a certificação
  enviada como etiqueta, por exemplo);
- ativo declarado no dbt que não chegou ao catálogo (falta `dbt run` ou
  ingestão), e ativo do catálogo que o dbt não declara mais;
- teste sem resultado — o sinal de que o `dbt test` não está chegando;
- coluna sem linhagem, métrica fora do Explorador de Ontologia, incidente
  aberto.

Só lê. Nunca escreve na instância.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from openmetadata.governanca import config
from openmetadata.governanca.catalogo import Catalogo
from openmetadata.governanca.cliente import ClienteOM
from openmetadata.governanca.governanca import CAMPOS_DA_TABELA
from openmetadata.governanca.metricas import nome_no_catalogo
from openmetadata.governanca.qualidade import casos_da_tabela

#: Critérios por ativo. O nome é o que aparece no relatório.
CRITERIOS = (
    "no_catalogo",
    "descricao",
    "dominio",
    "produto",
    "dono",
    "tier",
    "uso",
    "certificacao",
    "colunas_descritas",
    "formato_aplicado",
)


@dataclass
class Relatorio:
    por_produto: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    totais: Counter = field(default_factory=Counter)
    pendencias: list[str] = field(default_factory=list)

    def conferir(self, produto: str, criterio: str, ok: bool) -> None:
        self.por_produto[produto][f"{criterio}.total"] += 1
        self.por_produto[produto][f"{criterio}.ok"] += int(ok)

    def percentual(self, contador: Counter, criterio: str) -> str:
        total = contador[f"{criterio}.total"]
        return f"{100 * contador[f'{criterio}.ok'] / total:5.1f}%" if total else "    —"

    def markdown(self) -> str:
        cabecalho = "| produto | " + " | ".join(CRITERIOS) + " |"
        linhas = [cabecalho, "|" + "---|" * (len(CRITERIOS) + 1)]
        for produto, contador in sorted(self.por_produto.items()):
            linhas.append(
                f"| {produto} | "
                + " | ".join(self.percentual(contador, c) for c in CRITERIOS)
                + " |"
            )
        linhas.append("")
        for chave, valor in sorted(self.totais.items()):
            linhas.append(f"- **{chave}**: {valor}")
        if self.pendencias:
            linhas += ["", "### Pendências", ""]
            linhas += [f"- {p}" for p in self.pendencias[:200]]
            if len(self.pendencias) > 200:
                linhas.append(f"- … e mais {len(self.pendencias) - 200}")
        return "\n".join(linhas)


def _tem(entidade: dict, prefixo: str) -> bool:
    return any(
        str(t.get("tagFQN", "")).startswith(prefixo) for t in entidade.get("tags") or []
    )


def auditar_ativos(
    om: ClienteOM, cat: Catalogo, rel: Relatorio, servico: str, banco: str
) -> None:
    por_schema: dict[str, dict] = defaultdict(dict)
    for ativo in cat.ativos.values():
        por_schema[ativo.schema][ativo.nome] = ativo
    for schema, ativos in sorted(por_schema.items()):
        tabelas = {
            t["name"]: t
            for t in om.listar(
                "tables",
                {
                    "databaseSchema": f"{servico}.{banco}.{schema}",
                    "fields": CAMPOS_DA_TABELA,
                },
            )
        }
        for nome in sorted(set(tabelas) - set(ativos)):
            rel.totais["no catálogo sem declaração no dbt"] += 1
        for nome, ativo in ativos.items():
            produto = ativo.produto or "(fora de produto)"
            tabela = tabelas.get(nome)
            rel.conferir(produto, "no_catalogo", tabela is not None)
            if tabela is None:
                rel.pendencias.append(
                    f"{schema}.{nome}: declarado no dbt, ausente do catálogo"
                )
                continue
            certificacao = (
                (tabela.get("certification") or {}).get("tagLabel") or {}
            ).get("tagFQN")
            rel.conferir(produto, "descricao", bool(tabela.get("description")))
            rel.conferir(produto, "dominio", bool(tabela.get("domains")))
            rel.conferir(
                produto, "produto", bool(tabela.get("dataProducts")) or not ativo.produto
            )
            rel.conferir(produto, "dono", bool(tabela.get("owners")))
            rel.conferir(produto, "tier", _tem(tabela, "Tier."))
            rel.conferir(produto, "uso", _tem(tabela, "Uso."))
            rel.conferir(
                produto,
                "certificacao",
                certificacao == ativo.certificacao or not ativo.certificacao,
            )
            colunas = tabela.get("columns") or []
            if colunas:
                descritas = sum(bool(c.get("description")) for c in colunas)
                rel.conferir(produto, "colunas_descritas", descritas == len(colunas))
            com_formato = {c.nome for c in ativo.colunas if c.formato}
            if com_formato:
                aplicados = {
                    c["name"]
                    for c in colunas
                    if _tem(c, "FormatoMinC.") and c["name"] in com_formato
                }
                rel.conferir(produto, "formato_aplicado", aplicados == com_formato)


def auditar_linhagem(
    om: ClienteOM, cat: Catalogo, rel: Relatorio, servico: str, banco: str
) -> None:
    """Fração das colunas de silver/gold com linhagem de coluna até a origem."""
    for ativo in cat.ativos.values():
        if ativo.tipo != "model" or ativo.camada not in {
            "silver",
            "intermediate",
            "gold",
        }:
            continue
        grafo = om.obter(
            f"lineage/table/name/{ativo.fqn(servico, banco)}",
            {"upstreamDepth": 1, "downstreamDepth": 0},
        )
        if not grafo:
            continue
        cobertas = {
            coluna.split(".")[-1]
            for aresta in grafo.get("upstreamEdges") or []
            for linha in (
                (aresta.get("lineageDetails") or {}).get("columnsLineage") or []
            )
            for coluna in [linha.get("toColumn") or ""]
        }
        rel.totais["colunas silver/gold"] += len(ativo.colunas)
        rel.totais["colunas silver/gold com linhagem"] += len(
            {c.nome for c in ativo.colunas} & cobertas
        )
        if not grafo.get("upstreamEdges"):
            rel.pendencias.append(f"{ativo.schema}.{ativo.nome}: sem linhagem de origem")


def auditar_qualidade(
    om: ClienteOM, cat: Catalogo, rel: Relatorio, servico: str, banco: str
) -> None:
    """Teste sem resultado é o sinal de que a execução não chega ao catálogo."""
    agora = int(time.time() * 1000)
    for ativo in cat.ativos.values():
        if ativo.camada not in {"silver", "intermediate", "gold"}:
            continue
        for caso in casos_da_tabela(om, ativo.fqn(servico, banco)):
            rel.totais["casos de teste (silver/gold)"] += 1
            resultado = om.obter(
                f"dataQuality/testCases/{caso['id']}", {"fields": "testCaseResult"}
            )
            ultimo = (resultado or {}).get("testCaseResult") or {}
            if not ultimo:
                rel.totais["casos de teste sem resultado"] += 1
            elif agora - int(ultimo.get("timestamp") or 0) > 8 * 86_400_000:
                rel.totais["casos de teste com resultado de mais de 8 dias"] += 1
            if ultimo.get("testCaseStatus") == "Failed":
                rel.totais["casos de teste falhando"] += 1
    abertos = om.listar(
        "dataQuality/testCases/testCaseIncidentStatus",
        {"latest": "true", "startTs": agora - 30 * 86_400_000, "endTs": agora},
    )
    prefixo = f"{servico}.{banco}."
    for incidente in abertos:
        fqn_caso = (incidente.get("testCaseReference") or {}).get(
            "fullyQualifiedName"
        ) or ""
        if (
            fqn_caso.startswith(prefixo)
            and incidente.get("testCaseResolutionStatusType") != "Resolved"
        ):
            rel.totais["incidentes abertos (30 dias)"] += 1


def auditar_metricas(om: ClienteOM, cat: Catalogo, rel: Relatorio) -> None:
    """Métrica sem termo de glossário fica fora do Explorador de Ontologia."""
    for metrica in cat.metricas:
        nome = nome_no_catalogo(cat, metrica["name"])
        entidade = om.existe("metrics", nome, campos="tags")
        if not entidade:
            rel.pendencias.append(f"métrica {nome}: ausente do catálogo")
            continue
        rel.totais["métricas no catálogo"] += 1
        if not any(t.get("source") == "Glossary" for t in entidade.get("tags") or []):
            rel.pendencias.append(
                f"métrica {nome}: sem termo de glossário (isolada na ontologia)"
            )


def auditar(om: ClienteOM, cat: Catalogo) -> Relatorio:
    servico, banco = config.servico(), config.banco()
    rel = Relatorio()
    auditar_ativos(om, cat, rel, servico, banco)
    auditar_linhagem(om, cat, rel, servico, banco)
    auditar_qualidade(om, cat, rel, servico, banco)
    auditar_metricas(om, cat, rel)
    return rel
