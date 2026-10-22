#!/usr/bin/env python3
"""
Documenta em dbt/minc/models/sources.yml os schemas que nao vem do SALIC:
transferegov, bbagil, bacen e ibge_sidra.

Diferente do SALIC, essas fontes vem de API (TransfereGov, SIDRA, SGS do Bacen)
e de extracao do BB -- nao existe dicionario de origem com `remarks` para
herdar descricao. Entao aqui so entra o que o catalogo do banco garante:
nome, tipo real, testes derivados do perfil e tag PII. A descricao de negocio
fica com o marcador [NAO VERIFICADO], que e o mesmo usado no resto do projeto.

O que o script faz, preservando o resto do arquivo intacto:
  1. insere o bloco `columns:` nas tabelas ja declaradas que estao sem ele
  2. acrescenta tabelas que existem no banco e nao estao declaradas
  3. cria a source do ibge_sidra, que nao existe
  4. marca como pendente de ingestao a tabela declarada que nao existe no banco

CUIDADO com o regex de PII em portugues: `raca` como substring casa dentro de
operacao, duracao, administracao, alteracao, liberacao, procuracao, geracao e
declaracao. Por isso o padrao exige fronteira de palavra.

Uso:
    python3 scripts/documentar_sources_apis.py --catalogo catalogo_full.csv
"""
import argparse
import collections
import csv
import re

ARQ = "dbt/minc/models/sources.yml"
SCHEMAS = ["transferegov", "bbagil", "bacen", "ibge_sidra", "ancine"]

DOMINIO = {
    "transferegov": "Cultura.Repasse Federativo",
    "bbagil": "Cultura.Repasse Federativo",
    "bacen": "Cultura.Indicadores",
    "ibge_sidra": "Cultura.Indicadores",
    "ancine": "Cultura.Audiovisual",
}
def cita(nome):
    """Cita o nome quando ele nao e um identificador simples.

    As tabelas do ancine vieram de planilha, e os nomes de coluna sao os
    cabecalhos do Excel: espaco, acento, parentese, barra e ate `%`. Sem
    aspas o YAML quebra, e no SQL cada uma precisa de aspas duplas.
    """
    import re as _re
    return nome if _re.fullmatch(r"[a-z_][a-z0-9_]*", nome) else f'"{nome}"'


DESCR_SOURCE = {
    "ancine": (
        "Captacoes e fontes de fomento do audiovisual, vindas da ANCINE. As duas "
        "tabelas foram carregadas a partir de planilha, e os nomes de coluna sao "
        "os cabecalhos do arquivo original -- com espaco, acento e pontuacao, o "
        "que obriga a citar cada um entre aspas no SQL. `consulta` e consumida "
        "por ancine/bronze_consulta.sql."
    ),
    "ibge_sidra": (
        "Agregados, localidades e metadados do SIDRA/IBGE, ingeridos pela DAG "
        "ibge_agregados_dag. Usados como referencia territorial e de indicador "
        "nos modelos de cotas e de perfil."
    ),
}

PII_SENSIVEL = re.compile(
    r"((^|[^a-z])raca([^a-z]|$)|cor_?raca|raca_?cor|etnia|religi|saude|"
    r"deficien|biometr|orientacaosexual)", re.I)
PII_COMUM = re.compile(
    r"(cpf|cnpj|\brg\b|identidade|nome|nasc|email|e_mail|mail|telefone|fone|"
    r"celular|cep|endereco|logradouro|bairro|senha|password|token|renda|"
    r"salario|passaporte|\bpis\b|\bnit\b)", re.I)

NV = "[NÃO VERIFICADO] Coluna sem descrição na origem (fonte de API, sem dicionário de dados)."


def dobrar(texto, largura=94, recuo=""):
    saida, linha = [], recuo
    for p in texto.split():
        if len(linha) + len(p) + 1 > largura and linha.strip():
            saida.append(linha.rstrip())
            linha = recuo + p + " "
        else:
            linha += p + " "
    if linha.strip():
        saida.append(linha.rstrip())
    return saida


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def bloco_colunas(cols, recuo=8):
    """Gera o bloco `columns:` de uma tabela."""
    i = " " * recuo
    L = [f"{i}columns:"]
    st = collections.Counter()
    for c in cols:
        nome = c["column_name"]
        L.append(f"{i}  - name: {cita(nome)}")
        L.append(f"{i}    description: >")
        L += dobrar(NV, 94, i + "      ")
        L.append(f"{i}    data_type: {c['data_type']}")

        tg = ("PII.Sensitive" if PII_SENSIVEL.search(nome)
              else ("PII.NonSensitive" if PII_COMUM.search(nome) else None))
        if tg:
            L.append(f"{i}    meta:")
            L.append(f"{i}      openmetadata:")
            L.append(f"{i}        tags:")
            L.append(f"{i}          - {tg}")
            st["pii"] += 1

        linhas = int(num(c["table_rows"]) or 0)
        nf, nd = num(c["null_frac"]), num(c["n_distinct"])
        ts = []
        if nf == 0.0 and linhas > 0:
            ts.append("not_null")
        if nd == -1.0 and linhas > 0:
            ts.append("unique")
        if ts:
            L.append(f"{i}    tests:")
            for t in ts:
                L.append(f"{i}      - {t}   # inferido do perfil do banco")
                st[t] += 1
        st["colunas"] += 1
    return L, st


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--catalogo", required=True)
    a = p.parse_args()

    lake = collections.defaultdict(lambda: collections.defaultdict(list))
    for r in csv.DictReader(open(a.catalogo, encoding="utf-8")):
        if r["schema_name"] in SCHEMAS:
            lake[r["schema_name"]][r["table_name"]].append(r)

    linhas = open(ARQ, encoding="utf-8").read().split("\n")
    saida, i = [], 0
    schema_atual = None
    declaradas = collections.defaultdict(set)
    tot = collections.Counter()
    n = len(linhas)

    while i < n:
        l = linhas[i]
        m_schema = re.match(r"^    schema: (\S+)", l)
        if m_schema:
            schema_atual = m_schema.group(1)
        m_tab = re.match(r"^      - name: (\S+)", l)

        # fim do bloco tables de um schema alvo -> acrescenta as que faltam
        if (schema_atual in SCHEMAS and re.match(r"^  - name: ", l)
                and declaradas[schema_atual]):
            faltam = sorted(set(lake[schema_atual]) - declaradas[schema_atual])
            for t in faltam:
                saida.append(f"      - name: {cita(t)}")
                saida.append("        description: >")
                saida += dobrar(
                    f"[NÃO VERIFICADO] Tabela presente em {schema_atual} e ainda sem "
                    f"descrição de negócio. {len(lake[schema_atual][t])} colunas.",
                    94, "          ")
                saida.append("        meta:")
                saida.append("          openmetadata:")
                saida.append(f"            domain: {DOMINIO[schema_atual]}")
                saida.append("            tier: Tier.Tier4")
                saida.append("            owner: minc-data-engineering")
                b, st = bloco_colunas(lake[schema_atual][t])
                saida += b
                tot.update(st)
                tot["tabelas_novas"] += 1
            declaradas[schema_atual] = set()

        if m_tab and schema_atual in SCHEMAS:
            tab = m_tab.group(1)
            declaradas[schema_atual].add(tab)
            # copia o bloco da tabela ate a proxima tabela/source
            bloco = [l]
            j = i + 1
            while j < n and not re.match(r"^      - name: |^  - name: |^sources:", linhas[j]):
                bloco.append(linhas[j])
                j += 1
            texto = "\n".join(bloco)
            if "columns:" not in texto:
                if tab in lake[schema_atual]:
                    b, st = bloco_colunas(lake[schema_atual][tab])
                    while bloco and not bloco[-1].strip():
                        bloco.pop()
                    bloco += b
                    tot.update(st)
                    tot["tabelas_preenchidas"] += 1
                else:
                    # declarada mas ausente do banco: dependencia antecipada
                    while bloco and not bloco[-1].strip():
                        bloco.pop()
                    bloco.append("        config:")
                    bloco.append("          tags:")
                    bloco.append("            - pendente_ingestao")
                    tot["pendentes"] += 1
            saida += bloco
            i = j
            continue

        saida.append(l)
        i += 1

    # ultimo schema do arquivo: nao ha proxima source para disparar o flush
    for esquema, decl in list(declaradas.items()):
        if not decl:
            continue
        for t in sorted(set(lake[esquema]) - decl):
            saida.append(f"      - name: {cita(t)}")
            saida.append("        description: >")
            saida += dobrar(
                f"[NÃO VERIFICADO] Tabela presente em {esquema} e ainda sem "
                f"descrição de negócio. {len(lake[esquema][t])} colunas.",
                94, "          ")
            saida.append("        meta:")
            saida.append("          openmetadata:")
            saida.append(f"            domain: {DOMINIO[esquema]}")
            saida.append("            tier: Tier.Tier4")
            saida.append("            owner: minc-data-engineering")
            b, st = bloco_colunas(lake[esquema][t])
            saida += b
            tot.update(st)
            tot["tabelas_novas"] += 1
        declaradas[esquema] = set()

    # schemas sem source nenhuma
    for s in SCHEMAS:
        if s in declaradas and declaradas[s]:
            continue
        if any(re.match(rf"^    schema: {s}$", x) for x in saida):
            continue
        while saida and not saida[-1].strip():
            saida.pop()
        saida.append(f"  - name: {s}")
        saida.append(f"    schema: {s}")
        saida.append("    description: >")
        saida += dobrar(DESCR_SOURCE.get(s, f"Schema {s}."), 94, "      ")
        saida.append("    tables:")
        for t in sorted(lake[s]):
            saida.append(f"      - name: {cita(t)}")
            saida.append("        description: >")
            saida += dobrar(
                f"[NÃO VERIFICADO] Tabela de {s} ainda sem descrição de negócio. "
                f"{len(lake[s][t])} colunas.", 94, "          ")
            saida.append("        meta:")
            saida.append("          openmetadata:")
            saida.append(f"            domain: {DOMINIO[s]}")
            saida.append("            tier: Tier.Tier4")
            saida.append("            owner: minc-data-engineering")
            b, st = bloco_colunas(lake[s][t])
            saida += b
            tot.update(st)
            tot["tabelas_novas"] += 1
        tot["sources_novas"] += 1

    open(ARQ, "w", encoding="utf-8").write("\n".join(saida) + "\n")
    print(f"tabelas preenchidas com colunas : {tot['tabelas_preenchidas']}")
    print(f"tabelas acrescentadas           : {tot['tabelas_novas']}")
    print(f"sources novas                   : {tot['sources_novas']}")
    print(f"marcadas pendente_ingestao      : {tot['pendentes']}")
    print(f"colunas documentadas            : {tot['colunas']}")
    print(f"tags PII                        : {tot['pii']}")
    print(f"tests not_null={tot['not_null']} unique={tot['unique']}")


if __name__ == "__main__":
    main()
