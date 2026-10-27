#!/usr/bin/env python3
"""Anota os schema.yml do dbt com domain, tier, owner e glossario.

Escrito como script, e nao a mao, por dois motivos: sao 672 itens, e a
correspondencia schema -> subdominio precisa ser a mesma regra em todos os
arquivos. Rodar de novo e idempotente: so escreve o que falta ou diverge.

    python scripts/anotar_openmetadata.py [--dry-run]
"""

import sys
from pathlib import Path

from ruamel.yaml import YAML

RAIZ = Path(__file__).resolve().parents[1]
MODELOS = RAIZ / "dbt/minc/models"

DONO = "minc-data-engineering"

FOMENTO = "Cultura.Fomento Direto"
INCENTIVO = "Cultura.Incentivo Fiscal"
FEDERATIVO = "Cultura.Repasse Federativo"
CADASTRO = "Cultura.Cadastro Cultural"
# Indicador externo (deflator do SGS/Bacen, catalogo do SIDRA/IBGE): nao e
# mecanismo de fomento. PROVISORIO: o subdominio Cultura.Indicadores ainda NAO
# existe no OpenMetadata. Esta aqui para o anotador nao reescrever bacen e
# ibge_sidra para Fomento Direto; o nome definitivo sai quando o subdominio
# for criado.
INDICADORES = "Cultura.Indicadores"

# Subdominio por schema de origem. O mecanismo de fomento e o criterio --
# ver dbt/README.md.
DOMINIO_POR_SCHEMA = {
    "bronze": INCENTIVO,
    "dados_salic": INCENTIVO,
    "agentes": FOMENTO,
    "cotas": FOMENTO,
    "relatorio_gestao": FOMENTO,
    "transferegov": FEDERATIVO,
    "bbagil": FEDERATIVO,
    "bb_agil": FEDERATIVO,
    "bsc": FEDERATIVO,
    "bsc_pnab": FEDERATIVO,
    "execucao_pnab": FEDERATIVO,
    # A captacao registrada pela Ancine e aporte de investidor, mesma
    # estrutura da captacao da Rouanet -- incentivo fiscal, nao repasse.
    "ancine": INCENTIVO,
    "dados_mapa_cultura": CADASTRO,
    "bacen": INDICADORES,
    "ibge_sidra": INDICADORES,
}

# Tier pela camada: quem pode depender daquilo.
TIER_POR_CAMADA = {
    "gold": "Tier.Tier1",
    "intermediate": "Tier.Tier2",
    "silver": "Tier.Tier2",
    "views": "Tier.Tier2",
    "bronze": "Tier.Tier3",
    "metadata": "Tier.Tier3",
}
TIER_SOURCE = "Tier.Tier4"

# Subdominio dos modelos dbt, pela pasta. Na estrutura do ADR 0010 a pasta de
# fonte segue o subdominio do schema de origem; intermediate e produto sao
# fomento, que e o que a Meta 5 mede.
DOMINIO_POR_PASTA = {
    "cotas_dbt": FOMENTO,
    "metadata": FOMENTO,
    "bbagil": FEDERATIVO,
    "salic": INCENTIVO,
    "ancine": INCENTIVO,
    "transferegov": FOMENTO,
    # mapas_dbt lê o Mapa da Cultura, que é cadastro (schema
    # dados_mapa_cultura), não mecanismo de fomento
    "mapas": CADASTRO,
    "bacen": INDICADORES,
    "ibge_sidra": INDICADORES,
    "intermediate": FOMENTO,
    "cultura_em_numeros": FOMENTO,
}

# Produtos de dados (Gold, ADR 0010) moram em `data_products/<produto>/`, e o
# modelo nao leva prefixo de camada. O produto e a subpasta: nao ha lista a
# manter em dia.
DATA_PRODUCTS = "data_products"

# Camada pelo prefixo do nome do modelo, na estrutura plana do ADR 0010.
PREFIXOS_CAMADA = {"bronze_": "bronze", "silver_": "silver", "int_": "intermediate"}

# Coluna -> termo de glossario. So conceito cuja ambiguidade muda a leitura.
GLOSSARIO_POR_COLUNA = {
    "identificador_unico": "MinC.Identificadores.IdentificadorUnico",
    "raca_normalizada": "MinC.Qualificadores.RacaNormalizada",
    "origem_ano": "MinC.Qualificadores.OrigemDoAno",
    "contemplado": "MinC.Agentes.Contemplado",
    "categoria_primeiro_acesso": "MinC.Agentes.PrimeiroAcesso",
    "perfil_classificacao": "MinC.Agentes.PrimeiroAcesso",
    "cd_mun": "MinC.Identificadores.CodigoIBGE",
    "municipio_codigo_ibge": "MinC.Identificadores.CodigoIBGE",
    "chave_municipio_uf": "MinC.Identificadores.CodigoIBGE",
    "em_concentracao_urbana": "MinC.Identificadores.FCU",
    "valor_pago": "MinC.Cotas.ValorPonderado",
}

yaml = YAML()
yaml.preserve_quotes = True
yaml.width = 4096
yaml.indent(mapping=2, sequence=4, offset=2)


def bloco_om(item, *, em_config=False, contagem=None):
    """Devolve meta.openmetadata do item, criando se preciso.

    `em_config=True` grava em `config.meta`, e nao no `meta` de topo. E
    obrigatorio em modelo: a partir do dbt 1.10 declarar `meta` nos dois
    lugares no mesmo modelo aborta o parse com "found meta dictionary in
    'config' dictionary and as top-level key" -- e todo modelo daqui ja tem
    `config.meta.status`. No manifest da no mesmo: `node.meta` e lido de
    `config.meta`, que e o que o conector dbt do OpenMetadata consome.

    Source continua no `meta` de topo, que e onde o dbt le em source/table.
    """
    if em_config:
        dono = item.setdefault("config", {})
        # Anotacao antiga, no `meta` de topo: migra em vez de duplicar.
        antigo = (item.get("meta") or {}).pop("openmetadata", None)
        if antigo is not None:
            dono.setdefault("meta", {}).setdefault("openmetadata", antigo)
            if not item["meta"]:
                del item["meta"]
            if contagem is not None:
                contagem["migrado_para_config"] += 1
    else:
        dono = item
    meta = dono.setdefault("meta", {})
    return meta.setdefault("openmetadata", {})


def anotar(item, *, dominio, tier, contagem, em_config=False):
    om = bloco_om(item, em_config=em_config, contagem=contagem)
    if om.get("domain") != dominio:
        om["domain"] = dominio
        contagem["domain"] += 1
    if tier and om.get("tier") != tier:
        om["tier"] = tier
        contagem["tier"] += 1
    if om.get("owner") != DONO:
        om["owner"] = DONO
        contagem["owner"] += 1

    for coluna in item.get("columns") or []:
        termo = GLOSSARIO_POR_COLUNA.get(coluna.get("name"))
        if not termo:
            continue
        om_col = bloco_om(coluna)
        if om_col.get("glossary") != [termo]:
            om_col["glossary"] = [termo]
            contagem["glossario_coluna"] += 1


def camada_do_caminho(caminho: Path) -> str | None:
    for parte in caminho.parts:
        if parte in TIER_POR_CAMADA:
            return parte
    return None


def camada_do_modelo(caminho: Path, nome: str) -> str | None:
    """Camada pela subpasta (layout antigo) ou, no layout plano do ADR 0010,
    pela pasta de produto ou pelo prefixo do nome do modelo."""
    camada = camada_do_caminho(caminho)
    if camada:
        return camada
    if caminho.parts[0] == DATA_PRODUCTS:
        return "gold"
    return next(
        (c for prefixo, c in PREFIXOS_CAMADA.items() if nome.startswith(prefixo)),
        None,
    )


def pasta_do_modelo(rel: Path) -> str | None:
    """Fonte, `intermediate` ou, dentro de data_products/, o produto."""
    if rel.parts[0] == DATA_PRODUCTS and len(rel.parts) > 2:
        return rel.parts[1]
    return rel.parts[0] if len(rel.parts) > 1 else None


def main() -> int:
    dry = "--dry-run" in sys.argv
    contagem = {
        "domain": 0,
        "tier": 0,
        "owner": 0,
        "glossario_coluna": 0,
        "migrado_para_config": 0,
    }
    tocados = []

    for caminho in sorted(MODELOS.rglob("*.yml")):
        with caminho.open(encoding="utf-8") as f:
            doc = yaml.load(f)
        if not doc:
            continue
        mudou_antes = dict(contagem)

        for source in doc.get("sources") or []:
            schema = source.get("schema", source.get("name"))
            dominio = DOMINIO_POR_SCHEMA.get(schema)
            if not dominio:
                print(f"   ! schema sem subdominio mapeado: {schema}")
                continue
            for tabela in source.get("tables") or []:
                anotar(tabela, dominio=dominio, tier=TIER_SOURCE, contagem=contagem)

        modelos = doc.get("models") or []
        if modelos:
            rel = caminho.relative_to(MODELOS)
            dominio = DOMINIO_POR_PASTA.get(pasta_do_modelo(rel), FOMENTO)
            for modelo in modelos:
                camada = camada_do_modelo(rel, modelo.get("name", ""))
                tier = TIER_POR_CAMADA.get(camada) if camada else None
                anotar(
                    modelo,
                    dominio=dominio,
                    tier=tier,
                    contagem=contagem,
                    em_config=True,
                )

        if contagem != mudou_antes:
            tocados.append(caminho)
            if not dry:
                with caminho.open("w", encoding="utf-8") as f:
                    yaml.dump(doc, f)

    rotulo = "seriam alterados" if dry else "alterados"
    print(f"\n{len(tocados)} arquivo(s) {rotulo}")
    for chave, valor in contagem.items():
        print(f"   {chave:<20} {valor}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
