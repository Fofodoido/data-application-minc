# Inventário de fontes e modelos ingeridos no OpenMetadata

Levantado em 2026-09-04 a partir de `dbt/minc/dbt_project.yml`, dos `.yml` de
`dbt/minc/models/` e das recipes em `helpers/openmetadata/recipes/`.

Todo o conteúdo abaixo entra no serviço **MinC** (Postgres, banco
`data_warehouse`): a recipe `postgres_metadata` cria as tabelas no catálogo e a
`dbt_metadata` anexa descrição, tags e linhagem por cima.

---

## Fontes declaradas — 18 sources, 653 tabelas

O que o projeto dbt declara em `sources.yml` e nos `sources_*.yml` do SALIC.
A coluna da direita conta quantas colunas daquela fonte têm descrição escrita —
a recipe ingere o schema inteiro de propósito, inclusive o que ainda não foi
documentado, para o catálogo mostrar o estado real.

| Source | Schema | Arquivo | Tabelas | Colunas documentadas |
|---|---|---|---:|---:|
| `bronze_sac` | `salic_bronze` | `salic_bronze/sources_sac.yml` | 475 | 1990 |
| `bronze_tabelas` | `salic_bronze` | `salic_bronze/sources_tabelas.yml` | 77 | 373 |
| `bronze_agentes` | `salic_bronze` | `salic_bronze/sources_agentes.yml` | 15 | 0 |
| `bronze_controledeacesso` | `salic_bronze` | `salic_bronze/sources_controledeacesso.yml` | 3 | 14 |
| `bronze_bdcorporativo` | `salic_bronze` | `salic_bronze/sources_bdcorporativo.yml` | 1 | 5 |
| `bronze_sac_legado` | `bronze` | `salic_bronze/sources_sac_legado.yml` | 3 | 118 |
| `dados_mapa_cultura` | `dados_mapa_cultura` | `sources.yml` | 29 | 0 |
| `dados_salic` | `dados_salic` | `sources.yml` | 19 | 0 |
| `transferegov` | `transferegov` | `sources.yml` | 7 | 161 |
| `relatorio_gestao` | `relatorio_gestao` | `sources.yml` | 6 | 0 |
| `bbagil` | `bbagil` | `sources.yml` | 5 | 86 |
| `ibge_sidra` | `ibge_sidra` | `sources.yml` | 5 | 25 |
| `ancine` | `ancine` | `sources.yml` | 2 | 32 |
| `bsc` | `bsc` | `sources.yml` | 2 | 0 |
| `execucao_pnab` | `execucao_pnab` | `sources.yml` | 2 | 0 |
| `bacen` | `bacen` | `sources.yml` | 1 | 5 |
| `bb_agil` | `bb_agil` | `sources.yml` | 1 | 0 |
| `bsc_pnab` | `bsc_pnab` | `sources.yml` | **0** | 0 |
| **Total** | 14 schemas | | **653** | **2809** |

### Pontos de atenção

- **Sem nenhuma coluna documentada:** `bronze_agentes` (15 tabelas),
  `dados_mapa_cultura` (29), `dados_salic` (19), `relatorio_gestao` (6),
  `bsc` (2), `execucao_pnab` (2) e `bb_agil` (1). Entram no catálogo como tabela
  nua — nome de coluna e tipo, sem explicação.
- **`bsc_pnab` está declarada com zero tabelas.** O schema entra no filtro das
  recipes, mas o dbt não sabe nada sobre ele.
- **`bronze_agentes` é a exceção estranha do SALIC:** as outras quatro fontes do
  SALIC estão documentadas coluna a coluna, essa não.

---

## Modelos dbt por camada — 625 modelos, 6839 colunas

Todos os 625 modelos e todas as 6839 colunas têm descrição — cobertura de 100%.
O schema de destino vem do `+schema` no `dbt_project.yml`; não há prefixo
`minc_` porque `macros/get_custom_schema.sql` usa `generate_schema_name_for_env`
e o target padrão é `prod`, que aplica o custom schema literal.

### Bronze — 587 modelos

| Domínio | Schema destino | Materialização | Modelos | Colunas |
|---|---|---|---:|---:|
| `salic_bronze/sac` | `salic` | view | 475 | 5518 |
| `salic_bronze/tabelas` | `salic` | view | 77 | 597 |
| `salic_bronze/agentes` | `salic` | view | 15 | 126 |
| `salic_bronze/controledeacesso` | `salic` | view | 3 | 17 |
| `salic_bronze/bdcorporativo` | `salic` | view | 1 | 6 |
| `transferegov_bronze` | `transferegov_bronze` | view | 6 | 161 |
| `ibge_sidra_bronze` | `ibge_sidra_bronze` | view | 5 | 25 |
| `bbagil_bronze` | `bbagil_bronze` | view | 4 | 86 |
| `bacen_bronze` | `bacen_bronze` | view | 1 | 5 |
| `cotas_dbt/bronze` | `cotas` | table | 9 | 90 |
| `agentes_dbt/bronze` | `agentes` | table | 3 | 15 |
| **Total** | | | **599** | **6646** |

Os 571 modelos do SALIC são um por tabela do raw `salic_bronze` — view por
padrão, porque o objetivo da camada é a semântica no dbt docs e no
OpenMetadata, e os 52,7 GB do raw não se duplicam antes de alguém saber quais
modelos importam.

Nomes dos modelos bronze fora do SALIC:

| Domínio | Modelos |
|---|---|
| `transferegov_bronze` | `anexos_relatorios`, `plano_acao_minc`, `plano_acao_meta_minc`, `plano_acao_dado_bancario_minc`, `programa_minc`, `relatorios_gestao` |
| `ibge_sidra_bronze` | `agregados_catalogo`, `localidades`, `metadados`, `periodos`, `pesquisas` |
| `bbagil_bronze` | `extrato_bbagil`, `subtransacao_bbagil`, `controle_extracao_bbagil_extrato`, `controle_extracao_bbagil_subtransacoes` |
| `bacen_bronze` | `serie_sgs` |
| `cotas_dbt/bronze` | `stg_agentes_pf`, `stg_agentes_pj`, `stg_agentes_coletivos`, `stg_agentes_pnab_pf`, `stg_agentes_pnab_pj`, `stg_bbagil`, `stg_contemplados_lpg`, `stg_contemplados_pnab`, `stg_editais` |
| `agentes_dbt/bronze` | `bbagil_extrato_filtrado`, `bbagil_subtransacoes_filtrado`, `identificadores_contemplados` |

O schema de destino dos bronzes de API repete o nome da pasta em vez do domínio
porque o raw já ocupa o nome limpo (`transferegov`) — `+schema: transferegov`
faria a view colidir com a tabela de origem. No SALIC é o inverso: o raw é
`salic_bronze` e sobra o nome limpo `salic`.

### Silver — 14 modelos

| Domínio | Schema | Materialização | Modelos | Colunas |
|---|---|---|---:|---:|
| `cotas_dbt/silver` | `cotas` | view | 9 | 48 |
| `agentes_dbt/silver` | `agentes` | table | 5 | 39 |

| Domínio | Modelos |
|---|---|
| `cotas_dbt/silver` | `bbagil_ente_ano`, `contemplados_unif`, `diag_valores_cortados`, `editais_unif`, `edital_ano_por_anexo`, `edital_ano_por_arquivo`, `identificadores_agentes_cotas`, `perfil_agentes_normalizado`, `territorio_municipio` |
| `agentes_dbt/silver` | `eventos_fomento`, `eventos_fomento_ancine`, `eventos_fomento_rouanet`, `pagamentos_bbagil_beneficiarios`, `primeiro_acesso_agentes` |

### Gold — 10 modelos

| Domínio | Schema | Materialização | Modelos | Colunas |
|---|---|---|---:|---:|
| `cotas_dbt/gold` | `cotas` | table | 6 | 67 |
| `agentes_dbt/gold` | `agentes` | table | 4 | 31 |

| Domínio | Modelos |
|---|---|
| `cotas_dbt/gold` | `cobertura_pagamentos`, `comparativo_recebido_vs_pago`, `distribuicao_cotas_lpg`, `distribuicao_cotas_pnab`, `fct_pagamentos_bbagil`, `fct_pagamentos_elegiveis` |
| `agentes_dbt/gold` | `conversao_direto_rouanet_coorte`, `primeiro_acesso_anual`, `primeiro_acesso_contemplados`, `trajetoria_fomento_direto_rouanet` |

`cotas_dbt` também carrega a camada semântica do MetricFlow —
`gold/metrics.yml`, `gold/semantic_models.yml` e `silver/semantic_models.yml`.

### Fora das camadas — 2 modelos

| Modelo | Schema | Materialização | Colunas |
|---|---|---|---:|
| `metadata/models_metadata` | `metadata` | incremental | 7 |
| `metricflow_time_spine` | padrão do target | — | 1 |

---

## Os dois domínios de negócio

| Domínio | Meta | Schema | Camadas |
|---|---|---|---|
| `cotas_dbt` | Meta 3 — cotas e reservas de LPG e PNAB | `cotas` | 9 bronze → 9 silver → 6 gold |
| `agentes_dbt` | Meta 5 — novos entrantes no fomento federal, cruzando LPG, PNAB, LAB (BB Ágil), Rouanet (SALIC) e audiovisual (Ancine) | `agentes` | 3 bronze → 5 silver → 4 gold |

---

## Como reproduzir esta contagem

```bash
python3 - <<'PY'
import glob, yaml
tot_m = tot_c = 0
for f in sorted(glob.glob('dbt/minc/models/**/*.yml', recursive=True)):
    d = yaml.safe_load(open(f)) or {}
    if not isinstance(d, dict):
        continue
    ms = d.get('models') or []
    cols = sum(len(m.get('columns') or []) for m in ms)
    if ms:
        print(f"{len(ms):>4} modelos {cols:>5} colunas  {f}")
    tot_m += len(ms)
    tot_c += cols
print(f"TOTAL: {tot_m} modelos, {tot_c} colunas")
PY
```
