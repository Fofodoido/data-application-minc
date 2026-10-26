# Registro de decisões de arquitetura (ADR)

Decisões técnicas que custaram discussão e que alguém, daqui a seis meses, vai
querer entender antes de desfazer. Cada uma registra o contexto, a decisão, o
motivo, o que foi descartado e o que ficou de consequência.

Um ADR não se edita depois de aceito — se a decisão mudar, escreve-se um novo
que a substitui, e o antigo passa a `Status: substituído por ADR NNNN`. Se o
novo substitui só uma parte, o antigo passa a `Status: aceito; partes
substituídas pelo ADR NNNN`, dizendo quais. A linha de status é a única
coisa que se muda num ADR aceito.

Um ADR novo entra com `Status: proposto`. Ele passa a `aceito` só quando o PR
for aprovado por alguém que não é o autor, e a troca do status vai no próprio
PR, antes do merge.

Mudar a regra do projeto dbt e mudar os modelos são dois PRs. Se um modelo
precisa de uma regra nova, o PR do ADR entra primeiro e o do modelo depois. O
CI recusa o PR que muda um ADR ou o `verificar.py` da skill
`arquitetura-lakehouse-minc` junto com `dbt/minc/models/`
(`scripts/checar_regra_e_codigo.py`). ADR de ingestão pode entrar junto com o
código da DAG.

| # | Decisão |
|---|---|
| [0001](0001-formularios-de-issue-em-yaml.md) | Formulários de issue em YAML, com o texto do data-application-cidades |
| [0002](0002-skills-versionadas-no-repositorio.md) | Skills versionadas no repositório, não instaladas por marketplace |
| [0003](0003-labels-por-eixo.md) | Vocabulário de labels organizado por eixo |
| [0004](0004-revisao-semanal-apura-antes-de-narrar.md) | A revisão semanal apura por script e narra por modelo |
| [0005](0005-ingestao-salic-por-trino-em-fatias.md) | A ingestão do SALIC passa a ser feita pelo Trino, em fatias de chave |
| [0006](0006-silvers-salic-gate-a0-parcial.md) | Gate A0 parcial das silvers do SALIC: medidas, território e bloqueios dos KPIs |
| [0007](0007-cosmos-monta-a-dag-por-dbt-ls.md) | O Cosmos monta a DAG por `dbt ls`, não por manifest versionado |
| [0008](0008-ingestao-mapas-so-public-sem-historico.md) | A ingestão do Mapas leva só o `public`, sem o histórico de edições nem o log de acesso |
| [0009](0009-data-lakehouse-extracao-airflow-camadas-minio-acesso-trino.md) | A plataforma de dados do MinC é um Data Lakehouse: extração pelo Airflow, camadas no MinIO e todo acesso via Trino |
| [0010](0010-camadas-e-schemas-do-data-lakehouse.md) | Cinco camadas, schema por fonte até a Intermediate e um schema por produto na Gold |
| [0011](0011-transferegov-grava-no-datalakehouse.md) | A ingestão do TransfereGov grava no datalakehouse, e uma ponte devolve ao Postgres |
| [0012](0012-fontes-estaticas-no-schema-seeds.md) | Fonte estática vai de raw a Parquet no MinIO e chega ao Postgres no schema `seeds` |
| [0014](0014-siconfi-grava-so-o-raw-e-controla-pelo-trino.md) | A ingestão do SICONFI grava só o raw no MinIO e controla a fila pelo Trino |
| [0015](0015-produtos-em-data-products-e-gold-lendo-gold.md) | Produtos em `data_products/`, Intermediate nomeada pelas fontes, Gold lendo Gold do mesmo produto e o que é regra de negócio (substitui partes do 0010) |

Para escrever um novo, existe a skill
[`architecture-decision-records`](../../.claude/skills/architecture-decision-records/).
