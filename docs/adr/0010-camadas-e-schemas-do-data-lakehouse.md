# ADR 0010 — Cinco camadas, schema por fonte até a Intermediate e um schema por produto na Gold

- **Data:** 2026-10-06
- **Status:** aceito; partes substituídas pelo
  [ADR 0015](0015-produtos-em-data-products-e-gold-lendo-gold.md): a pasta
  dos produtos, o nome da Intermediate, a definição de regra de negócio
  (regra 3) e o que a Gold pode ler (regra 5). Onde os dois divergem, vale o
  0015.
- **Escopo:** camadas, schemas e nomenclatura do projeto dbt e das DAGs
- **Desenho:** [`docs/arquitetura/modelagem-dados-minc.png`](../arquitetura/modelagem-dados-minc.png)
- **Complementa:** [ADR 0009](0009-data-lakehouse-extracao-airflow-camadas-minio-acesso-trino.md)
- **Substitui:** a organização "medalhão por domínio" descrita em
  `docs/documentos/modelo-conceitual-logico-fisico.pdf`, cap. 02

## Contexto

O projeto dbt de hoje é organizado **por domínio de meta**: `cotas_dbt` (Meta
3), `agentes_dbt` (Meta 5), `salic_dbt` (Rouanet), `mapas_dbt`, mais quatro
pastas `*_bronze` para as fontes de API. Cada domínio tem a própria ideia de
camada: a "bronze" do `cotas_dbt` filtra e tipa (`stg_*`), a do `agentes_dbt`
aplica oito filtros de negócio e faz join (`*_filtrado`), a do `salic_dbt`
só tipa, e a do `mapas_dbt` não existe como modelo — é `source`. A silver do
`agentes_dbt` cruza BB Ágil, SALIC e Ancine; a do `salic_dbt` não cruza nada.

A consequência prática: a mesma pergunta — "em que camada entra esta
transformação?" — tem resposta diferente em cada pasta, e a mesma regra (a
normalização de CPF/CNPJ, por exemplo) está escrita de duas formas
incompatíveis em dois domínios. Três ferramentas que leem o projeto (a
governança do SALIC, o coletor do site de documentação e os geradores de
modelo) codificam a convenção de pastas por conta própria e discordam entre si.

Dos 712 modelos `.sql` do projeto, **nenhum** segue a nomenclatura deste ADR.
A migração é o custo desta decisão e está nas consequências.

O desenho aprovado em outubro de 2026 está em
`docs/arquitetura/modelagem-dados-minc.png`.

## Decisão

### As cinco camadas

| | Camada | O que faz | O que **não** faz | Organização | Consumo |
|---|---|---|---|---|---|
| 0 | **Raw** | Guarda o dado como extraído da fonte pelas DAGs do Airflow. Base para reprocessar e auditar | Não é sobrescrita. Não é modelo dbt: entra no projeto como `source` | Arquivos no MinIO, em `raw/<fonte>/<entidade>/` | Nenhum |
| 1 | **Bronze** | Carga do `raw` via Trino. Cópia fiel, com histórico permanente e metadados de ingestão | Não tipa, não agrega, não remove registro, não aplica regra de negócio | Schema por fonte | Nenhum |
| 2 | **Silver** | Limpa, tipa, deduplica e normaliza | Não cruza fontes. Não aplica regra de negócio final | Schema por fonte | ML, IA, RAG e análise exploratória, via Trino |
| 3 | **Intermediate** | Entre a Silver geral e a Gold: cruza bases e concentra transformações reutilizáveis. Melhora o desempenho da Gold e evita SQL duplicado entre produtos | Não aplica regra de negócio final | Schema único `intermediate` | Nenhum |
| 4 | **Gold** | Regra de negócio final: fatos, dimensões e indicadores | Não muda de forma incompatível sem versionamento | Um schema por produto de dados | Consumo oficial: indicadores, dashboards, APIs, BI |

Raw, Bronze, Silver e Intermediate são comuns a todos os produtos. Só a Gold é
separada por produto.

### Schemas e nomenclatura

| Camada | Schema | Tabela | Arquivo dbt | Exemplo |
|---|---|---|---|---|
| Raw | — | — | — (`source`) | `raw/salic/agentes/` |
| Bronze | `<fonte>` | `bronze_<entidade>` | `<fonte>/bronze_<entidade>.sql` | `salic.bronze_agentes_agentes` |
| Silver | `<fonte>` | `silver_<entidade>` | `<fonte>/silver_<entidade>.sql` | `salic.silver_agentes_agentes` |
| Intermediate | `intermediate` | `int_<fonte1>_<fonte2>` | `intermediate/int_<fonte1>_<fonte2>.sql` | `intermediate.int_salic_transferegov` |
| Gold | `<produto>` | `<nome>`, sem prefixo | `<produto>/<nome>.sql` | `cultura_em_numeros.eixo2_meta3_fct_pagamento_profissional_rouanet` |

- **O nome do arquivo é o nome da tabela.** A fonte fica na pasta e no
  schema, não no nome. Não há `alias`: `bronze_agentes_agentes.sql` vira
  `salic.bronze_agentes_agentes`.
- **A entidade preserva o que identifica a tabela na origem.** No SALIC a
  origem são cinco bancos, então a entidade é `<banco>_<tabela>`:
  `agentes_agentes`, `sac_aberturadecontabancaria`. No BB Ágil a entidade é
  o nome da tabela extraída: `controle_extracao_bbagil_extrato`.
- Fonte com uma única entidade usa o nome da fonte como entidade:
  `transferegov/bronze_transferegov.sql`.
- **A Gold não leva prefixo de camada nem de produto**: a pasta e o schema já
  dizem o produto. **Dentro da pasta, cada produto nomeia suas tabelas do
  jeito que o produto precisa.** O Cultura em Números leva eixo e meta no
  nome (`eixo2_meta3_fct_pagamento_profissional_rouanet`) porque é assim que
  seus consumidores procuram um indicador; isso é convenção desse produto,
  não regra da camada. Outro produto define a sua e a registra no `schema.yml`
  da pasta.
- Produtos de dados até agora: `cultura_em_numeros` (Cultura em Números).
- O dbt exige nome de modelo único no projeto inteiro. Com o prefixo da
  camada e a entidade no nome, duas fontes só colidem se tiverem uma entidade
  de mesmo nome. Quando acontecer, a entidade da fonte que chegou depois
  leva a fonte no nome (`bronze_ibge_localidades`), e esse caso é a exceção
  registrada no `schema.yml`.

### Pastas no projeto dbt

O desenho fixa schema e tabela, não pasta. **Uma pasta por fonte, uma por
produto, e uma para a Intermediate**, sem subpasta de camada: a camada está
no prefixo do arquivo. Cada pasta tem o próprio `+schema`.

```text
dbt/minc/models/
├── salic/
│   ├── bronze_agentes_agentes.sql
│   ├── bronze_sac_aberturadecontabancaria.sql
│   ├── silver_agentes_agentes.sql
│   └── silver_sac_aberturadecontabancaria.sql
├── bbagil/
│   ├── bronze_controle_extracao_bbagil_extrato.sql
│   └── silver_controle_extracao_bbagil_extrato.sql
├── intermediate/
│   └── int_salic_transferegov.sql
└── cultura_em_numeros/
    └── eixo2_meta3_fct_pagamento_profissional_rouanet.sql
```

Pasta de fonte contém só `bronze_*` e `silver_*`. Pasta de produto contém
só Gold. A pasta `intermediate` contém só `int_*`. Um arquivo fora dessa
regra está na pasta errada.

### Regras transversais

1. **Valor ausente fica nulo e nunca vira falso**, em todas as camadas.
2. **Cruzamento entre fontes só na Intermediate.** Silver que faz `ref()` a
   outra fonte está na camada errada.
3. **Regra de negócio final só na Gold.** Filtro que define "quem conta como
   contemplado" não entra em Silver nem em Intermediate.
4. **Bronze lê exatamente uma `source` e nada mais.** Sem `join`, sem
   `where`, sem cast.
5. **Gold referencia Intermediate ou Silver.** Nunca Bronze, nunca `source`.
6. **Dimensão comum** (tempo, território) é Intermediate, para ser
   compartilhada entre produtos. A lista completa e o mecanismo de
   compartilhamento ficam em aberto.

### Exemplo de ponta a ponta

```text
raw/salic/agentes/              ─▶ salic.bronze_agentes_agentes     ─▶ salic.silver_agentes_agentes
raw/transferegov/transferegov/  ─▶ transferegov.bronze_transferegov ─▶ transferegov.silver_transferegov

salic.silver_agentes_agentes + transferegov.silver_transferegov
  ─▶ intermediate.int_salic_transferegov
  ─▶ cultura_em_numeros.eixo2_meta5_primeiro_acesso
```

## Por quê

**Schema por fonte, e não por domínio de meta.** Uma fonte é uma coisa que
existe fora do projeto e não muda quando a meta muda: o SALIC continua sendo
o SALIC quando a Meta 5 virar Meta 6. Organizar por meta fazia o mesmo dado
ser limpo duas vezes, uma em cada domínio que o usava, com regras diferentes.
Por fonte, a Silver do SALIC é uma só e todo produto a reaproveita.

**Intermediate como camada própria.** Sem ela, o cruzamento entre fontes
acontece ou na Silver (que então deixa de ser reutilizável, porque carrega a
escolha de um produto) ou na Gold (que então repete o mesmo join em cada
produto). Com uma camada só para cruzar, o join é escrito uma vez e a Gold
fica só com a regra de negócio.

**Gold por produto.** Cada produto tem dono, ritmo e consumidor próprios. Um
schema por produto permite versionar, dar permissão no Ranger e desligar um
produto sem tocar nos outros. Prefixo de camada na tabela seria redundante
com o schema e poluiria o nome que o painel exibe.

**Nome do arquivo igual ao nome da tabela.** Quem vê `salic.bronze_agentes_agentes`
no banco acha o arquivo sem precisar saber de `alias`, e quem vê o arquivo sabe
a tabela. Um nome só para as duas coisas é menos para errar. O prefixo da
camada no nome é o que torna `bronze_x` e `silver_x` modelos distintos no
projeto, como o dbt exige.

**Pasta por fonte, arquivos planos.** O `+schema: salic` é declarado uma vez
e vale para bronze e silver. Sem subpasta de camada, a camada se lê no nome
do arquivo, e a pasta inteira cabe num `ls`. Também é o que `salic_dbt/bronze`
já faz hoje com `sac__tabela.sql`: a pasta diz a fonte, o nome diz a origem.

**A convenção de nome da Gold é do produto, não da camada.** A Gold é a
única camada com dono e consumidor próprios por pasta. O que serve ao Cultura
em Números (eixo e meta no nome, porque a coordenação procura por meta) pode
não servir a um produto de patrimônio. Fixar uma regra única para todos
obrigaria o produto a nomear para o catálogo, e não para quem o consome. O
que a camada fixa é só o que vale para todos: pasta e schema por produto,
sem prefixo.

**Nulo nunca vira falso.** Um `false` que veio de ausência é indistinguível de
um `false` que veio da fonte. O ADR 0006 já registra a regra para endereço e
classificação ausentes; aqui ela vale para tudo.

## O que foi descartado

**Medalhão por domínio** (`cotas_dbt`, `agentes_dbt`), a organização de hoje
e a descrita no documento de modelo conceitual. Descartada pelo motivo acima:
limpeza duplicada com regras divergentes, e convenção de camada diferente em
cada pasta.

**Três camadas, sem Intermediate.** É o medalhão clássico e é o que o projeto
tem hoje. Descartado porque o cruzamento entre fontes não tem onde morar sem
contaminar a Silver ou duplicar na Gold.

**Schema por camada** (`bronze.salic_agentes`, `silver.salic_agentes`).
Descartado porque a permissão e a propriedade seguem a fonte, não a camada:
quem pode ver o SALIC pode ver bronze e silver do SALIC.

**Pasta por camada** (`models/bronze/salic/`) e **subpasta de camada dentro
da fonte** (`models/salic/bronze/`). Perderam para os arquivos planos: a camada
já está no prefixo do nome, e uma pasta a mais não acrescenta informação.

**Fonte no nome do arquivo com `alias`** (`bronze_salic__agentes.sql` virando
`bronze_agentes`). Era o que o desenho sugeria. Descartado porque cria dois
nomes para a mesma tabela; a fonte já está na pasta e no schema.

**Prefixo de camada ou de produto na Gold** (`gold_meta_5_primeiro_acesso`,
`cultura_em_numeros__meta_5`). Redundante com a pasta e o schema do produto.

**Etapa de staging** (`stg_*`) entre Raw e Bronze. A Bronze já é a cópia
fiel; o que o `stg_` faz hoje (tipar, filtrar lixo de parsing) é Silver.

## Consequências

**Nenhum modelo atual está em conformidade, e a migração é incremental.**
As regras valem para todo modelo novo a partir desta data. Os 712 existentes
migram por fonte, começando pela que destrava um produto, e cada lote migrado
é um PR com contagem antes e depois registrada, como o ADR 0004 e a skill
`govhub-pipeline-guide-minc` exigem. Não há data para a migração terminar;
há a regra de que o número de modelos fora do padrão não sobe.

**O que cada pasta de hoje vira.** Em linhas gerais, e a ser confirmado
modelo a modelo na migração:

| Hoje | Vira |
|---|---|
| `salic_dbt/bronze/*` (589 modelos que só tipam) | Silver do `salic`; a Bronze passa a ser `source` ou cópia fiel |
| `salic_dbt/core`, `meta3`, `meta4`, `meta5` | Silver do `salic` quando não cruzam; Intermediate quando cruzam |
| `cotas_dbt/bronze/stg_*` (tipam e filtram) | Silver da fonte correspondente (`relatorio_gestao`, `bbagil`) |
| `cotas_dbt/silver`, `agentes_dbt/silver` (cruzam fontes) | Intermediate |
| `cotas_dbt/gold`, `agentes_dbt/gold` | `models/cultura_em_numeros/`, na convenção desse produto (eixo e meta no nome) |
| `mapas_dbt/silver` | Já é Silver; vai para `models/mapas/silver_<entidade>.sql`, e o `alias` que hoje faz esse papel sai |
| `transferegov_bronze`, `bacen_bronze`, `ibge_sidra_bronze`, `bbagil_bronze` (tipam) | Silver da fonte |

**Macros e ferramentas que leem a convenção de pastas precisam acompanhar.**
`macros/get_custom_schema.sql` (schema por fonte e por produto),
`tests/test_salic_silver_governance.py` (exclui por `parts[0] == "bronze"`),
`docs-pages/tooling/collectors/dbt_models.py` (`CAMADAS` e sufixo `_dbt`),
os geradores em `scripts/` e na skill `bronze-salic-dbt`, e o
`schemaFilterPattern` das recipes do OpenMetadata. Uma reorganização de pasta
sem tocar nesses cinco pontos quebra em silêncio.

**A governança passa a cobrir todas as fontes.** Hoje só o `salic_dbt` tem
guarda offline. Com uma convenção única, o mesmo teste serve a todas as
camadas e fontes.

**A verificação de aderência é por script.** As regras automáticas deste ADR
(nome por camada, schema por camada, Bronze com uma só `source`, Silver sem
`ref()` a outra fonte, Gold sem `ref()` a Bronze, ausência de
`coalesce(…, false)`) são verificáveis sem banco e sem `dbt run`. A skill que
faz essa verificação e orienta a migração é entregue à parte; este ADR é a
fonte das regras que ela aplica.

**Fica em aberto**, para ADR próprio: a lista completa de produtos de dados e
como as dimensões comuns (tempo, território) são compartilhadas entre eles.
