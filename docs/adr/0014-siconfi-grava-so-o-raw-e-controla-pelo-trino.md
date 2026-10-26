# ADR 0014 — A ingestão do SICONFI grava só o raw no MinIO e controla a fila pelo Trino

- Status: proposto (até a aprovação do PR #86)
- Data: 2026-10-21
- Escopo: `dags/data_ingest/siconfi/`, `plugins/siconfi_storage.py`,
  `plugins/siconfi_controle.py`, `plugins/siconfi_migracao.py`,
  `plugins/cliente_siconfi.py`
- Origem: PR #86 e a revisão dele
- Relacionado: [ADR 0005](0005-ingestao-salic-por-trino-em-fatias.md),
  [ADR 0009](0009-data-lakehouse-extracao-airflow-camadas-minio-acesso-trino.md),
  [ADR 0010](0010-camadas-e-schemas-do-data-lakehouse.md),
  [ADR 0011](0011-transferegov-grava-no-datalakehouse.md)

## Contexto

A `siconfi_ingestion_dag` foi escrita antes dos ADRs 0009 e 0010. Ela gravava
cada página da API no Postgres, em `siconfi_bronze.raw_pages_<endpoint>` e
`<endpoint>_items`, e controlava a fila em `siconfi_control.work_queue` por
`psycopg2`, com `FOR UPDATE SKIP LOCKED` e um limitador de requisições
coordenado pelo banco. Isso fere duas regras do ADR 0009: o Airflow grava em
camada, e fala com o Postgres, que em produção ele não alcança (o ADR 0005 já
registra que um `psycopg2` passa no ambiente local e quebra no Serpro).

Havia também cerca de 300 mil páginas baixadas e uma fila com 1,1 milhão de
unidades, 292 mil delas já concluídas. Baixar tudo de novo custaria dias a 1
requisição por segundo.

O ADR 0011 (TransfereGov) grava `raw/` e `staging/` no MinIO. O ADR 0009, de um
dia depois, diz que não existe staging entre o raw e a bronze. Os dois
convivem sem que um diga qual vale.

## Decisão

**Só raw, sem staging.** Cada página vira um arquivo:

```text
raw/siconfi/<entidade>/ano=AAAA/mes=MM/dia=DD/<run_id>/<request_hash>_<page_offset>.json
```

O arquivo é o envelope da página (os campos da antiga `raw_pages_<endpoint>`,
com o `payload` intacto). A carga da bronze `siconfi.bronze_<entidade>` lê o
raw direto, como o ADR 0009 descreve. Para o SICONFI, isso resolve o conflito
entre o ADR 0009 e o 0011 a favor do 0009.

**O controle sai pelo Trino, só com inserção**, no padrão dos logs do SALIC v2 e
do Mapas (`control.*_ingestion_log`): tabelas no Postgres, criadas e escritas
por SQL enviado pelo `TrinoHook`. São quatro, em `<catálogo>.control`
(Variables `siconfi_trino_target_catalog` e `siconfi_trino_control_schema`, com
padrão `dw` e `control`):

| Tabela | Uma linha por |
|---|---|
| `siconfi_work_units` | unidade planejada, ou revisão nova de uma unidade |
| `siconfi_work_events` | tentativa: `success`, `no_data`, `permanent_error` ou `retry` |
| `siconfi_reference_refresh` | refresh completo de `/entes` e `/anexos-relatorios`, com a pasta do raw |
| `siconfi_plan_keys` | arquivo do extrato já planejado, por recorte |

Uma unidade está pendente enquanto a revisão vigente dela não tem evento final.
Nenhuma linha é atualizada ou apagada.

**Todo o SQL é do Trino.** A revisão vigente de cada unidade e o último evento
de cada revisão saem de `row_number()`, calculado no Trino; o filtro por
endpoint e por chave desce ao Postgres. A primeira versão usava a table function
`system.query` (SQL nativo, com `DISTINCT ON`) e criava índices pela procedure
`system.execute`. No servidor, o Trino nega as duas ao usuário do Airflow
(`Access Denied: Cannot execute function hml_analytics.system.query`,
26/10/2026), e a `preparar` da conversão parou ali. O mesmo pode valer no
Serpro, então o controle não depende delas.

Sem `system.execute`, a DAG não cria índice. Medido em 26/10/2026 com 397 mil
unidades, sem índice: a fila pendente da MSC sai em 1,5 a 2,7 s e a contagem por
estado em 2,5 s. A contagem por estado deu o mesmo resultado da consulta com
`DISTINCT ON`. O planejamento lê a revisão vigente de todas as chaves do
endpoint numa varredura só, uma vez por task, e compara em Python: 0,3 s no
extrato (5,6 mil chaves), 2 s na MSC orçamentária (94 mil) e 6,5 s na
patrimonial (189 mil). Consultar por lote de 500 chaves custava 1,4 s por lote,
e o planejamento reenfileira todos os entes × anos a cada run. Se a
fila ficar lenta, quem administra o banco pode criar os índices, uma vez:

```sql
CREATE INDEX IF NOT EXISTS siconfi_work_units_busca_idx
    ON control.siconfi_work_units (endpoint, work_key, planned_at DESC);
CREATE INDEX IF NOT EXISTS siconfi_work_events_busca_idx
    ON control.siconfi_work_events (endpoint, work_key, revision_marker, event_at DESC);
CREATE INDEX IF NOT EXISTS siconfi_plan_keys_busca_idx
    ON control.siconfi_plan_keys (source, scope_hash);
CREATE INDEX IF NOT EXISTS siconfi_reference_refresh_busca_idx
    ON control.siconfi_reference_refresh (endpoint, refreshed_at DESC);
```

O catálogo vem da Variable `siconfi_trino_target_catalog`. O padrão `dw` é o
nome no ambiente local; no servidor o catálogo do dbanalytics é outro, e sem a
Variable a `preparar` falha dizendo isso.

**Um consumidor por fila, em vez de trava de linha.** A DAG roda um run por vez
e uma task por vez (`max_active_tasks=1`). Isso garante que duas tasks nunca
pegam a mesma unidade e que o limitador de 1 req/s, agora em memória, vale para
a API inteira. A vazão não cai: a API já limita tudo a 1 req/s.

**Asset por entidade, só quando há arquivo.** Uma task `anunciar_<entidade>`
roda mesmo se a ingestão falhou, lista a pasta do run e emite o Asset
`raw/siconfi/<entidade>` com a pasta no `extra`. Sem arquivo, ela é pulada e
nenhum evento sai. A task `falhar_se_algo_falhou` deixa o run vermelho quando
qualquer task falha.

**Conversão única do histórico**, na `siconfi_migracao_raw_pages_dag` (manual):
copia as páginas antigas para o raw pela mesma função de gravação da ingestão,
com o campo `_migrado_de`, e a fila antiga para o controle novo. É a **exceção**
a esta decisão: lê o Postgres antigo por `psycopg2`, porque roda uma vez, no
ambiente atual, antes da mudança do Airflow para o Serpro.

## Por quê

**Sem staging** porque o ADR 0009 é posterior ao 0011 e diz isso com todas as
letras, e porque o SICONFI não precisa de conversão de formato: a página já é
JSON. O planejamento também lê o raw, e não um Parquet de texto, porque o
marcador de revisão é `str()` de campos do item, e com tudo virando texto ele
mudaria e reabriria unidades concluídas.

**Controle pelo Trino** porque é a topologia de produção, não preferência: o
Airflow só alcança o Trino e o MinIO. É o mesmo caminho que o SALIC v2 e o Mapas
já usam, então a equipe tem um padrão só.

**Só inserção** porque o conector PostgreSQL do Trino não tem `ON CONFLICT` nem
`FOR UPDATE SKIP LOCKED`, e um `DELETE` seguido de `INSERT` perderia a unidade
se a task caísse entre os dois. Um log de eventos nunca perde estado: o pior
caso é refazer uma unidade cujo evento não chegou a ser gravado, e a key do raw
é determinística, então refazer não duplica.

**Conversão em vez de nova extração** porque as páginas antigas já guardam a
resposta inteira e o `request_hash` delas é o mesmo que o código novo calcula.
Medido em 21/10/2026: nenhuma chave `(run_id, request_hash, page_offset)`
repetida nas 9 tabelas; planejar a partir do extrato convertido gerou 6.845
unidades, todas já na fila e nenhuma com marcador de revisão diferente.

## O que foi descartado

- **Staging em Parquet, como no ADR 0011.** Era o desenho da primeira versão do
  PR #86. Fere o ADR 0009, e o SICONFI não tem formato para converter.
- **Manter a fila no Postgres por `psycopg2`.** Não funciona em produção.
- **SQL nativo pelo `system.query` e índices pelo `system.execute`.** Era a
  versão do PR #86. O Trino do servidor nega as duas ao usuário do Airflow, e
  liberar `system.query` daria a ele SQL qualquer direto no Postgres.
- **Tabela de controle Iceberg via Trino.** Depende do Polaris, que ainda não
  existe, e tem os mesmos limites de transação.
- **Fila nenhuma, com o pendente calculado como "planejado menos o que existe no
  raw".** Perde tentativas e backoff por unidade e exige listar centenas de
  milhares de arquivos a cada run.
- **Pool do Airflow com uma vaga para o limite da API.** Se o pool não existir
  na instância, as tasks ficam em `scheduled` para sempre, sem log. O
  `max_active_tasks=1` da própria DAG tem o mesmo efeito sem depender de
  configuração externa.

## Consequências

- **Ordem no deploy:** a conversão da fila precisa rodar antes de a ingestão nova
  planejar. Sem ela, a ingestão não sabe o que já foi baixado. A conversão das
  páginas só termina com a ingestão antiga desligada.
- **Contrato com a carga da bronze (próximo PR):**
  - cada pasta de run tem só o que aquele run processou, então quem consome lê
    **todos** os eventos recebidos (`eventos_em_ordem`), não só o mais novo;
  - o `extra` traz a pasta do run, não a key de um arquivo;
  - uma requisição pode ter só parte das páginas na pasta, se a paginação falhou
    no meio, e a mesma página pode aparecer em runs diferentes. A bronze
    deduplica por `(request_hash, page_offset)`;
  - se o `page_limit` mudar entre duas tentativas no mesmo run, a mesma
    requisição fica com páginas de dois tamanhos na pasta, com deslocamentos
    diferentes e itens repetidos. Deduplicar por `(request_hash, page_offset)`
    não resolve: a bronze fica, por `request_hash`, só com as páginas da
    tentativa de `fetched_at` mais recente;
  - as pastas convertidas não emitem evento: a carga inicial da bronze varre
    `raw/siconfi/<entidade>/` inteiro.
- **O raw admite uma sobrescrita, como exceção aceita ao "nunca é sobrescrito"
  do ADR 0009:** a mesma página regravada no mesmo run (retry da task ou
  conversão repetida) cai na mesma key. O conteúdo é o mesmo, salvo se a API
  mudar a resposta entre as duas tentativas; nesse caso vale a mais recente.
- **Página vazia também vira arquivo.** Uma unidade `no_data` grava um JSON
  pequeno; na MSC isso pode chegar a centenas de milhares de arquivos. Fica
  assim por enquanto, porque o arquivo é a prova de que a requisição foi feita.
- **A ingestão roda em série.** Com fila cheia, um run leva de 5 a 6 horas (7
  tasks de ingestão de até `max_run_minutes`). O `@hourly` deixa de ser uma
  coleta por hora: com um run por vez e `catchup=False`, o próximo sai assim que
  o anterior termina, e de hora em hora só quando a fila está vazia. Se um dia a API do
  Tesouro aceitar mais de 1 req/s, paralelizar exige trava ou um consumidor por
  fila, não só tirar o `max_active_tasks`.
- **O retry não tem limite de tentativas**, como já não tinha antes: uma unidade
  com erro recuperável volta à fila 5 minutos depois, indefinidamente.
- **O TransfereGov e o INEP continuam com staging** (ADR 0011 e 0012). Se o
  "sem staging" do ADR 0009 valer para todos, cabe um ADR que substitua os dois.
- **`docs/siconfi-migracao-raw-pages.sql`** e os schemas `siconfi_bronze` e
  `siconfi_control` ficam obsoletos depois da conversão conferida, e saem com a
  equipe de acordo.
