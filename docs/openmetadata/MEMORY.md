# MEMORY — OpenMetadata (log operacional e canal de handoff)

Registro contínuo da convergência da integração de OpenMetadata entre
`data-application-minc` e `data-application-cidades`, e da frente SALIC que
vem depois dela.

> **Este arquivo é o canal de handoff entre agentes.** Várias IAs trabalham
> nisto em paralelo hoje. Quem escreve aqui está escrevendo para as outras,
> não para si. Se você é uma IA assumindo trabalho, leia §0 antes de tocar em
> qualquer arquivo.

---

## 0. Protocolo (LEIA ANTES DE MEXER EM QUALQUER COISA)

### Ao ASSUMIR

1. Leia **§1 (Estado atual)** — é a foto de onde as coisas pararam.
2. Leia **§9 (Frentes)** e **escolha uma frente ainda livre**. Marque-a como
   `EM CURSO · <seu nome> · <hora>` **antes** de começar. Duas IAs na mesma
   frente é retrabalho garantido.
3. Leia **§10 (Diário)** de trás para frente até entender o contexto recente.
4. **Confirme o estado antes de confiar nele.** Este arquivo é uma observação
   datada, não o sistema. Rode o que for barato (`git status`, um `grep`, o
   `make lint`) antes de assumir que algo está de pé.

### Ao ENTREGAR

1. Atualize **§1** — ela descreve o presente, não o histórico.
2. Marque a frente em §9 como `FEITO` ou devolva-a a `LIVRE` dizendo onde
   parou e qual era o próximo passo.
3. Acrescente entrada em **§10**: `### AAAA-MM-DD · <agente> · <título>`, com
   o que mudou, por quê, e o que ficou pendente.
4. Armadilha nova vai para **§7**, não só para o diário — no diário se perde.
5. **Nunca apague entrada de diário.** Corrija com entrada nova.

### Regras de convivência

- **Respeite a posse de arquivo declarada em §9.** Cada frente lista os
  arquivos que ela pode tocar. Precisou de um arquivo de outra frente? Registre
  em §10 e combine — não edite por cima.
- **Não desfaça decisão de outro agente sem entender o motivo.** As decisões
  estão em §8, com justificativa. Discordou? Registre e pergunte ao Lucas.
- **Distinga verificado de presumido.** Se não rodou, escreva "não verificado".
- **Nada de credencial em arquivo versionado** — ver §6, que já é um caso real.

---

## 1. Estado atual

**Última atualização: 2026-10-18 · Claude (F6 implementada na `feat/openmetadata-governanca`, não aplicada; ver §19)**

| Item | Situação |
|---|---|
| MinC — integração OM | ✅ **na `main`** (commit `4eec22a`, PR #21). `origin/main=daaae4c`; os 2 commits posteriores não alteram os arquivos OM. Airflow 3.2, pacote assado na imagem |
| MinC — DAG | `dags/openmetadata_ingestion_dag.py`, `@task` normal, segredo por `Variable` |
| MinC — recipes | 6, serviço `MinC` / `MinC - Airflow` / `MinC - Superset` |
| MinC — governança dbt | `meta.openmetadata` em 8 `schema.yml` (71 blocos), domínio/tier/owner/glossary |
| MinC — camada declarativa REST | 🟡 **implementada, não aplicada**: `dbt/minc/governance/` + `helpers/openmetadata/governanca/`, na `feat/openmetadata-governanca`. Simulação contra a instância limpa (0 falhas); `--confirmar` aguarda o Lucas. Ver §19 |
| MinC — `markDeletedTables` | ✅ `false` explícito em `postgres_metadata.yaml`, com dois testes (F2) |
| MinC — `glossary.py` | ✅ ligado na DAG como primeira task, antes das recipes (F3) |
| MinC — `semantic_relationships.py` | ⚠️ segue **órfão**, de propósito: valida `kind: MCIDSemanticRelationshipCatalog` e não existe catálogo desse formato aqui |
| MinC — `lineage.py` | ⚠️ em uso por 7 DAGs do `transferegov_fundo_a_fundo`, mas apontava para o database antigo `MinC.minc` até a correção de §19 (`DB_DW_DBNAME` passa a vencer) |
| MinC — testes OM | 660 linhas, 26 funções / 28 casos coletados (`test_openmetadata_packaging.py`, `..._execution.py`); validação desta sessão: 25 passed, 3 skipped em ambiente emprestado |
| Cidades — integração OM | ⚠️ na branch `fixture/ajustes-conjuntura`, **não mergeada** |
| Cidades — DAG | `@task.virtualenv` (255 pacotes) por causa do Airflow 2.8.1; ⚠️ sync de glossário/relações provavelmente perde o global `SEGREDOS_INGESTAO` ao serializar (§7) |
| Cidades — camada declarativa REST | ✅ `scripts/governance/` (4.064 linhas) + `dbt/mcid/governance/*.yml` (976) |
| Cidades — testes OM | ✅ 69 testes: `test_governanca_openmetadata.py` (890 linhas) + guarda de recipes (31 linhas); todos passaram nesta sessão |
| Cidades — `lineage.py` | ❌ não existe |
| Cidades — credencial vazada | 🟠 literal removido da cabeça em `88a110a`; ainda está em 2 commits da branch remota ativa. Rotação/instância não verificadas. Ver §6 |
| SALIC — bronze | ✅ 571 modelos SQL tipados/documentados na `main@d404531` |
| SALIC — silver núcleo | ✅ **6 modelos em `dbt/minc/models/salic_dbt/core/`**, na `feat/dbt-salic-silver`. A normalização de PRONAC foi corrigida após auditoria live; `status: Disabled` até o run da DAG. Ver §14 e §18 |
| SALIC — silvers M3/M4/M5 | 🟡 **8 de 14 modelos**: fato restrito de pagamento (F13) e bridge de local (F14) acrescentados. Perfil, município/IBGE, vulnerabilidade computável, execução municipal, proponente-ano e alvo seguem bloqueados. Ver §18 |
| SALIC — guarda de governança | ✅ `tests/test_salic_silver_governance.py`, 90 casos; suíte completa 187 passed, 3 skipped |

**Direção do fluxo histórico:** Cidades (`origin/refactor/openmetadata`, 2026-08-18)
→ MinC portou e evoluiu → Cidades recuperou de volta em `fixture/ajustes-conjuntura`.
O `semantic_relationships.py` do MinC ainda valida `kind: MCIDSemanticRelationshipCatalog`,
que é a impressão digital dessa origem.

---

## 2. Os dois repositórios lado a lado

### 2.1 Módulos compartilhados — o quanto já são iguais

Comparação byte a byte de `helpers/openmetadata/` (MinC `main` × Cidades `fixture/ajustes-conjuntura`):

| Módulo | Situação |
|---|---|
| `rendering.py` | **IDÊNTICO** |
| `runner.py` | **IDÊNTICO** |
| `workflows.py` | **IDÊNTICO** |
| `dbt_artifacts.py` | **IDÊNTICO** |
| `glossary.py` | difere em **1 linha** — Cidades tem `str(...)` no `sort key` para o type checker |
| `semantic_relationships.py` | difere em **2 comentários** e um retorno não usado |
| `config.py` | **divergente por projeto** (é o ponto de adaptação, por desenho) |
| `lineage.py` | **só no MinC** |
| `airflow_log_config.py` | **só no Cidades** |

**Leitura:** 4 dos 9 módulos já são o mesmo arquivo. Outros 2 estão a 3 linhas
de distância. O núcleo comum é real e está quase pronto — ver frente **F5**.

### 2.2 Recipes

Mesma estrutura nos dois, divergindo em `serviceName`, `schemaFilterPattern` e
comentários. Duas diferenças que **não** são cosméticas:

- Cidades tem `markDeletedTables: false` em `postgres_metadata.yaml`; MinC não.
- Desde `88a110a`, Cidades e MinC usam `type: SupersetAPI` na conexão do
  Superset. Antes desse fix, a fixture do Cidades usava `type: Postgres`
  diretamente e carregava credenciais literais; compare sempre com a cabeça
  atual, não com `ca5e0f5`/`4e0461d`.

### 2.3 Empacotamento — a divergência que **não** deve ser convergida

| | MinC | Cidades |
|---|---|---|
| Airflow | 3.2.2 | 2.8.1 |
| SQLAlchemy | 2.x | 1.4 |
| `openmetadata-ingestion` | assado na imagem (`infra/docker/airflow/requirements.lock.txt`, fora do Poetry) | virtualenv isolado por task, 255 pacotes, `venv_cache_path=/tmp/airflow_venvs` |
| Task | `@task` | `@task.virtualenv` |
| Segredo | `Variable.get` | `SEGREDOS_INGESTAO` resolvido no runtime |

O `openmetadata-ingestion` exige SQLAlchemy ≥ 2. O Airflow 2.8.1 exige < 2.
Não há versão que conviva (Cidades testou 1.10, 1.11, 1.12, 1.13). **O
isolamento do Cidades é consequência do Airflow 2, não preferência.** Quando o
Cidades subir para Airflow 3, essa diferença some sozinha — até lá, é a única
divergência legítima entre os dois, e o núcleo comum precisa tolerá-la.

---

## 3. O que funciona no Cidades e falta aqui

Ordenado por razão valor/custo.

| # | O quê | Onde está no Cidades | Custo |
|---|---|---|---|
| 1 | `markDeletedTables: false` | `recipes/postgres_metadata.yaml` | 1 linha |
| 2 | Flags `OM_INGEST_POSTGRES` / `OM_INGEST_DBT` | `infra/env/.env.example` | pequeno |
| 3 | Governança declarativa em YAML | `dbt/mcid/governance/{dominios,schemas,servicos,glossary,termos_mcid}.yml` | médio |
| 4 | Sincronizadores REST | `scripts/governance/sincronizar_{openmetadata,governanca,lake}.py` | grande |
| 5 | Auditoria instância × repo | `scripts/governance/auditar_openmetadata.py` + `make governance-audit-om` | médio |
| 6 | Linhagem coluna a coluna | `scripts/governance/linhagem_colunas.py` | médio |
| 7 | Chaves no catálogo (`tableConstraints`) | `scripts/governance/restricoes_dbt.py` | pequeno |
| 8 | Catálogo semântico seguro (**base do RAG**) | `scripts/governance/exportar_catalogo_openmetadata.py`; hoje aceita só `model`, não os 561 `source` do SALIC, e precisa política fail-closed de coluna | médio |
| 9 | Ordem de glossário e relações semânticas desenhada na DAG | `dags/openmetadata_ingestion_dag.py::_encadear`; ⚠️ as tasks virtualenv têm provável perda de global (§7), então a ordem é portável, a execução ainda não está comprovada | pequeno |
| 10 | Governança **depois** do conector dbt | idem — o conector dbt apaga certificação | pequeno |

**O item 8 é a ponte para a segunda metade do trabalho.** O
`exportar_catalogo_openmetadata.py` existe exatamente para entregar significado
e relação ao OpenMetadata *e ao GraphRAG* sem publicar SQL compilado, caminho de
lake, amostra ou coluna sensível. É o molde do que a frente SALIC precisa.

O que a camada REST entrega e o conector não conhece:
produto de dados · classificação `Uso` · certificação · MinIO/containers ·
linhagem de coluna · `tableConstraints` · auditoria.

## 4. O que funciona aqui e falta lá

| # | O quê | Onde está no MinC |
|---|---|---|
| 1 | Teste abrangente de empacotamento de recipes | `tests/test_openmetadata_packaging.py`; o Cidades já recebeu em `88a110a` uma guarda focada contra credenciais literais |
| 2 | `lineage.py` — `inlets`/`outlets` de DAG para tabela | `helpers/openmetadata/lineage.py`, usado por 7 DAGs |
| 3 | Airflow 3 nativo, sem venv de 255 pacotes por task | `infra/docker/airflow/requirements.lock.txt` |
| 4 | Suíte de execução (paginação, CLI vs in-process) | `tests/test_openmetadata_execution.py` |
| 5 | Conexão de Superset por API, sem credencial de banco | `recipes/superset_metadata.yaml` |
| 6 | Teste que deriva o `schemaFilterPattern` do projeto dbt | `test_scoped_recipes_share_the_same_schema_filter` |

---

## 5. O núcleo comum candidato

O que deve ser **idêntico em todo repositório do Gov Hub**, sem adaptação:

```
helpers/openmetadata/rendering.py          já idêntico
helpers/openmetadata/runner.py             já idêntico
helpers/openmetadata/workflows.py          já idêntico
helpers/openmetadata/dbt_artifacts.py      já idêntico
helpers/openmetadata/glossary.py           1 linha de distância
helpers/openmetadata/semantic_relationships.py   2 comentários de distância
helpers/openmetadata/lineage.py            só falta portar (defaults por env)
```

O que é **ponto de adaptação declarado**, e deve continuar sendo:

```
helpers/openmetadata/config.py             nomes, caminhos, flags, ordem
helpers/openmetadata/recipes/*.yaml        serviceName + schemaFilterPattern
helpers/openmetadata/glossaries/<org>.*    conteúdo
dbt/<projeto>/governance/*.yml             conteúdo
Variables do Airflow                       OM_HOST, INGESTION_TOKEN,
                                           PROFILER_TOKEN, CLASSIFICATION_TOKEN
```

**Questão aberta (decidir na frente F5):** onde o núcleo comum mora. O critério
do `CLAUDE.md` — *"se o conhecimento sobrevive a um `git mv` neste repositório,
pertence ao GovHub-skills"* — aponta para fora daqui, porque `rendering.py` e
`workflows.py` não citam MinC em lugar nenhum. Mas skill não é pacote Python:
copiar código continua sendo copiar. Alternativas a pesar: (a) manter cópia
sincronizada com teste de diff entre repos; (b) pacote instalável em
`GovHub-br`; (c) submódulo. **Não implemente antes de registrar a decisão em
§8 e confirmar com o Lucas.**

---

## 6. 🔴 Alerta de segurança — credencial commitada no Cidades

**Estado novo em 2026-09-01 17:36 -03:** o commit `88a110a`
(`fix(openmetadata): remove hardcoded credentials`) removeu o literal da cabeça
de `origin/fixture/ajustes-conjuntura`, voltou a conexão para `SupersetAPI`,
apagou o `local.env` versionado e adicionou um teste contra `password`/
`jwtToken` literais. O risco histórico descrito abaixo permanece até reescrita
das refs; rotação da credencial e limpeza da instância continuam não
verificadas. Portanto, “corrigido” aqui significa **mitigado na cabeça da
branch**, não encerrado.

**Topologia observável depois do fetch (correção às 17:44 -03).** A única ref remota
ativa que contém a exposição é `origin/fixture/ajustes-conjuntura`, nos commits
reescritos `ca5e0f5` e `4e0461d`; `88a110a` remove o literal na cabeça. A ref
`origin/refactor/openmetadata` não existe mais no servidor (`git ls-remote`) nem
no clone após o fetch. Os quatro
IDs antigos listados abaixo ainda existem como objetos locais/reflog, mas
`git branch -r --contains` não os associa hoje a ref remota. Eles podem persistir
em outros clones/caches; não devem mais ser descritos como “duas branches
publicadas” sem reconferir no servidor.

**Registro histórico anterior à reescrita.** A primeira versão desta seção
dizia "commit `40f0272`, de hoje". Uma segunda passagem mostrou que, naquele
histórico local, a exposição vinha de **18/08/2026** e aparecia em **4 commits,
2 refs e 2 caminhos de arquivo**. Esses IDs não são mais alcançáveis pelas refs
remotas observadas; a topologia atual é a do parágrafo anterior.

Senha em texto claro do usuário `superset` do PostgreSQL `10.0.0.73:5432`
(database `superset`):

```
4d6d9ae  2026-08-18  arthrok        airflow_lappis/dags/openmetadata/recipes/superset_metadata.yaml:13
59d8f1e  2026-08-18  arthrok        airflow_lappis/dags/openmetadata/recipes/superset_metadata.yaml:13
40f0272  2026-09-01  Lucas Bottino  helpers/openmetadata/recipes/superset_metadata.yaml:13
f1805ac  2026-09-01  Lucas Bottino  helpers/openmetadata/recipes/superset_metadata.yaml:13   (HEAD)
```

Esses quatro IDs pertencem ao histórico anterior à reescrita. No estado remoto
observado agora, a exposição alcançável está em `ca5e0f5` e `4e0461d`, na
fixture. **O caminho mudou entre históricos — busca por caminho não acha tudo;
busque por conteúdo (`git log --all -S`) e depois confirme alcance com
`git branch -r --contains`.**

**Terceiro lugar, além do git:** a recipe tem `storeServiceConnection: true`, o
que faz o OpenMetadata persistir a connection config no próprio servidor ao
executar. Se a ingestão já rodou, a senha está também no serviço
`Cidades - Superset` da instância. **Não verificado** — exige acesso à
instância.

Ordem da correção: **rotacionar primeiro** (o resto é higiene enquanto a senha
for válida) → tirar o literal do YAML → teste anti-reincidência → limpar as duas
branches → pedir GC ao GitHub.

**O conserto do YAML custa zero configuração:** o `config.py` do Cidades já
declara `SUPERSET_USERNAME`/`SUPERSET_PASSWORD` em `SEGREDOS_SUPERSET`
(linhas 114–119) e o `.env.example` já tem as três chaves (85–87). A fiação
ficou **órfã** quando a recipe trocou a conexão por API pela conexão direta no
banco. Voltar à forma do MinC (`type: SupersetAPI`) só reconecta o que existe —
e troca credencial de banco por credencial de API, de alcance menor.

**Runbook completo** com comandos prontos, as duas vias de reescrita de
histórico e a verificação final: entregue ao Lucas em
`secret-exposta-cidades-superset.md` (fora do repositório, de propósito).

**Nenhuma frente pode portar esse arquivo para cá com o literal dentro.** Ao
trazer a recipe de Superset do Cidades, traga a forma do MinC.

Isto não bloqueia as outras frentes. Vale como frente **F1**, independente.

---

## 7. Armadilhas registradas (as duas equipes)

- **`markDeletedTables` tem default `true`.** Rodar `postgres_metadata` contra
  um banco incompleto — ambiente restaurado pela metade, VPN caindo, schema
  ainda não materializado — marca como deletado tudo que o catálogo tem e o
  banco não. Sem aviso. **Protegido no MinC desde a F2**, com teste que também
  impede declarar a chave nas outras recipes, onde ela não tem efeito.
- **O sync de glossário usa o `INGESTION_TOKEN` e envia `displayName`.** O bot
  de ingestão tem a regra `DisplayName-Deny` (ver adiante nesta seção). Termo de
  glossário legitimamente tem `displayName`, e o módulo sempre o enviou — mas
  isto nunca rodou pela DAG aqui. **Se a primeira execução live recusar, o
  caminho é um token com papel próprio para o glossário, não remover o
  `displayName`.**
- **O conector dbt não cria tabela.** Ele faz `es_search_from_fqn` e *anexa*
  metadado a tabela existente. Quem cria é `postgres_metadata`; a ordem entre as
  duas é obrigatória. Modelo dbt sem tabela materializada não aparece no
  catálogo, e nenhuma configuração muda isso — falta `dbt run`, não recipe.
- **O conector dbt apaga a certificação.** Por isso, no Cidades, a task de
  governança é sempre a última do encadeamento.
- **A partir do dbt 1.10, `meta` no topo do modelo E em `config.meta` aborta o
  parse.** Modelo: vai sob `config`. Coluna e source: seguem no topo.
- **`dbt docs generate` precisa do banco** — é ele que monta o `catalog.json`.
  Sem conexão (VPN), `prepare_dbt_artifacts` falha antes de qualquer contato com
  o OpenMetadata, mesmo que só o `manifest.json` seja obrigatório.
- **O caminho do CLI não sai de `sys.executable`.** O task runner do Airflow roda
  com outro Python. É `shutil.which`.
- **Linhagem é operator, não backend.** `OpenMetadataLineageBackend` importa
  `airflow.lineage.backend`, módulo removido no Airflow 3.
- **`rendering.py` levanta se sobrar `${...}` sem valor, e varre o arquivo
  inteiro — inclusive comentários.** Não deixe marcador em linha comentada.
- **Paginação:** sem `set_entity_list_page_size`, o fetcher busca tabelas de 100
  em 100 e estoura timeout de 60s, que chega parecendo problema de rede. Por
  isso profiler e classifier rodam no processo, não pelo CLI.
- **O source `airflow` não roda pelo CLI** — ele inicializa o próprio pacote
  `airflow` e num subprocesso falha com erro genérico de plugin ausente.
- **`bulk_sink_batch_size: 10` é de propósito** — o proxy na frente do
  OpenMetadata corta a conexão antes de um PUT grande terminar.
- **O classifier usa `storeSampleData: false` e 50 linhas** — detecta PII sem
  persistir amostra. Não mexa: o repositório lida com CPF, CNPJ e dados de raça
  e deficiência.
- **O bot de ingestão é proibido de alterar `displayName`** (`IngestionBotRole`,
  regra `DisplayName-Deny`). Declarar um pede uma recusa a cada execução.
- **`@task.virtualenv` não leva globals do módulo junto.** No Airflow 2.8.1,
  `PythonVirtualenvOperator.get_python_source()` retorna apenas
  `inspect.getsource(self.python_callable)`. Hoje as tasks de glossário e
  relações do Cidades consultam `SEGREDOS_INGESTAO` sem importá-lo dentro da
  função nem recebê-lo como argumento; o resultado provável é `NameError` no
  venv. A suíte de 69 testes não executa a task serializada. Antes de portar ou
  declarar funcional, passe o mapping como argumento (preferível) ou faça o
  import interno e adicione teste de serialização/execução.
- **`airflow_log_config.py` do Cidades está órfão.** Busca no repositório só o
  encontra no README; não conte como componente funcional/portável até estar
  configurado no Airflow.
- **O `manifest.json` deixou de ser versionado em 06/09/2026.** O Cosmos passou a
  montar a DAG por `dbt ls` (102s sem partial parse, 34s com; cacheado por
  `enable_cache_dbt_ls`) e os
  timeouts de parse subiram no `docker-compose.yml`. Sumiram `make dbt-manifest`,
  `scripts/gerar_manifest_dbt.sh` e `tests/test_dbt_manifest.py`. Ver ADR 0007.
  As menções ao manifest versionado abaixo são registro histórico.
- **`OM_DATABASE=minc` aponta para o catálogo morto.** O database vivo é
  `MinC.dbanalytics` (1.774 tabelas); `MinC.minc` tem 647 tabelas paradas. O
  `local.env` trazia `OM_DATABASE=minc` e o `lineage.py` tinha `minc` como
  default: a linhagem das DAGs ia para o lugar errado sem erro. A simulação da
  governança casou 69 tabelas em vez de 1.227 pelo mesmo motivo. Corrigido:
  `DB_DW_DBNAME` vence. **O `.env` de quem já copiou o modelo continua errado.**
- **`dbt docs generate` grava um `run_results.json` sem teste.** Era o único
  comando da DAG do OM: os casos de teste do dbt chegavam ao catálogo, todos
  sem resultado, e nenhum incidente abria. `dbt test` precisa rodar DEPOIS do
  `docs generate`, senão o arquivo é sobrescrito.
- **Vírgula sem aspas no `minc.csv` truncava a descrição** (caso de
  `MinC.IncentivoFiscal`), e o resto virava sinônimo. `load_glossary` agora
  reprova linha com campo a mais.
- **`PUT` de termo de glossário só aceita relação genérica.** Relação tipada
  (`broader`, `partOf`…) só por `PATCH /relatedTerms` com `TermRelation`. Por
  isso o `glossary.py` só cria termos, e as relações moram em `termos_minc.yml`.
- **Suíte lógica executada não pode conter teste do dbt**: o OpenMetadata
  tentaria rodá-lo e falharia sempre. A suíte da recipe `postgres_testsuite`
  (`minc_testes_nativos`) só tem nativos; as suítes por produto são de
  observação.
- **O hook de pre-commit roda `make format` no repositório inteiro.** Com
  black em Python 3.11 ele reformata dezenas de arquivos alheios e falha em
  `scripts/perfilar_colunas.py` (f-string com barra, sintaxe do 3.12),
  abortando o commit. Restaure os arquivos alheios antes de tentar de novo.
- **`lpad` do Postgres TRUNCA.** `lpad('2023', 2, '0')` devolve `'20'`, sem
  aviso. Toda montagem de PRONAC a partir de `anoprojeto`/`sequencial` precisa
  do guarda de regex ANTES do `lpad` — é o que a macro `pronac_de_ano_sequencial`
  faz. Sem ele, ano gravado com quatro dígitos produz chave errada que casa com
  outro projeto.
- **O SALIC identifica o mesmo projeto por três chaves.** `pronac`, `idpronac` e
  `idprojeto`, e nenhuma tabela da bronze v2 tem as três juntas.
  `sac__abrangencia` — única fonte de local de realização, e portanto da Meta 4
  — só tem `idprojeto`. `sac__captacao.idprojeto` é 0 na maior parte das linhas
  e não serve de ponte. Ver `map_chave_projeto_rouanet` (§14).
- **Com dbt desligado, relações semânticas podem ficar desconectadas no
  Cidades.** `_encadear` só liga a task de relações dentro do ramo
  `task_id == "dbt_metadata"`; com `OM_INGEST_DBT=false`, ela é criada mas não
  entra na cadeia principal. Testar flags individualmente e definir política
  explícita (pular relações ou ligá-las ao predecessor correto).

---

## 8. Decisões registradas

| Data | Decisão | Por quê |
|---|---|---|
| 2026-09-01 | O caminho é **somar as duas abordagens**, não escolher | Conector faz estrutura melhor (roda sozinho, em dia); REST faz governança que o conector não conhece. São camadas, não alternativas |
| 2026-09-01 | A divergência de empacotamento (venv isolado × imagem) **não** é convergida | É consequência do Airflow 2 no Cidades. Some quando ele subir para o 3 |
| 2026-09-01 | Ao portar a recipe de Superset, vale a forma do **MinC** (`SupersetAPI`) | Não exige credencial de banco de terceiro — e foi a forma do Cidades que vazou senha |
| — | *(aberto)* Onde mora o núcleo comum — cópia sincronizada, pacote ou submódulo | Ver §5 |
| 2026-10-18 | Governança do OM é declarada em `dbt/minc/governance/` e aplicada por PATCH depois dos conectores; conectores seguem a única fonte da estrutura | ADR 0013 (proposto). Sem isso, 0 de 1.774 tabelas tinham produto, e a certificação some a cada execução do conector dbt |
| 2026-10-18 | `dbt test` roda na DAG do OM, sem a bronze do SALIC | Lucas: resultado de teste precisa chegar ao catálogo para haver incidente. 495 testes no recorte; os 4.883 da bronze varreriam 157 M de linhas |
| 2026-10-18 | Alerta de qualidade só dentro do OpenMetadata (incidente + feed) | Lucas, 2026-10-18 |
| 2026-10-18 | Linhagem de coluna fica só com o conector dbt | Ele já a calcula do SQL compilado (6–23 colunas por aresta, fonte `DbtLineage`); um segundo writer brigaria com ele. A auditoria mede a cobertura |

---

## 9. Frentes de trabalho — reivindique uma antes de começar

Cada frente lista os arquivos que ela pode tocar. **Frentes com conjuntos
disjuntos rodam em paralelo sem conflito.** Marque o estado ao assumir.

| ID | Frente | Repo | Estado |
|---|---|---|---|
| F1 | Credencial vazada: rotacionar, parametrizar, testar | cidades | 🟠 PARCIAL — HEAD/teste corrigidos em `88a110a`; rotação, instância e histórico pendentes |
| F2 | `markDeletedTables: false` em `postgres_metadata` + teste | minc | ✅ FEITO · Claude · 2026-09-02 19:40 -03 |
| F3 | Ligar `glossary` e `semantic_relationships` na DAG | minc | ✅ FEITO · Claude · 2026-09-02 19:40 -03 — só o glossário, como a própria frente recomendava |
| F4 | Flags `OM_INGEST_POSTGRES` / `OM_INGEST_DBT` | minc | ✅ FEITO · Claude · 2026-09-02 19:40 -03 |
| F5 | Definir e implementar o núcleo comum | ambos | 🟡 LIVRE — decidir §5 antes |
| F6 | Extrair núcleo e adaptar `scripts/governance/` + declarar `dbt/minc/governance/*.yml` | minc | 🟡 IMPLEMENTADA · Claude · 2026-10-18 · branch `feat/openmetadata-governanca`; falta `--confirmar` e a primeira execução da DAG — ver §19 |
| F7 | Portar `lineage.py` para o Cidades | cidades | 🟢 LIVRE |
| F8 | Portar o teste anti-credencial para o Cidades | cidades | ✅ FEITO em `88a110a` (`test_openmetadata_recipes.py`) |
| F9 | SALIC: bronze e silver para RAG | minc | 🟣 EM CURSO na `feat/dbt-salic-silver`; bronze concluída (571), plano silver em F11 |
| F10 | Validar o levantamento, comparar os commits atuais e detalhar o próximo recorte | ambos | ✅ FEITO · Codex · 17:47 -03 |
| F11 | Planejar silvers SALIC do FigJam + documentação OpenMetadata e dividir entre agentes | minc | ✅ FEITO · Codex + 3 agentes · 2026-09-02 16:10 -03 · plano em §13 |
| F12 | Implementar o núcleo silver do SALIC (Onda 1, papel do Agente A) | minc | ✅ FEITO · Claude · 2026-09-02 18:30 -03 · entrega em §14 |
| F13 | Meta 3 — perfil temporal do agente e fato de pagamento | minc | 🟠 PARCIAL · Codex · 2026-09-03 — fato de pagamento entregue; perfil temporal e gold bloqueados pela fonte/regra |
| F14 | Meta 4 — IBGE, capitais, vulnerabilidade e bridge de execução | minc | 🟠 PARCIAL · Codex · 2026-09-03 — bridge entregue como `Disabled`; cobertura de PRONAC é só 4.269 projetos |
| F15 | Meta 5 — histórico do proponente e golds de primeiro acesso | minc | 🟢 LIVRE — depende de `fct_evento_acesso_rouanet` e da chave HMAC |
| F16 | Unificar `salic_bronze` e `salic_dbt` sob um único grupo de modelos dbt | minc | ✅ FEITO · Claude · 2026-09-02 20:20 -03 |

### F1 — Credencial (cidades) 🔴
**Arquivos/refs:** HEAD e histórico de
`helpers/openmetadata/recipes/superset_metadata.yaml`, teste de recipes,
instância OpenMetadata e `origin/fixture/ajustes-conjuntura`. A antiga
`refactor/openmetadata` não existe no servidor na verificação de 17:44 (§6).
**Passos restantes:** rotacionar a credencial, verificar/remover a conexão
persistida na instância, reescrever os dois commits ancestrais da fixture e
verificar clones/caches/GC. O HEAD e a guarda de teste já foram corrigidos em
`88a110a`.

### F2 — `markDeletedTables` (minc) 🟢
**Arquivos:** `helpers/openmetadata/recipes/postgres_metadata.yaml`,
`tests/test_openmetadata_packaging.py`.
**Passos:** acrescentar `markDeletedTables: false` sob `sourceConfig.config` de
`postgres_metadata.yaml`. **Só nessa recipe** — verificado: no Cidades a chave
aparece uma única vez, e é a única das três com `type: DatabaseMetadata`;
profiler e classifier não apagam nada. Comentar o porquê no arquivo. Teste novo
que falha se a chave sumir. `make lint && make test`.

### F3 — Glossário e relações na DAG (minc) 🟢
**Arquivos:** `dags/openmetadata_ingestion_dag.py`, `helpers/openmetadata/config.py`.
**Contexto:** os dois módulos existem, são funcionais, e **ninguém chama**. O
glossário `MinC` (26 termos, `glossaries/minc.yaml` + `.csv`) foi aplicado à mão
em algum momento; se alguém editar os termos, os FQNs referenciados pelos
`schema.yml` passam a apontar para termo inexistente.
**Molde:** `_encadear` do `dags/openmetadata_ingestion_dag.py` do Cidades —
glossário **antes** das recipes (os FQNs precisam existir para a ingestão
resolvê-los), governança **depois** de tudo.
**Cuidado:** `semantic_relationships.py` valida
`kind: MCIDSemanticRelationshipCatalog`. Não existe catálogo desse formato aqui.
Ou você cria um (`helpers/openmetadata/semantic_relationships/minc.yaml`, molde
em `mcid.yaml` do Cidades, 1.255 linhas), ou deixa esse módulo de fora e liga só
o glossário. **Ligar só o glossário já resolve o problema real.**

### F4 — Flags faltando (minc) 🟢
**Arquivos:** `helpers/openmetadata/config.py`, `infra/docker-compose.yml`,
`local.env`, `tests/test_openmetadata_packaging.py`.
**Nota:** o `compose` já define `OM_INGEST_PROFILER` e `OM_INGEST_CLASSIFIER`
com default `true`, enquanto o `config.py` tem default `False`. Divergência
inofensiva hoje (o compose vence), mas confusa — alinhe.

### F5 — Núcleo comum (ambos) 🟡
**Antes de escrever código:** registre a decisão em §8 e confirme com o Lucas.
**Trabalho técnico já mapeado:** reconciliar `glossary.py` (1 linha — adote a
versão do Cidades, com `str()`, que é a que passa no type checker) e
`semantic_relationships.py` (2 comentários). Depois disso, 6 dos 9 módulos são
byte a byte idênticos.

### F6 — Camada declarativa REST (minc) 🟡
**Arquivos novos:** núcleo parametrizado sob `scripts/governance/*.py`,
`dbt/minc/governance/*.yml`, alvos no `Makefile` e testes. **Não copiar a pasta
inteira**: ela contém caminhos `dbt/mcid`, vocabulário conjuntura, propriedades
`mcid*` e operações globais perigosas (§12).
**Ordem sugerida** (cada item é entregável sozinho):
1. contrato de configuração compartilhado (project/governance/output dirs,
   service/database, namespace, dialect, env mapping e source adapter)
2. funções puras/auditorias offline + `restricoes_dbt.py` + testes
3. `dbt/minc/governance/{servicos,schemas,dominios}.yml` com conteúdo MinC/SALIC
4. exporter fail-closed para RAG, com suporte explícito a `source` e `model`
5. overlay idempotente pós-dbt, por propriedade, sem reescrever estrutura
6. auditoria live exata e paginada + `make governance-audit-om`
7. somente depois: linhagem de coluna/GX/adaptadores de origem necessários
**Princípio que vem junto:** *nada é criado pela interface*. O YAML é a verdade;
quem editar na tela perde no próximo sync.
**Não porte `sincronizar_lake.py` por ora** — o MinC não tem MinIO no fluxo.

### F7 — `lineage.py` para o Cidades 🟢
**Arquivos:** `helpers/openmetadata/lineage.py` (novo, no Cidades), DAGs de
ingestão do Cidades.
**Adaptação:** `OM_SERVICE`, `OM_DATABASE`, `OM_PIPELINE_SERVICE` têm default do
MinC; precisam casar com o `serviceName`/`database` das recipes do Cidades,
senão a linhagem aponta para tabela que não existe.

### F16 — Um domínio dbt só para o SALIC (minc) 🟢
**Arquivos:** `dbt/minc/models/salic_bronze/**` (571 SQL + 6 `schema.yml` + 6
`sources_*.yml`), `dbt/minc/models/salic_dbt/**`, `dbt/minc/dbt_project.yml`,
`dbt/minc/manifest.json`.
**FEITA em 2026-09-02 20:20 -03.** O resultado está em §16; o texto abaixo é o
enunciado original da frente, preservado.

**Decisão (Lucas, 2026-09-02):** as duas pastas têm que virar **um grupo de
modelos só**. Hoje o SALIC é o único domínio do projeto que quebra a convenção
dos outros dois — `agentes_dbt` e `cotas_dbt` são uma pasta por domínio com as
camadas como subpastas, e o SALIC ficou com `salic_bronze/` e `salic_dbt/` no
mesmo nível. O alvo é `salic_dbt/{bronze,core,...}`, com o bloco do
`dbt_project.yml` colapsado num só.
**Por que é barato:** os dois já materializam no mesmo schema físico (`salic`),
então **nenhum FQN do OpenMetadata muda** — o catálogo é
`database.schema.tabela`, e nem o schema nem o nome das relações se mexem.
Ninguém precisa reingerir nada.
**Cuidados:**
- `+materialized` difere entre as camadas (bronze é `view`, o núcleo é
  `table`); ao colapsar o bloco, as duas configurações precisam sobreviver como
  configuração de subpasta, não como default único;
- o comentário longo do `dbt_project.yml` que explica por que o schema é
  `salic` e não `salic_bronze` continua valendo e não pode se perder no move;
- `tests/test_openmetadata_packaging.py` deriva os schemas do `dbt_project.yml`
  — se o `+schema` sumir junto do bloco, as recipes passam a divergir;
- regerar o `manifest.json` com dbt 1.10 e commitar junto (§7);
- `git mv` preserva histórico dos 571 arquivos; cópia + delete não.

---

## 10. Diário

### 2026-09-01 · Claude · Investigação inicial e montagem deste documento

**O que foi feito.** Levantamento comparativo completo entre
`data-application-minc` (`main`, commit `4eec22a`) e
`data-application-cidades` (`fixture/ajustes-conjuntura`), sem alterar código em
nenhum dos dois. Este arquivo é o produto.

**Operações executadas** (todas leitura; nenhuma escrita fora deste arquivo):

1. `git branch -a` e `git remote -v` nos dois repositórios.
2. `git log --oneline` em `main`, `feat/openmetadata-airflow3` e `feat/catalogo`
   (minc) — confirmado que a integração está **na `main`** via PR #21.
3. `find` + `grep -ril openmetadata` nos dois repositórios — inventário de
   arquivos.
4. Leitura de `docs/governance/comparacao-minc-openmetadata.md` e
   `docs/governance/backlog-openmetadata.md` (cidades). **O comparativo já
   existia, escrito hoje, do lado do Cidades** — boa parte de §2 e §3 vem dali,
   conferida contra o código.
5. Leitura de `helpers/openmetadata/GUIA.md` (minc).
6. `diff -u` arquivo a arquivo de `helpers/openmetadata/*.py` e de
   `recipes/*.yaml` entre os dois repositórios → resultado em §2.1 e §2.2.
7. `diff -u` de `dags/openmetadata_ingestion_dag.py` → §2.3.
8. `git log`/`git show` em `helpers/openmetadata/recipes/superset_metadata.yaml`
   (cidades) → **credencial commitada**, §6.
9. `grep -rnE '(password|token|secret|senha)...'` em `helpers/openmetadata/` e
   `scripts/governance/` do Cidades → uma única ocorrência, a de §6.
10. `grep -rn markDeleted` nas recipes do MinC → **nenhuma ocorrência**, §7.
11. Leitura dos cabeçalhos dos 8 scripts de `scripts/governance/` (cidades) e
    dos 3 YAML de `dbt/mcid/governance/` → §3.
12. Leitura de `helpers/openmetadata/config.py` (minc) e comparação com o do
    Cidades → §2.3 e F4.
13. `grep` de `openmetadata.lineage` em `dags/` (minc) → 6 DAGs do
    `transferegov_fundo_a_fundo`, §4.
14. Inventário do SALIC: `ls`/`wc -l` em `dbt/minc/models/salic_bronze/` e
    `scripts/salic_docs/` → §11.
15. `grep -n "^def test_"` nas duas suítes de teste do MinC → §4.

**Achados que mudam o plano.**

- A integração do MinC **não está em branch**, está na `main`. O trabalho aqui é
  de complemento, não de merge.
- 4 dos 9 módulos já são byte a byte idênticos; outros 2 estão a 3 linhas. O
  núcleo comum é mais barato do que parecia.
- O MinC **não tem** `markDeletedTables: false`, apesar de a armadilha estar
  documentada no próprio `GUIA.md` do MinC. É o item de maior razão
  valor/custo do levantamento inteiro.
- Credencial real commitada no Cidades (§6).
- O `exportar_catalogo_openmetadata.py` do Cidades já foi escrito **pensando em
  GraphRAG**. É o molde da frente SALIC, não código novo a inventar.

**Não verificado.** Nada foi executado contra a instância
(`openmetadata.clusterlab.lappis.rocks`) nem contra banco — sem VPN nesta
sessão. Números de cobertura de catálogo citados em §3 vêm do
`backlog-openmetadata.md` do Cidades, datados de 2026-08-31, e **não foram
reconferidos**.

**Próximo passo.** Distribuir as frentes de §9. F1 e F2 são as de maior retorno
imediato e não colidem entre si.

---

## 11. Frente SALIC — bronze e silver para o RAG

*(segunda metade do trabalho; a investigação começa quando as frentes de §9
estiverem distribuídas)*

### O que já existe

| Arquivo | Tabelas | Linhas |
|---|---|---|
| `dbt/minc/models/salic_bronze/sources_sac.yml` | 431 | 29.489 |
| `dbt/minc/models/salic_bronze/sources_tabelas.yml` | 67 | 3.164 |
| `dbt/minc/models/salic_bronze/sources_agentes.yml` | 57 | 2.641 |
| `dbt/minc/models/salic_bronze/sources_controledeacesso.yml` | 5 | 241 |
| `dbt/minc/models/salic_bronze/sources_bdcorporativo.yml` | 1 | 54 |
| **total** | **561** | **35.589** |

Todos apontam para `schema: bronze`. Todos já trazem
`meta.openmetadata` com `tier`, `domain` e `owner` por tabela, e descrição por
coluna gerada de `scripts/salic_docs/` (dicionário SchemaSpy do SALIC cruzado
com perfil estatístico do bronze). Descrição ausente vem marcada como
`[não documentado no dicionário de dados original do SALIC]`.

`scripts/salic_docs/` tem 9 etapas numeradas (`01_parse_dictionary` →
`09_generate_dbt_sources`) e uma `lib/` com 10 módulos, incluindo `profiler.py`,
`semantics.py` e `dbt_source_builder.py`.

### O que **não** existe — e a exceção já funcional

**Nenhum modelo `.sql` dentro de `salic_bronze`.** A pasta tem só YAML de
source e não há ainda `salic_dbt/bronze/` nem `salic_dbt/silver/`. Existe,
porém, um silver SALIC funcional em
`agentes_dbt/silver/eventos_fomento_rouanet.sql`: ele lê
`sac__projetos`, `sac__tbapiprojetorouanet` e `sac__captacao`, cruza por PRONAC
e alimenta modelos posteriores. Ele contém `beneficiario_documento` normalizado
(CPF/CNPJ), então é referência de regra/linhagem, **não corpus direto para RAG**.

Os dois domínios principais
com modelos são `cotas_dbt` (24 SQL) e `agentes_dbt` (**12 SQL em
`origin/main=daaae4c`**; o worktree local em `4eec22a`, anterior à reorganização,
ainda tem 23). Há ainda 1 SQL utilitário em `models/metadata/`.

### Ganchos já disponíveis

- A ingestão tem duas vias — a direta e a por Trino em fatias
  ([ADR 0005](../adr/0005-ingestao-salic-por-trino-em-fatias.md)),
  em `dags/data_ingest/salic/salic_ingestion{,_trino}.py`.
- `scripts/governance/exportar_catalogo_openmetadata.py` (Cidades) já resolve
  o problema difícil do RAG: publicar **significado e relação, nunca SQL
  compilado, caminho de lake, amostra ou coluna sensível**. Snapshots nunca
  entram. Coluna classificada como sensível é omitida **sem expor sequer o
  identificador**.
- `dbt/mcid/governance/schemas.yml` (Cidades) já tem o vocabulário de política
  de RAG por schema: `rag_publication: prohibited` |
  `eligible_after_security_validation` | `determined_per_model`. Bronze é
  `prohibited` e mesmo assim é publicada no catálogo — *"o que se publica é a
  existência e a topologia, nunca o conteúdo"*.

### Perguntas a responder na investigação

1. Quais das 561 tabelas de fato entram no escopo? 431 delas são do `sac`, e o
   dicionário do SALIC não documenta boa parte das colunas.
2. O bronze está materializado no banco? **Sem tabela materializada, o conector
   dbt não publica o modelo no OpenMetadata** (§7). Verificar exige VPN.
3. Bronze = view fiel 1:1 sobre a source, ou já há limpeza? A convenção dos
   outros dois domínios responde — conferir `cotas_dbt/bronze/`.
4. Que política de `rag_publication` vale para o SALIC? O SALIC tem CPF, CNPJ e
   dados de proponente — a resposta não é a mesma de `cotas`.
5. O RAG consome do OpenMetadata ou do catálogo semântico exportado? Isto muda o
   formato do que a silver precisa expor.

### O snapshot de 561 não é o universo atual

- As contagens 431 + 67 + 57 + 5 + 1 e 35.589 linhas de YAML estão corretas.
- O `manifest.json` versionado (dbt 1.10.23, gerado em 2026-08-31) contém 561
  sources/5.064 ocorrências de coluna. Destas, 4.426 (**87,4%**) estão marcadas
  como não documentadas no dicionário original; 147 tabelas são
  `possivel_obsoleta` e 28 `nao_documentado`.
- Existem 858 testes de source (552 `not_null`, 257 `unique`, 49
  `accepted_values`) cobrindo 440 das 561 tabelas. As regras do gerador são
  conservadoras e usam documentação + perfil observado.
- Porém, `infra/trino/GUIA.md` registra dry-run de **1.139 tabelas / 1,14 TB**,
  e `plugins/trino_bronze.py` registra **953 tabelas só no SAC**. Logo os 561
  representam um recorte/snapshot documentado, não a dimensão atual da origem.

### Drift e reprodutibilidade do pipeline `scripts/salic_docs`

Antes de reexecutar as nove etapas, corrigir:

1. `lib/db.py` ainda exige `DB_DW_PASS` e fixa database `minc`; o contrato atual
   documenta `DB_DW_PASSWORD` e `DB_DW_DBNAME`.
2. `scripts/salic_docs/output/` é ignorado e está ausente nesta sessão; sem
   `dictionary.json`, perfis e `merged.json`, as etapas 3–9 não reproduzem
   offline os arquivos atuais.
3. `dbt/minc/docs/salic/salic_semantic_layer.yaml` referencia cinco
   `schemas/*.yaml` que não estão no repositório.
4. O gerador da etapa 9 ainda usa domínio genérico `Cultura` e gera
   `Certification.Bronze` sem owner, enquanto os YAMLs atuais têm
   `Cultura.Incentivo Fiscal` e `minc-data-engineering`. Reexecutá-lo hoje
   causaria regressão nos 561 blocos.
5. Não há testes específicos de reprodutibilidade/privacidade para
   `scripts/salic_docs`.

Há ainda drift entre código e documentação no modelo existente: o SQL de
`eventos_fomento_rouanet` já usa `sac__projetos` como fonte principal e a API
como fallback, mas o `silver/schema.yml` ainda diz que a primeira estava vazia.

### Segurança para documentação e RAG

O profiler coleta cinco linhas reais por tabela, persiste amostras em JSON,
leva até cinco linhas ao YAML semântico e publica min/max/frequências nos
documentos técnicos. O `.gitignore` reduz chance de commit, mas não torna o
fluxo seguro. A heurística atual acha CPF/CNPJ/senha/e-mail/telefone, porém não
cobre adequadamente nome, endereço, nascimento, raça, deficiência, conta,
agência, usuário/logon ou IP.

Até existir exportador com política explícita e testes:

- bronze e perfis não são corpus de RAG;
- o conector Postgres pode catalogar existência/topologia, sem sample data;
- coluna para RAG entra por allowlist/classificação, não por denylist textual;
- não versionar os `schemas/*.yaml` se puderem carregar amostras reais;
- reaproveitar termos já existentes no glossário MinC (Rouanet, Proponente e
  Identificador Único), sem criar fonte paralela.

### Primeiro recorte bronze/silver proposto

Novo domínio dbt `salic_dbt`, schema físico `salic` (não `dados_salic`, que já
nomeia um source legado):

| Camada | Modelo | Origem/conteúdo |
|---|---|---|
| bronze view | `stg_salic_projetos` | `sac__projetos`, omitindo CPF/CNPJ, analista, logon e textos operacionais sensíveis |
| bronze view | `stg_salic_captacoes` | `sac__captacao`, omitindo documento do mecenas |
| bronze views | área, segmento, situação, mecanismo | quatro dimensões pequenas, tipadas e descritas |
| silver table | `projetos_rouanet_catalogo` | uma linha por PRONAC; projeto, UF, dimensões descritas, datas e totais de captação; nenhuma pessoa/documento/contato/conta/usuário |

Escala registrada no snapshot: 276.666 projetos, 668.536 captações, 8 áreas,
222 segmentos, 296 situações e 7 mecanismos. Aprovação (472.266 linhas) fica
para a segunda fatia, depois de validar grão e escolha entre múltiplos registros.

O `eventos_fomento_rouanet` existente deve ser refatorado para consumir os
stagings **depois** que o MVP preservar resultados e reconciliações; incluí-lo
no primeiro commit amplia o risco. Criar o schema `salic` exigirá atualizar
`dbt_project.yml` e os filtros equivalentes das três recipes OpenMetadata.

### Validações live pendentes

1. comparar por banco as 1.139 tabelas descobertas com as 561 declaradas;
2. confirmar existência/contagem e grão das seis fontes do MVP;
3. medir unicidade, casts inválidos e reconciliações de captação;
4. confirmar que o catálogo/export RAG não contém PII nem métricas reveladoras;
5. gerar manifest/catalog em temporário e conferir linhagem na instância.

Não havia PostgreSQL, Trino ou containers locais escutando; nenhuma contagem
live da origem foi executada nesta passagem.

### 2026-09-01 · Claude · Correção do escopo do segredo exposto (§6)

**O que estava errado.** A primeira versão da §6 dizia que a credencial do
Superset estava num commit único (`40f0272`), de 01/09, na
`fixture/ajustes-conjuntura`. Eu havia buscado pelo *caminho do arquivo*, e o
caminho mudou entre as branches — `airflow_lappis/dags/openmetadata/recipes/`
na antiga, `helpers/openmetadata/recipes/` na nova.

**O que é verdade.** Busca por *conteúdo* (`git log --all -S`) devolve **4
commits em 2 branches**, a mais antiga de **18/08/2026**, autor `arthrok`, na
`origin/refactor/openmetadata`. Detalhe em §6.

**Achados novos.**

1. `storeServiceConnection: true` na recipe → se a ingestão rodou, a senha está
   persistida também no serviço `Cidades - Superset` da instância OpenMetadata.
   Não verificado (sem acesso à instância).
2. O conserto do YAML custa zero configuração: `SEGREDOS_SUPERSET` e o
   `.env.example` do Cidades já declaram as chaves da conexão por API. Ficaram
   órfãs quando a recipe passou a conectar no banco direto.

**Lição de método, para as outras IAs.** Em varredura de segredo, **buscar por
caminho de arquivo é insuficiente**. `git log --all --oneline -S '<literal>'` e
`git grep '<literal>' $(git rev-list --all)` são o mínimo — o primeiro acha os
commits, o segundo acha os caminhos.

**Entregue.** Runbook `secret-exposta-cidades-superset.md` (fora do repositório).
Nenhum arquivo do `data-application-cidades` foi alterado.

**Pendente.** Rotação da senha; verificação da instância OpenMetadata; decisão
sobre apagar a `refactor/openmetadata`.

### 2026-09-01 · Codex · Retomada e validação independente (em curso)

**Objetivo desta passagem.** Confirmar o handoff existente contra o estado
atual dos dois worktrees, sem alterar código de produção, e transformar os
achados em um recorte executável por múltiplas IAs. Frente assumida: **F10**.

**Operações executadas até 17:36 -03** (somente leitura, exceto esta memória):

1. No MinC, `pwd`, busca de `AGENTS.md`, `git status --short --branch`,
   `git remote -v` e `git branch --all`: confirmado `main` em `4eec22a`, com
   mudanças preexistentes em `docs/GUIA.md` e `docs/openmetadata/`.
2. Busca por repositórios irmãos com `find .. -maxdepth 2`: localizado
   `../data-application-cidades`.
3. Leitura integral desta memória (543 linhas antes desta entrada) e inspeção
   do diff preexistente de `docs/GUIA.md`. Nada foi sobrescrito.
4. No Cidades, `git status --short --branch`, remotes e branches: worktree na
   `fixture/ajustes-conjuntura`, acompanhando a branch remota, com alterações
   locais **preexistentes e alheias a OpenMetadata**. Esses arquivos não serão
   tocados.
5. Resolvidos os commits observados: MinC `main=4eec22a`, MinC
   `origin/feat/openmetadata-airflow3=2dc2d11` e Cidades
   `origin/fixture/ajustes-conjuntura=88a110a`. O teste
   `merge-base --is-ancestor` entre a branch histórica de OpenMetadata do MinC
   e `main` retornou 1, compatível com merge por squash/cherry-pick; portanto a
   equivalência deve ser conferida por conteúdo, não por ancestralidade.

**Cuidado de concorrência.** O worktree do Cidades está sujo em arquivos de
boletim/conjuntura. Toda comparação será feita contra refs Git quando possível,
e nenhuma frente desta passagem editará aquele repositório.

**Próximo passo imediato.** Comparar por conteúdo as refs atuais, validar os
testes e recipes prioritários e aprofundar o inventário SALIC sem depender de
VPN.

**Atualização às 17:40 -03.** Foi feito `git fetch` explícito das refs relevantes
nos dois repositórios. O `origin/main` do MinC avançou de `4eec22a` para
`daaae4c` (2 commits de reorganização de agentes, sem mudanças nos caminhos de
OpenMetadata comparados); o worktree local ficou 2 commits atrás e **não foi
atualizado**, para preservar as mudanças locais. No Cidades, a fixture segue em
`88a110a`. A inspeção de `git show 88a110a` confirmou a mitigação descrita na
§6 e um teste novo em `tests/test_openmetadata_recipes.py`. Nenhum valor de
credencial foi copiado para esta memória.

**Validações locais adicionais.**

6. Uma primeira tentativa de inventário por blob falhou imediatamente porque
   o script usou `status` como variável no zsh (`read-only variable: status`).
   Nenhum arquivo foi alterado. A repetição usou `comparison_state` e concluiu
   que `rendering.py`, `runner.py`, `workflows.py`, `dbt_artifacts.py` e
   `__init__.py` são idênticos por blob nas refs atuais. A classificação dessa
   repetição para arquivos ausentes não é confiável, porque `git rev-parse`
   pode imprimir a expressão não resolvida mesmo retornando erro; a checagem
   posterior com `git show` confirmou que `lineage.py` continua exclusivo do
   MinC. Registrar a falha evita que outra IA confie no inventário bruto.
7. Diffs por conteúdo confirmaram: `glossary.py` difere no `str()` para o type
   checker; `semantic_relationships.py` difere apenas em comentários e no fato
   de o Cidades não guardar um retorno não usado; `lineage.py` não existe na
   fixture do Cidades.
8. Conferência direta das recipes confirmou `markDeletedTables: false` no
   Cidades e ausência no MinC. Conferência dos `config.py` confirmou as flags
   `OM_INGEST_POSTGRES`/`OM_INGEST_DBT` somente no Cidades. A DAG do Cidades
   liga glossário, relações semânticas e reaplicação de governança; a do MinC
   não contém essas etapas.
9. Versões reconfirmadas nas refs: MinC usa Airflow `3.2.2`,
   `openmetadata-ingestion==1.13.3.2` e SQLAlchemy `2.0.52`; Cidades usa imagem
   Airflow `2.8.1` e documenta o isolamento do pacote OM por incompatibilidade
   com SQLAlchemy 1.4.
10. Testes focados no Cidades:
    `poetry run pytest -q tests/test_openmetadata_recipes.py
    tests/test_governanca_openmetadata.py` → **69 passed em 0,54 s**.
11. A tentativa nativa no MinC com `poetry run pytest ...` não executou testes:
    o Poetry criou um virtualenv vazio no cache do usuário e respondeu
    `Command not found: pytest`. Para não instalar dependências nem alterar o
    lock, a suíte foi executada com o Python do virtualenv já existente do
    Cidades, mantendo o cwd do MinC: **25 passed, 3 skipped em 5,65 s**. Isto
    valida a lógica portátil, mas não substitui teste no ambiente Poetry próprio
    do MinC; os 3 skips são dependências opcionais ausentes.
12. `git status` após os testes confirmou que nenhum arquivo rastreado novo foi
    alterado em qualquer repositório. Permanecem apenas as mudanças
    preexistentes já listadas nesta entrada e esta memória.
13. `git diff` entre `origin/feat/openmetadata-airflow3` (`2dc2d11`) e o commit
    squash da `main` (`4eec22a`) não mostrou diferença nos caminhos da
    integração OM, testes, lock e metadados dbt selecionados. Logo, para essa
    investigação, a antiga branch do MinC e o conteúdo incorporado na `main`
    são equivalentes; não é necessário trocar o worktree para compará-la.
14. Consulta anônima e somente leitura a
    `GET /api/v1/system/version` da instância documentada respondeu HTTP 200
    com OpenMetadata **1.13.3**, compatível com o cliente `1.13.3.2` do MinC.
    A tentativa de listar `databaseServices` sem token respondeu HTTP 401.
    Portanto a saúde/versão é verificável nesta sessão, mas catálogo,
    credenciais persistidas e materialização SALIC exigem autenticação. Nenhum
    token foi usado ou lido.
15. Recontagem SALIC com `rg` confirmou 431 + 67 + 57 + 5 + 1 = **561**
    tabelas-source e zero SQL sob `salic_bronze`. A recontagem dos domínios de
    referência corrigiu um número antigo da §11: `agentes_dbt` tem 12 SQL em
    `origin/main` (e 23 no HEAD local anterior aos 2 commits recém-buscados),
    não 11. `cotas_dbt` segue com 24.
16. Uma tentativa de contar linhas dos testes do Cidades usou a forma
    `"$ref:caminho"`; o zsh interpretou os dois-pontos como modificador e o Git
    recebeu uma revisão corrompida. A repetição correta, com
    `"${ref}:caminho"`, confirmou 890 + 31 linhas e 68 + 1 funções de teste.
17. Uma IA independente comparou os helpers/DAG/refs e outra analisou a camada
    REST por `git archive` da fixture em diretório temporário. A segunda repetiu
    os 69 testes (69 passed em 0,61 s) sem checkout e sem editar repositórios.
    Os achados reconciliados estão nas correções de §1–§9 e na matriz §12.
18. `git ls-remote --heads origin` no Cidades confirmou no servidor a fixture
    em `88a110a` e ausência de `refs/heads/refactor/openmetadata`. A branch
    remota ativa ainda alcança `ca5e0f5` e `4e0461d`; os IDs anteriores à
    reescrita continuam relevantes para clones/reflogs, não como ref publicada
    observável hoje.
19. A IA da frente SALIC fez inventário por `find`/`rg`/`wc`, consultou o
    `manifest.json` com `jq`, leu os nove estágios e módulos do pipeline e
    verificou portas/containers locais. Nenhum banco/Trino estava escutando e
    nenhuma conexão live foi tentada. Ela identificou a silver existente, o
    drift 561 × 1.139, riscos de amostras/PII e o MVP de seis fontes consolidado
    em §11.
20. Estado final verificado às 17:47 -03: MinC continua em `main@4eec22a`, 2
    commits atrás de `origin/main`, apenas com `docs/GUIA.md` e
    `docs/openmetadata/` preexistentes/modificados; Cidades continua na fixture
    com apenas os arquivos de boletim/conjuntura preexistentes. Nenhuma IA
    alterou código de produção em qualquer repositório.
21. Uma busca de consistência escrita com backticks dentro de uma string de
    shell tentou executar acidentalmente o trecho textual `type: Postgres` e
    imprimiu `command not found`; nenhum dado/arquivo foi afetado. A busca foi
    refeita abaixo com regex entre aspas simples. Lição: nunca interpolar
    Markdown com backticks em comando shell com aspas duplas.

---

## 12. Matriz de portabilidade da governança declarativa

### Invariante de arquitetura

Os conectores Postgres/dbt do MinC continuam como **única autoridade
estrutural** para schemas, tabelas e colunas. A camada REST é um **overlay
idempotente e por campo**, sempre depois de `dbt_metadata`, para o que o
conector não expressa: produto, classificação de uso, certificação, restrições,
relações e projeção segura para RAG. Dois writers estruturais causam ping-pong e
podem apagar metadado um do outro.

### Portar primeiro (extraindo paths/config)

| Componente no Cidades | Parte reutilizável | Adaptação mínima |
|---|---|---|
| `auditar_estrategias_carga.py` | algoritmo puro sobre `manifest.json` | project/output dirs e chave `meta.governance` |
| `auditar_metadados.py` | auditoria read-only YAML/manifest | usar `unique_id`, não nome simples; regras configuráveis |
| `restricoes_dbt.py` | `unique + not_null` → PK | models dir; testes compostos/model-level |
| `governanca_comum.py` | normalização, referências, JSON diff e três modos | cliente/config por contrato; erros HTTP e paginação |

### Portar com adaptação obrigatória

| Componente | Motivo da adaptação |
|---|---|
| `exportar_catalogo_openmetadata.py` | hoje só inclui `model`; precisa `source`, política de coluna allowlist/fail-closed, paths MinC e testes de privacidade |
| `gerar_catalogo_openmetadata_seguro.py` | hardcodes Poetry/MCID; reutilizar `helpers/openmetadata/dbt_artifacts.py`, que já trabalha em tmp |
| `linhagem_colunas.py` | parser atual cobre SQL simples/`ref()`, não `source()`, macros/Jinja complexo do SALIC; declarar `sqlglot` |
| `sincronizar_governanca.py` | separar namespace/propriedades `mcid*`, ownership de campos, glossário e granularidade conjuntura |
| `auditar_openmetadata.py` | paginação, FQNs/IDs exatos e dono/domínio/produto exatos; booleano “existe” não basta |
| `validar_silver_gx.py` | MinC ainda não tem contrato `silver_contract` nem GX; limitar scans e só adotar após modelos SALIC |
| `semantic_descriptions.py` | vocabulário é de indicadores/conjuntura; para RAG, falhar em modo strict em vez de inventar texto |

### Não copiar diretamente

- `sincronizar_openmetadata.py`: duplicaria o writer estrutural já coberto pelos
  conectores. Aproveitar apenas funções puras de tipo/comparação/lineage.
- `sincronizar_lake.py`: depende de convenções `meta.bucket/caminho/dag` e
  `fonte_lake()` ausentes no MinC; a limpeza atual pode remover `dbtTags.*` de
  outros órgãos em serviço compartilhado.
- `inventariar_colunas.py`: vocabulário MCID e full scans (`count(distinct)`) de
  custo e sensibilidade altos; apenas opt-in, amostrado e com saída protegida.
- YAMLs de domínios/termos/serviços do MCID: reutilizar o schema conceitual,
  nunca o conteúdo. O MinC já tem sua fonte de glossário; não criar uma segunda.
- scaffolding/testes de virtualenv do Airflow 2: o MinC Airflow 3 proíbe esse
  desenho e instala o cliente na imagem.

### Riscos que bloqueiam `--confirmar`

1. Operações do Cidades gerenciam raízes globais (`dbtTags`) e podem alterar
   outro tenant; toda mutação deve ser limitada a namespace/owner explícito.
2. Algumas escritas substituem `tags`, relações ou `extension` inteiras. Fazer
   patch por propriedade preservando extensões, como já faz
   `semantic_relationships.py` do MinC.
3. O default atual pode tratar tabela não declarada como Gold/certificável.
   Para MinC/SALIC, ausência de declaração é erro ou “não publicável”.
4. Denylist por substring não classifica PII contextual. Catálogo OM e corpus
   RAG são produtos separados: presença/topologia pode entrar no OM enquanto
   conteúdo/colunas só entram no RAG por allowlist e validação de segurança.
5. Listagens com `limit=1000` sem paginação já estão próximas do limite com 561
   sources; paginação é requisito, não melhoria futura.

### Evidência versionada do Cidades (não equivale a integração live)

Os artefatos da fixture declaram 140 models, 2.244 colunas e 156 arestas; 98
models aparecem elegíveis para RAG. Os testes offline passaram, mas ainda há 3
descrições de tabela e 29 de coluna derivadas por convenção, e não houve sync
autenticado nesta sessão. Portanto isso comprova geração/configuração, não o
estado atual da instância.

---

## 13. Plano de implementação — silvers SALIC e OpenMetadata

### 2026-09-02 · Codex · Levantamento para o plano (em curso)

**Objetivo.** Planejar a documentação/implementação das silvers que respondem
às Metas 3, 4 e 5 da Rouanet, usando todas as bronzes atuais e o mapa do FigJam,
e definir a documentação que deverá chegar ao OpenMetadata. Frente **F11**.

**Operações executadas até 14:45 -03:**

1. A skill de gerenciamento de plugins foi lida integralmente porque a fonte
   pedida é um board externo do Figma.
2. A busca pelas ferramentas disponíveis confirmou que o conector Figma não
   estava instalado. Foi sugerida e confirmada pelo usuário a instalação de
   `figma@openai-curated-remote`; depois da instalação, as ferramentas de
   leitura do FigJam ficaram disponíveis.
3. `get_figjam` leu `aV7NYMKnFiVe0LR78jcaub`, nó raiz `0:1`, incluindo imagens.
   O board contém sete fluxos: Meta 3 P1/P2/P3, Meta 4 P1/P2/P3 e Meta 5 P1.
4. Foram renderizados individualmente os nós-imagem `4:243`, `4:247`, `4:251`,
   `5:125`, `5:138`, `5:145` e `5:155`. As imagens temporárias foram baixadas
   em `/tmp/tmp.3Keht2QOy1/` e inspecionadas em resolução original. URLs
   temporárias do Figma não foram copiadas para esta memória.
5. O Google Doc público ligado pelo board não abriu pelo navegador integrado.
   A exportação pública `?export=txt` funcionou via `curl`; o documento foi
   salvo apenas em `/tmp/tmp.3Keht2QOy1/indicadores_perguntas.txt` e a seção
   Rouanet foi lida. Ela confirma prioridades, definições e lacunas do board.
6. Três agentes somente leitura foram disparados em paralelo: (a) modelos e
   joins silver por indicador; (b) contrato OpenMetadata; (c) bases externas,
   PII e qualidade. O arquivo temporário e as imagens foram compartilhados com
   eles; nenhum recebeu posse de arquivo para edição.
7. Estado Git: branch `feat/dbt-salic-silver`, HEAD e `origin/main` em
   `d404531`; mudanças preexistentes apenas em `docs/GUIA.md` e nesta pasta de
   memória.
8. `find` e diff desde `daaae4c` mostraram a mudança de estado decisiva desde a
   investigação anterior: a `main` agora contém **571 SQLs bronze SALIC**,
   organizados por `sac`, `tabelas`, `agentes`, `controledeacesso` e
   `bdcorporativo`, além de documentação e classificação PII.
9. Foram lidos integralmente o guia técnico recém-versionado
   `.claude/skills/bronze-salic-dbt/SKILL.md` e suas três referências de
   conexão, tipagem/casts e documentação. Embora não seja uma skill registrada
   nesta sessão, ele é a especificação vigente do repositório para o trabalho.

**Correção do §11.** As afirmações sobre “zero SQL em `salic_bronze`” descrevem
o snapshot de 2026-09-01 e devem ser lidas como histórico. Em 2026-09-02 a
bronze real está concluída com 571 views dbt; o plano abaixo será baseado nesse
estado, não no snapshot antigo.

### Resultado da leitura do FigJam

O board descreve sete perguntas, mas não sete silvers. As perguntas são
indicadores agregados; as entidades compartilhadas que permitem calculá-los
devem ser silvers conformadas. Os sete resultados ficam em golds/métricas e
serão registrados como `exposures` dbt do dashboard.

| Fluxo | Pergunta observada | Evento/medida candidata | Silvers necessárias | Estado atual |
|---|---|---|---|---|
| M3 P1 | percentual do recurso efetivamente pago a profissionais/contratados negros, indígenas, PCD ou de território vulnerável, em prestação de contas | pagamento/comprovante, `vlpago`, data do pagamento | projeto, pagamento, prestador/perfil temporal, geografia de residência | **bloqueado para diversidade**: pagamento detalhado tem substituto possível, mas mestre do agente e atributos demográficos não estão confirmados |
| M3 P2 | percentual captado por proponentes autodeclarados nos grupos, em projetos em execução | recibo de captação, `captacaoreal`, data do recibo | captação, projeto/status temporal, projeto-proponente, perfil temporal, residência | **parcial**: captação e ponte de projeto existem; perfil e semântica do status precisam de validação live |
| M3 P3 | comparar o realizado com 25%/10%/5% e alvo territorial | resultados agregados de P1/P2 + alvo vigente | dimensões/fatos anteriores e meta-alvo versionada | **bloqueado por regra**: território está como `x%`; os demais valores também precisam de fonte normativa e vigência |
| M4 P1 | percentual executado fora das capitais | valor pago/executado e local de execução | pagamento, local de execução, município/capital | **parcial**: abrangência existe; falta dimensão oficial de capitais e política para projeto multicidade |
| M4 P2 | percentual executado no Norte, Nordeste e Centro-Oeste | valor pago/executado e região do local | mesmas de M4 P1 | **parcial**: região aparece em view não verificada; IBGE deve ser a autoridade |
| M4 P3 | total captado por projetos executados em territórios vulneráveis/periféricos | o board mistura captação com execução | captação, locais, classificação territorial versionada | **bloqueado por definição**: medida, critério de classificação e regra multicidade não estão homologados |
| M5 P1 | percentual de proponentes ativos no ano — aprovados ou com captação — sem PRONAC anterior | projeto/aprovação/captação e histórico do proponente | projeto, aprovação, captação, identidade e primeiro acesso | **parcial**: fontes existem, mas “primeiro projeto”, “primeira aprovação” e “primeiro recebimento” são conceitos diferentes |

O documento público vinculado ao board confirma três cuidados: captação não é
pagamento; execução e prestação de contas não podem ser misturadas; e nem
sempre o proponente recebe o recurso. Por isso residência do proponente,
residência do prestador e local de execução são três papéis geográficos
distintos.

### Matriz das fontes e lacunas

O manifest atual contém 619 modelos e 653 sources. Há 571 bronzes SALIC: 475
SAC, 77 Tabelas, 15 Agentes, 3 Controle de Acesso e 1 BDCorporativo. As bronzes
têm 6.264 colunas descritas/tipadas e 423 colunas marcadas como PII, mas os 571
modelos ainda não têm o bloco completo de governança OpenMetadata no nível de
modelo. Os testes atuais não provam os caminhos de negócio do FigJam.

Fontes já presentes e candidatas:

- `sac__captacao`: recibo, data e valor de captação;
- `sac__aprovacao`: aprovação e publicação;
- `sac__abrangencia`: projeto e município/local de realização;
- `sac__tbplanilhaaprovacao`: planilha aprovada, item, agente, valores e local;
- `sac__vwagentesseusprojetos`: PRONAC, agente/proponente, situação e datas;
- `sac__vwpagamentodefornecedordoprojetoporitemdetalhado`: PRONAC,
  prestador, comprovante, data e valor pago;
- `sac__vwitenscomprovados_fonte_produto_etapa_uf_municipio`: item
  comprovado, valor e município;
- `agentes__vperfil`, `agentes__vverificacao`, `agentes__vuf` e
  `agentes__vufmunicipio`: úteis, mas descritas como `[NÃO VERIFICADO]`;
- sources legadas `sac__projetos` e `sac__tbapiprojetorouanet`: só podem ser
  usadas com reconciliação e plano de migração para a v2.

Objetos centrais desenhados no FigJam e ausentes do recorte ingerido incluem
`Projetos` na v2, `PreProjeto`, `Situacao`, `tbApiIncentivos`,
`tbApiComprovacoes`, `Agentes`, `tbAgenteFisico`, `EnderecoNacional`,
`tbCompPagXPlanilha`, `tbComprovantePagamento`,
`tbEncaminhamentoPrestContas`, `tbSituacaoEncPrestContas`,
`tbAlteracaoNomeProponente`, `Municipios` e `PopulacaoMunicipio`.
BDCorporativo contém hoje somente `sysdiagrams`. A auditoria live deve procurar
também `VIEW` e `SYNONYM`, não apenas `BASE TABLE`, e registrar banco, schema,
casing, permissões, grão, chave e cobertura.

Não existem no repositório bases versionadas de capitais, vulnerabilidade e
metas-alvo. A população municipal do SIDRA não é carregada pelos defaults
atuais: IDs de resultados estão vazios e o nível configurado é UF, não
município. As tabelas de períodos/variáveis/localidades/resultados ainda fazem
append sem upsert. População não é dependência de M4 enquanto a fórmula não for
per capita.

### Decisão arquitetural do plano

O fluxo alvo é:

```text
sources/raw -> salic_bronze tipada -> silvers conformadas/restritas
            -> golds dos 7 indicadores -> exposures do dashboard
            -> Postgres metadata -> dbt metadata -> overlay de governança
            -> exportador RAG fail-closed
```

As silvers guardam grão e semântica reutilizáveis. Percentuais, comparações com
alvo e cortes anuais ficam em gold. Um KPI não será implementado diretamente
sobre uma view agregada da origem quando for possível preservar o fato
auditável.

O pacote lógico será `dbt/minc/models/salic_dbt/`, separado por `core`,
`meta3`, `meta4`, `meta5` e `gold`. O schema físico ainda é uma decisão **A0**:
o default coerente com o repositório é `salic`, pois as camadas são separadas
por pasta e as recipes já incluem esse schema. Se a autorização live exigir
fronteira física, separar ativos restritos/publicáveis antes de materializar e
atualizar em conjunto `dbt_project.yml`, as três recipes e os testes de filtro.
Não adotar `salic_silver` isoladamente sem esse ADR, pois mudaria FQNs e
governança.

### Contratos silver propostos

Os nomes são provisórios até o gate A0, mas os grãos não podem ser alterados
silenciosamente durante a implementação.

| Modelo | Grão e finalidade | Origem principal | Sensibilidade / RAG |
|---|---|---|---|
| `dim_projeto_rouanet` | 1 linha por PRONAC; situação, ciclo, áreas, datas e valores autorizados | projetos legado + `sac__vwagentesseusprojetos` + aprovação, com fallback auditável | retirar identificadores pessoais da projeção elegível ao RAG |
| `brg_projeto_proponente_rouanet` | 1 PRONAC × proponente × vigência; separa projeto de identidade | projetos/agentes | restrito; nunca corpus RAG |
| `fct_captacao_rouanet` | 1 linha por recibo de captação | `sac__captacao` | sem mecenas/documentos pode ser elegível após validação |
| `fct_aprovacao_rouanet` | 1 aprovação/publicação/versionamento | `sac__aprovacao` | elegível somente sem identificadores pessoais |
| `dim_agente_perfil_rouanet_scd` | 1 agente × início de vigência; autodeclaração e qualidade da origem | mestre Agentes + perfil/verificação, a confirmar | PII sensível, restrito e proibido para RAG |
| `fct_pagamento_profissional_rouanet` | 1 pagamento × comprovante × item × prestador | view detalhada de pagamentos; validar contra BDCorporativo | PII sensível, restrito e proibido para RAG |
| `dim_municipio_ibge` | 1 código IBGE municipal oficial de 7 dígitos; UF, região, capital e versão | IBGE + base oficial de capitais | publicável |
| `brg_projeto_local_execucao` | 1 PRONAC × município × período/papel do local | `sac__abrangencia` | publicável quando restrito a município |
| `brg_territorio_classificacao` | território × granularidade × critério × fonte/versão/vigência | fonte externa aprovada | publicável; desconhecido não vira `false` |
| `fct_execucao_municipal_rouanet` | 1 pagamento/item × local após deduplicação ou rateio | pagamento + item comprovado + local | detalhe restrito; projeção agregada pode ser pública |
| `fct_evento_acesso_rouanet` | 1 evento qualificável: registro, aprovação ou captação | projeto + aprovação + captação | sem documento claro; identidade técnica restrita |
| `fct_proponente_ano_rouanet` | 1 proponente × ano, preservando flags de aprovado/captou e primeiras datas distintas | evento de acesso + identidade | restrito e proibido para RAG |
| `dim_meta_alvo_rouanet` | política × meta × indicador × grupo × início de vigência | seed/configuração com base legal | publicável; alvo ausente = `sem_alvo`, nunca zero |

O perfil deve ser unido ao evento pela vigência (`as of`). Se a origem tiver
somente o snapshot atual, o modelo marca `perfil_nao_historico` e não atribui
retroativamente uma condição ao passado. PJ não herda raça/PCD de sócio ou
responsável sem regra aprovada. Alteração de nome não cria novo agente.

Para identidade entre bases, o documento claro permanece apenas na zona
restrita. A chave de integração será HMAC-SHA256 com segredo gerenciado; hash
simples de CPF/CNPJ é enumerável. A chave HMAC continua sendo dado linkável e
deve ser classificada como sensível.

### Golds e exposures do painel

Após as silvers e os gates de negócio, criar um gold por pergunta:

1. `kpi_meta3_pagamentos_diversidade`;
2. `kpi_meta3_captacao_diversidade`;
3. `kpi_meta3_comparacao_alvos`;
4. `kpi_meta4_execucao_fora_capitais`;
5. `kpi_meta4_execucao_regioes_prioritarias`;
6. `kpi_meta4_captacao_territorios_vulneraveis`;
7. `kpi_meta5_primeiro_acesso_rouanet`.

Cada gold terá um `exposure` do tipo `dashboard`, com owner, maturidade, URL do
board/nó, descrição da fórmula e `depends_on`. O FigJam é requisito e
documentação; não é source de dados.

Todo resultado publica, além do numerador/denominador/percentual: universo,
ano/data de referência, unidade monetária, cobertura do join, quantidade não
classificada, regra de desconhecidos, versão da regra e data de processamento.
Resultados demográficos públicos devem suprimir células pequenas; o limiar
inicial sugerido é `k < 10`, mas depende de aprovação do DPO e não será
codificado como política definitiva sem ela.

### Gate A0 — decisões de negócio que bloqueiam os KPIs

Registrar uma ADR/matriz assinada para cada um dos sete indicadores com:

1. `tipo_valor`: captado, comprovado, pago/liquidado ou autorizado;
2. evento e data que definem o ano de referência;
3. numerador, denominador, universo, inclusões e exclusões;
4. papel da geografia: residência do proponente, residência do prestador ou
   local de execução;
5. regra de projetos multicidade e prova de que pesos somam 1;
6. taxonomia temporal de “em execução” e “prestação de contas”;
7. tratamento de ausente/desconhecido;
8. definição, granularidade, fonte, versão e vigência de vulnerabilidade;
9. base legal e vigência das metas-alvo;
10. definição e horizonte histórico de primeiro acesso;
11. nível de publicação, PII e supressão estatística.

Regras conservadoras até aprovação:

- numerador e denominador usam a mesma medida;
- localizar a despesa no nível do pagamento/item é preferível;
- sem localização financeira, usar rateio aprovado cuja soma seja 1 ou
  publicar apenas cobertura/contagem, nunca replicar 100% por município;
- FCU/concentração urbana permanece atributo próprio e nunca vira sinônimo de
  vulnerabilidade/periferia;
- código IBGE incompleto/inexistente vai para quarentena; não usar `zfill(7)`
  para fabricar um município;
- “primeiro projeto”, “primeira aprovação” e “primeira captação” permanecem
  colunas separadas;
- o `primeiro_acesso_anual` existente compara outro universo e não será
  substituído silenciosamente.

### Plano por ondas e divisão entre agentes

Há quatro slots de trabalho. A divisão abaixo evita dois agentes editando o
mesmo arquivo e permite handoff entre IAs.

| Papel | Posse principal | Entregas | Dependências |
|---|---|---|---|
| **Agente A — núcleo SALIC/Figma** | SQL em `salic_dbt/core`, `meta5` e golds correspondentes | macro PRONAC, projeto, captação, aprovação, eventos e primeiro acesso; matriz fonte/substituto; reconciliação com `eventos_fomento_rouanet` | A0 e auditoria das fontes centrais |
| **Agente B — identidade/perfil e Meta 3** | SQL restrito de identidade, perfil e pagamento; golds M3 | ponte temporal de agente, fato de pagamento, P1/P2/P3, cobertura de perfil e seed de alvo homologado | núcleo do A; mestre de Agentes/BDCorporativo; regra PII/alvos |
| **Agente C — dados externos, geografia e Meta 4** | ingestão/normalização IBGE/capitais/vulnerabilidade e SQL territorial | município oficial, capitais, classificação versionada, bridge de execução, M4 P1/P2/P3, idempotência e quarentena | A0; fontes externas aprovadas; fato financeiro do A/B |
| **Agente D — OpenMetadata/QA** | `schema.yml`, exposures, glossário/governança, testes de contrato e auditoria | documentação incremental de todos os modelos/colunas, classificação PII, recipes/overlay, artifacts, exporter RAG e auditoria live | nomes/grãos congelados; fecha somente após reconciliação |
| **Integrador/root** | ADRs, `dbt_project.yml`, recipes/config compartilhados, integração e esta memória | arbitrar A0, proteger arquivos comuns, revisar diffs, ordenar merges e executar os gates finais | recebe entregas A–D |

Distribuição dos agentes usados nesta investigação:

- `plano_silvers_figma`: derivou modelos, joins, grãos e testes por fluxo;
- `plano_openmetadata_silvers`: derivou o contrato de governança, ingestão,
  glossário, RAG e auditoria;
- `plano_dados_externos_qa`: auditou lacunas de fontes, IBGE/FCU, identidade,
  PII, idempotência e reconciliações;
- Codex/root: inspecionou o FigJam e documento associado, reconciliou os três
  handoffs com o estado Git e é responsável pela integração.

#### Onda 0 — congelar contrato e inventário

- aprovar A0 e o schema físico;
- auditar no banco todas as fontes do FigJam, incluindo views/synonyms;
- marcar cada fonte como `disponível`, `substituto validado` ou `bloqueada`;
- medir grão, chaves, cobertura e série histórica com consultas agregadas;
- congelar nomes, grãos, chaves e vocabulário dos modelos;
- criar stubs de documentação como `Disabled`, sem
  `Certification.Silver`, para ativos ainda bloqueados.

**Aceite:** nenhum SQL referencia relação não ingerida; cada KPI tem fórmula
sem mistura de medidas; há ADR para identidade, território, multicidade e
primeiro acesso.

#### Onda 1 — fundações e testes de contrato

- A implementa projeto, ponte proponente, captação, aprovação e evento de
  acesso;
- C torna ingestões externas mínimas idempotentes e implementa município e
  capitais; população fica fora do caminho crítico;
- D cria template/linter que exige documentação completa e corrige o mapeamento
  de `salic_bronze`/`salic_dbt` para `Cultura.Incentivo Fiscal` no anotador;
- root configura o pacote físico somente após o ADR e protege
  `markDeletedTables: false` antes de qualquer ingestão live.

**Aceite:** chaves e relacionamentos passam; captação v2 reconcilia por
ano/PRONAC com a fonte e, no período comum, com o silver legado; rerun de fonte
externa não duplica; exatamente 27 capitais, uma por UF/DF.

#### Onda 2 — verticais em paralelo

- B implementa perfil temporal/pagamento e M3, mantendo P1 bloqueado caso a
  ponte prestador-perfil não seja comprovada;
- C implementa locais, vulnerabilidade aprovada e M4, sem multiplicar valores;
- A implementa histórico e M5 com as três primeiras datas separadas;
- D documenta cada entrega aceita, não apenas ao final.

**Aceite:** `count(distinct id_evento)` e soma financeira são invariantes antes
e depois de joins; peso multicidade soma 1; numerador não supera denominador;
percentuais ficam no domínio; cobertura/desconhecidos são visíveis.

#### Onda 3 — golds, governança e RAG

- criar os sete golds e exposures;
- sincronizar classificações, Custom Properties, domínio, owner e glossário;
- materializar, gerar artifacts e ingerir no OpenMetadata na ordem definida;
- aplicar overlay, classificador sem amostra, profiler seguro e exportador RAG;
- auditar idempotência e o catálogo final.

**Aceite:** as sete fórmulas passam casos sintéticos e reconciliação real;
lineage vai de source/bronze até exposure; nenhuma amostra/estatística
reidentificável é persistida; o export RAG contém somente modelos e colunas
explicitamente allowlisted.

### Contrato de documentação dbt/OpenMetadata

Cada modelo silver terá, no `schema.yml`:

- descrição com grão, chave, propósito, universo, inclusão/exclusão, janela
  temporal, medida/unidade, joins, tratamento de nulos/desconhecidos,
  limitações, cobertura e atualização;
- `config.tags`: `silver`, `salic`, `rouanet` e `meta3|meta4|meta5` conforme o
  uso;
- `config.meta.status: Disabled` enquanto bloqueado e `Active` após gates;
- `config.meta.openmetadata.domain: Cultura.Incentivo Fiscal`;
- `tier: Tier.Tier2`, `owner: minc-data-engineering` e, somente após aceite,
  `Certification.Silver`;
- termos de glossário existentes e os novos somente depois de sincronizados;
- Custom Properties apenas se já existirem na entidade `Table`; propriedade
  desconhecida não pode ser declarada como se tivesse sido ingerida;
- contrato local `meta.governance` para produto, classificação e
  `rag_publication`, lido pelo auditor/exportador, com ausência significando
  `prohibited`.

Cada coluna final terá:

- descrição semântica, origem/transformação relevante, unidade e moeda,
  timezone/data de referência, domínio de valores, sentinelas/nulos e limites;
- `data_type` explícito;
- testes de `not_null`, unicidade simples/composta, `relationships`, domínio,
  intervalo e regra temporal conforme o papel;
- glossário para PRONAC, código IBGE, proponente e demais conceitos comuns;
- classificação manual de PII, inclusive chaves técnicas, HMAC, raça/cor,
  pertencimento indígena, PCD, endereço/CEP, telefone/e-mail, conta/agência,
  usuário/logon e IP.

Novos termos candidatos: `MinC.Identificadores.PRONAC`, raiz
`MinC.IncentivoFiscal`, `ProjetoCultural`, `CaptacaoEfetiva`,
`ExecucaoFinanceira`, `PrestacaoDeContas`, `LocalDeExecucao`,
`ProfissionalContratado` e `CoberturaHistorica`. Antes de criá-los, revisar o
termo atual de território vulnerabilizado, que hoje herda uma semântica de FCU
incompatível com a Rouanet.

`meta.governance` é um contrato do repositório, não algo que o conector dbt
ingere automaticamente. O bloco reconhecido pelo conector continua sendo
`config.meta.openmetadata`; `status`, classificação `Uso`, certificação
confirmada, produto e propriedades fora do suporte exigem overlay REST.

### Política RAG e PII

A publicação é fail-closed:

- ausência de allowlist em modelo **ou coluna** significa `prohibited`;
- qualquer tag `PII.Sensitive` ou `PII.NonSensitive` vence a allowlist;
- fatos de pagamento, proponentes/perfis e primeiro acesso são totalmente
  proibidos;
- projetos, captações, locais e golds agregados só entram após validação de
  segurança e supressão;
- não exportar SQL compilado, amostras, perfil estatístico, caminhos físicos,
  valores de enumeração obtidos do dado nem o nome de colunas excluídas;
- descrições com exemplos reais, documentos, endereços ou caminhos fazem o
  export falhar.

`storeSampleData: false` já protege o classifier, mas não torna o profiler
seguro. O profiler precisa excluir tabelas/colunas sensíveis e a auditoria via
API deve confirmar ausência de min/max, distribuição ou valores reveladores.

### Ordem da ingestão OpenMetadata

1. sincronizar classificações, Custom Properties, domínio, produto, owner e
   glossário;
2. executar `dbt build` das silvers/golds;
3. gerar `manifest.json`, `catalog.json` e `run_results.json`;
4. rodar `postgres_metadata`, que cria/atualiza os ativos físicos;
5. rodar `dbt_metadata`, que anexa descrições, tags, testes e linhagem;
6. executar o overlay por propriedade, reaplicando governança e certificação;
7. rodar classifier com `storeSampleData: false`;
8. rodar profiler somente com exclusões PII verificadas;
9. exportar o corpus RAG por allowlist;
10. auditar a instância de forma exata, paginada e idempotente.

O conector dbt atual usa `dbtUpdateDescriptions: true` e `includeTags: true`, e
pode limpar certificação; por isso o overlay fica depois dele. Modelos não
materializados não aparecem: documentação YAML e manifest sozinhos não criam
ativos no catálogo.

### Testes e gates de publicação

Criar uma suíte dedicada, por exemplo
`tests/test_salic_silver_governance.py`, que verifique offline:

1. todo `.sql` de silver/gold aparece em exatamente um `schema.yml`;
2. toda coluna final tem descrição efetiva e `data_type`;
3. nenhuma descrição contém `[NÃO VERIFICADO]`;
4. todo modelo tem owner, domínio, tier, status e política RAG exatos;
5. `Certification.Silver` só existe em modelo aprovado;
6. todas as PII conhecidas estão marcadas, inclusive chaves/HMAC;
7. nenhuma coluna elegível ao RAG carrega PII;
8. FQNs de glossário e Custom Properties existem nas fontes declarativas;
9. o mapeamento automático nunca envia `salic*` para
   `Cultura.Fomento Direto`;
10. recipes derivam e incluem todos os schemas dbt necessários;
11. constraints representam PKs simples/compostas sem duplicação;
12. o exportador omite, em casos adversariais, CPF/CNPJ, nome, endereço,
    raça, deficiência, conta, agência, logon e IP.

Gates live obrigatórios:

- **G0:** A0 e matriz de fontes aprovados;
- **G1:** carga completa/fresca, idempotente e reconciliada;
- **G2:** grãos, PK/FK, temporalidade, PII e fan-out passam;
- **G3:** totais financeiros e contagens reconciliam por ano;
- **G4:** cobertura mínima homologada e desconhecidos publicados;
- **G5:** OpenMetadata completo, lineage/exposures corretos e RAG seguro.

Falha em qualquer gate mantém o ativo `Disabled`/provisório, sem
`Certification.Silver` e fora do dashboard certificado.

### Operações finais desta investigação

Após os três handoffs, foram consolidados: (a) o mapa por indicador; (b) a
arquitetura silver/gold; (c) 13 contratos canônicos; (d) a divisão entre quatro
papéis; (e) ondas 0–3; (f) contrato OpenMetadata/RAG; e (g) gates G0–G5.

Nenhum SQL dbt, DAG, recipe ou estado externo foi alterado. Não houve acesso ao
banco, porque o `.env` não está presente neste worktree; grãos e coberturas
marcados como live permanecem hipóteses a verificar com VPN/credenciais. A
única alteração versionável desta etapa é este documento de memória.

---

## 14. Núcleo silver do SALIC — entrega da F12

### 2026-09-02 · Claude · Onda 1 implementada

**O que foi entregue.** O papel do *Agente A* do plano (§13), na parte que não
depende do gate A0: seis modelos silver em `dbt/minc/models/salic_dbt/core/`,
mais as macros de chave, a documentação completa e a suíte de guardas.

| Modelo | Grão | Política de RAG |
|---|---|---|
| `map_chave_projeto_rouanet` | 1 PRONAC | elegível após validação |
| `dim_projeto_rouanet` | 1 PRONAC | elegível após validação |
| `brg_projeto_proponente_rouanet` | 1 PRONAC × agente | **prohibited** (zona restrita) |
| `fct_captacao_rouanet` | 1 recibo de captação | elegível após validação |
| `fct_aprovacao_rouanet` | 1 registro de aprovação | elegível após validação |
| `fct_evento_acesso_rouanet` | 1 evento datado (aprovação ou captação) | elegível após validação |

Arquivos novos: `dbt/minc/macros/salic/chaves_salic.sql` (5 macros),
`dbt/minc/models/salic_dbt/core/*.sql` (6),
`dbt/minc/models/salic_dbt/core/schema.yml` e
`tests/test_salic_silver_governance.py`. Alterados: `dbt/minc/dbt_project.yml`
(bloco `salic_dbt`), `helpers/openmetadata/glossaries/minc.csv` (4 termos) e
`dbt/minc/manifest.json` (regerado, 625 modelos).

**Decisões tomadas dentro do espaço que o plano deixou aberto.**

1. **Schema físico `salic`, pasta `salic_dbt`** — o default coerente que a §13
   já apontava. As recipes de OpenMetadata **não precisaram mudar**: `^salic$`
   já consta das três, e `test_scoped_recipes_share_the_same_schema_filter`
   deriva a lista do `dbt_project.yml`, então ela continua batendo.
2. **Um modelo a mais do que os 13 contratos do plano:
   `map_chave_projeto_rouanet`.** Ele não estava previsto e é o que destrava a
   Meta 4: `sac__abrangencia` só tem `idprojeto`, e nenhuma tabela da bronze v2
   liga as três chaves. Ele resolve os pares observando sete tabelas e é
   **fail-closed** — PRONAC com mais de um `idpronac` sai com o id NULL e a flag
   `id_pronac_ambiguo` em `true`, em vez de um `min()` que escolhe sozinho.
3. **`fct_evento_acesso_rouanet` nasce com dois tipos, não três.** O plano
   previa registro, aprovação e captação; a data de registro está em
   `PreProjeto`/`Projetos` da v2, que não foram ingeridas. Derivá-la de
   `dtsituacao` seria errado — aquilo é a última mudança de situação. Está
   documentado no modelo: o que existe hoje mede primeiro acesso **a recurso**.
4. **O documento do mecenas não entra em `fct_captacao_rouanet`.** No lugar,
   `tipo_pessoa_mecenas`, derivado só do comprimento. É o que mantém o fato
   candidato a publicação; com o CPF/CNPJ dentro ele seria restrito como a
   ponte. Mesma lógica descartou `logon` e `resumoaprovacao` da aprovação —
   texto livre de analista é o vetor por onde PII entra sem classificação.
5. **A dispensa de classificação de PII é declarativa.** O guarda por nome
   acusou `documento_valido` (booleano derivado, que não identifica ninguém). A
   exceção ficou em `meta.governance.derivadas_nao_identificantes`, no
   `schema.yml`, e não na regex do teste — assim ela aparece no diff do modelo,
   e o teste ainda exige que a coluna dispensada exista.
6. **Todos os seis nascem `status: Disabled`, sem `Certification.Silver`.** É a
   regra do plano: gate não verificado, ativo não certificado. O teste
   `test_certificacao_silver_so_em_modelo_ativo` impede que um dos dois mude
   sem o outro.

**Verificado nesta sessão.** `dbt parse` e `dbt compile --select salic_dbt`
passam (625 modelos, 5.548 testes, 653 sources); `sqlfluff` com a config da CI
dá **0 violações** nos sete arquivos novos — para comparação, os cinco silvers
de `agentes_dbt` acumulam 326; `sqlfmt --check` passa; a suíte inteira dá
**155 passed, 3 skipped**, incluindo os 68 casos novos de governança. As
colunas documentadas foram conferidas uma a uma contra o SQL compilado: 25/25
na dimensão, 9/9 na ponte, 8/8 no evento, e as demais batem.

**NÃO verificado, e é o que falta.** Nada rodou contra banco — não há `.env`
neste worktree e nenhum container no ar. Portanto continuam hipóteses:

- o grão de `sac__vwalterarprojeto` (o `distinct on` **garante** o grão
  declarado, mas não diz quanto ele descarta);
- a cobertura de cadastro da dimensão e a taxa de PRONAC sem cadastro;
- a cobertura e a ambiguidade de `map_chave_projeto_rouanet` — o número que
  decide se a Meta 4 é viável pela abrangência;
- que todo agente de `sac__vwagentesseusprojetos` seja proponente, e não outro
  papel;
- a reconciliação por ano e PRONAC com `eventos_fomento_rouanet`, que lê a
  bronze v1 no schema `bronze` (gate G3). Os dois convivem; nenhum substitui o
  outro ainda.

**Armadilha nova, registrada em §7:** o `lpad` do Postgres trunca em silêncio;
o `manifest.json` versionado precisa nascer do dbt 1.10, e um `dbt` 1.12 de
outro ambiente gera artefato que o guarda rejeita falando de versão, não do
modelo novo.

**Nota de ambiente.** `poetry run pytest` continua não funcionando neste
worktree: o Poetry cria um virtualenv vazio (`lappis-tmFU8tI0-py3.11`) e
responde `Command not found`. A suíte rodou com o Python do virtualenv do
Cidades (`lappis-CmPlItMs-py3.11`), mantendo o cwd aqui — o mesmo contorno que
a entrada de 2026-09-01 do Codex registrou. Consequência: `black` e `ruff` de
lá são muito mais novos que os do lock daqui e reprovam 56 arquivos e 845
regras **que ninguém tocou nesta sessão**. Os arquivos novos passam nos dois.
Isso é ruído de ambiente, não regressão — mas confirme com o ambiente próprio
antes de abrir PR.

**Próximo passo.** Gate A0 e a auditoria live das fontes (Onda 0 do plano). Com
banco, as três primeiras medições da lista acima decidem se a Meta 4 sai pela
abrangência ou precisa da view de itens comprovados. As frentes F13, F14 e F15
de §9 já estão abertas para quem assumir.

---

## 15. Ingestão OpenMetadata correta e completa — F2, F3 e F4

### 2026-09-02 · Claude · As três frentes verdes do MinC, fechadas

**Por que estas três, e nesta ordem.** A F3 deixou de ser opcional no momento em
que a §14 entregou seis `schema.yml` referenciando `MinC.Identificadores.PRONAC`
e mais três termos novos: sem alguém chamando `sync_glossary`, essas referências
apontam para termo que não existe no servidor — e o OpenMetadata **não reclama**,
ele descarta o vínculo em silêncio. A F2 é pré-requisito de qualquer execução
live, como a Onda 1 do plano já dizia. A F4 fecha a lacuna de flags e, de
quebra, resolve uma divergência que estava anotada como "inofensiva".

**F2 — `markDeletedTables: false`.** Uma linha em `postgres_metadata.yaml`, com
o porquê no arquivo, e **duas** guardas: uma exige a chave lá, a outra proíbe a
chave nas outras cinco recipes. A segunda existe porque declarar em três lugares
daria a impressão de que há três pontos a proteger, e no dia em que um fosse
esquecido ninguém saberia qual importava — profiler e classifier não são
`DatabaseMetadata` e não apagam nada.

**F3 — glossário como primeira task da corrente.** `sync_glossary` entra antes
de todas as recipes, com flag `OM_INGEST_GLOSSARY` (default ligado). O
`semantic_relationships.py` **ficou de fora de propósito**: ele valida
`kind: MCIDSemanticRelationshipCatalog`, formato do Ministério das Cidades, e
não existe catálogo desse tipo aqui. A própria frente já registrava que ligar só
o glossário resolve o problema real; criar um catálogo de 1.255 linhas para o
MinC é outra decisão, não um efeito colateral desta.

**F4 — flags, e uma decisão de segurança dentro delas.** Entraram
`OM_INGEST_POSTGRES`, `OM_INGEST_DBT` e `OM_INGEST_GLOSSARY`, ligados por
padrão. A divergência anotada em §9 (config.py com default `False`, compose com
`true`) foi resolvida **separando os dois casos em vez de escolher um lado**:

- **classifier alinhado para `true`** nos dois — ele roda com
  `storeSampleData: false` e nunca persiste linha bruta;
- **profiler alinhado para `false`** nos dois — ele publica min, max e
  distribuição, que são estatísticas reveladoras num banco com CPF, CNPJ e
  dados de raça e deficiência. A §11 já dizia que o profiler não é seguro sem
  exclusões verificadas; deixá-lo ligado por default contradizia isso.

**Isto muda comportamento local:** quem sobe o compose hoje deixa de rodar o
profiler. Volta com `OM_INGEST_PROFILER=true` no `.env`, e a intenção fica
registrada onde alguém a lê.

**Guardas novas (10 casos em `test_openmetadata_packaging.py`).** Além das duas
do `markDeletedTables`: as duas recipes de catálogo ligadas por padrão, cada uma
isolável por flag, o par profiler/classifier com os defaults acima, o glossário
como cabeça da corrente (por AST — importar a DAG exigiria Airflow configurado)
e, a mais útil no médio prazo, **flag declarada no `config.py` tem que chegar ao
container pelo compose**. Flag que o código lê e o compose não passa é flag
morta: o default do código vence e mexer no `.env` não muda nada.

**Verificado.** Suíte inteira: **165 passed, 3 skipped** (eram 155). `black`,
`ruff` e validação de YAML passam nos arquivos tocados.

**NÃO verificado.** A DAG **não foi parseada de verdade**: o Airflow disponível
nesta máquina é o 2.8.1 do virtualenv do Cidades, e este repositório é Airflow
3.2 (`airflow.sdk`). A guarda do encadeamento é sintática. O parse real acontece
quando o container subir — é o primeiro a conferir. E nada rodou contra a
instância: o glossário nunca foi sincronizado por DAG aqui, o que traz o risco
de `DisplayName-Deny` registrado em §7.

**Próximo passo.** F16 (unificar `salic_bronze` e `salic_dbt`) é a única frente
grande ainda offline. As Metas 3, 4 e 5 (F13–F15) continuam esperando o gate A0
e a auditoria live.

---

## 16. Um domínio dbt só para o SALIC — entrega da F16

### 2026-09-02 · Claude · `salic_bronze` vira `salic_dbt/bronze`

`git mv` dos 571 modelos e 6 `sources_*.yml`, e os dois blocos do
`dbt_project.yml` colapsados em um. O SALIC deixa de ser o único domínio do
projeto com duas pastas de topo: agora é uma pasta por domínio com as camadas
como subpasta, igual a `agentes_dbt` e `cotas_dbt`.

**Configuração preservada e conferida no manifest**, não presumida:

| Camada | Modelos | `materialized` | `schema` |
|---|---|---|---|
| `salic_dbt/bronze` | 571 | `view` | `salic` |
| `salic_dbt/core` | 6 | `table` | `salic` |

**Nenhum FQN do OpenMetadata muda**, que era a premissa da decisão: o catálogo
é `database.schema.tabela`, e nem o schema (`salic`) nem o nome das relações se
mexeram. Não há reingestão a fazer, e as três recipes seguem intocadas.

**Onze arquivos fora do dbt citavam o caminho antigo** e foram corrigidos no
mesmo commit, que é exatamente o critério do `CLAUDE.md` para a skill morar
aqui: a skill `bronze-salic-dbt` (SKILL.md, três scripts e uma referência),
`plugins/trino_bronze.py`, três scripts de geração, dois estágios de
`scripts/salic_docs/`, a docstring de `salic_ingestion_trino` e os cinco
catálogos do Trino. As menções ao caminho antigo **no diário desta memória
ficaram como estão** — são observação datada, e §0 proíbe reescrevê-las.

**A unificação quebrou um guarda meu, e isso foi útil.**
`tests/test_salic_silver_governance.py` varria `models/salic_dbt` inteiro; com a
bronze lá dentro, os 571 modelos gerados passaram a ser cobrados pelo contrato
da silver — 1.549 falhas. O escopo agora exclui **por camada**
(`CAMADAS_NAO_SILVER`), e não por lista de pastas, de modo que `meta3`, `meta4`
e `gold` entrem sozinhas quando existirem. A suíte voltou a 165 passed.

**Acervo do site recoletado** (`make docs-collect`), como o `CLAUDE.md` exige.
O diff é grande porque a última coleta era anterior às bronzes do SALIC e das
APIs; os seis modelos novos aparecem com o caminho novo.

---

## 17. O que falta para a silver do SALIC estar completa

*(levantamento pedido em 2026-09-02; é a foto de hoje, não um plano novo — o
plano continua sendo §13)*

### 17.1 Modelos: 6 de 14

Dos 13 contratos canônicos de §13, **5 estão implementados**; o
`map_chave_projeto_rouanet` foi acrescentado no caminho, então são 6 modelos
existentes e **8 faltando**.

| # | Modelo que falta | O que o bloqueia | Natureza do bloqueio |
|---|---|---|---|
| 1 | `dim_agente_perfil_rouanet_scd` | mestre de Agentes não ingerido; `agentes__vperfil` sem semântica verificada; origem provavelmente sem histórico | **fonte** |
| 2 | `fct_pagamento_profissional_rouanet` | fonte existe (`sac__vwpagamentodefornecedordoprojetoporitemdetalhado`), grão não medido; falta A0 item 1 (`tipo_valor`) | **medição + regra** |
| 3 | `dim_municipio_ibge` | não há base versionada de municípios nem de capitais; `ibge_sidra_bronze.localidades` é por agregado e a carga atual é nível UF | **fonte externa** |
| 4 | `brg_projeto_local_execucao` | `sac__abrangencia` só tem `idprojeto`; depende da cobertura de `map_chave_projeto_rouanet`, que é o número não medido | **medição** |
| 5 | `brg_territorio_classificacao` | não há critério homologado de vulnerabilidade para a Rouanet; o termo atual herda semântica de FCU, que é da LPG | **regra** |
| 6 | `fct_execucao_municipal_rouanet` | depende de 2, 3 e 4, mais a regra de projeto multicidade (A0 item 5) | **cadeia** |
| 7 | `fct_proponente_ano_rouanet` | depende de 1 e da chave HMAC, cujo segredo não existe | **cadeia + infra** |
| 8 | `dim_meta_alvo_rouanet` | falta base legal e vigência dos alvos; o board traz o territorial como `x%` | **regra** |

Nenhum dos oito está bloqueado por falta de tempo de implementação. **Sete dos
oito esperam uma decisão de negócio ou uma fonte que não está no banco** — e é
por isso que continuar codando agora produziria modelo que a primeira validação
derruba.

### 17.2 Fora dos modelos

1. **Gate A0.** As 11 decisões de §13 não foram tomadas. Sem elas nenhum dos
   sete KPIs pode ser escrito sem inventar regra — e regra inventada em gold é
   a coisa mais cara de desfazer depois que um painel publicou o número.
2. **Auditoria live (Onda 0).** Nada foi medido contra banco em nenhuma sessão
   até aqui. As cinco medições estão listadas em §11 e §14.
3. **Fontes do FigJam ausentes da ingestão:** `Projetos` da v2, `PreProjeto`,
   `Situacao`, `tbApiIncentivos`, `tbApiComprovacoes`, `Agentes`,
   `tbAgenteFisico`, `EnderecoNacional`, `tbCompPagXPlanilha`,
   `tbComprovantePagamento`, `tbEncaminhamentoPrestContas`,
   `tbSituacaoEncPrestContas`, `tbAlteracaoNomeProponente`, `Municipios` e
   `PopulacaoMunicipio`. O BDCorporativo carregado só tem `sysdiagrams`.
4. **Tabelas de domínio não localizadas.** Achado desta implementação: mecanismo,
   enquadramento, situação, tipo de apoio e tipo de aprovação estão expostos
   como **código sem descrição** nos seis modelos, porque a tabela de domínio de
   cada um não foi encontrada na bronze v2. Isso é dívida visível no catálogo:
   um painel que mostre `codigo_situacao` não é legível por humano.
5. **Segredo do HMAC-SHA256** para a chave de identidade entre bases. Sem ele a
   Meta 5 não cruza SALIC com LPG/PNAB/BB Ágil, e hash simples de CPF/CNPJ está
   descartado por ser enumerável.
6. **Reconciliação e aposentadoria da bronze v1.** `eventos_fomento_rouanet` lê
   o schema `bronze` e `fct_captacao_rouanet` lê o `salic_bronze`. Os dois
   convivem hoje; falta o gate G3 e um plano de migração declarado.
7. **Certificação.** Os seis modelos estão `Disabled`, sem
   `Certification.Silver`, e assim continuam até G1–G5.
8. **Golds e exposures.** Os sete KPIs e seus `exposure` de dashboard não
   existem. Dependem inteiramente do A0.
9. **Camada REST de governança (F6).** `status`, certificação, produto e
   classificação de `Uso` **não são ingeridos pelo conector dbt** — hoje eles
   vivem só no `schema.yml` e no teste. Sem o overlay, o catálogo não sabe que
   os modelos estão `Disabled`.
10. **Exportador RAG fail-closed.** Não existe neste repositório. A política
    está escrita e testada offline; quem a aplica ainda não foi escrito.

### 17.3 O caminho mais curto

Se a pergunta for "o que destrava mais coisa por menos esforço", a ordem é:

1. **VPN + `.env` e rodar `dbt build --select salic_dbt.core`.** Materializa os
   seis, e a primeira execução responde sozinha três das cinco medições
   pendentes — inclusive a cobertura de `map_chave_projeto_rouanet`, que decide
   se a Meta 4 sai pela abrangência ou precisa da view de itens comprovados.
2. **A0 em uma sessão com quem responde pelo indicador.** É decisão, não
   trabalho técnico, e é o gargalo de 7 dos 8 modelos que faltam.
3. **Base de capitais e municípios IBGE versionada.** É a única fonte externa
   pequena o bastante para entrar sem projeto: 27 capitais e ~5.570 municípios.
   Destrava `dim_municipio_ibge` e metade da Meta 4.
4. **F6 (overlay REST)**, para que `Disabled` e certificação signifiquem algo no
   catálogo — hoje eles são verdade só no repositório.

---

## 18. Auditoria live e continuação das silvers SALIC

### 2026-09-03 · Codex · F13/F14 parciais e correção da chave PRONAC

**Fontes confrontadas.** FigJam raiz `0:1`, `gate-a0-fontes.md`, relatório
FGV/OEI/MinC de 2024, IN MinC nº 10/2023 e IN MinC nº 29/2026. A IN nº 10 é da
PNAB: suas 13 categorias territoriais foram aceitas como taxonomia de
referência solicitada pelo produto, mas suas cotas de vagas não foram
transportadas como metas normativas da Rouanet. A decisão ficou registrada na
ADR 0006.

**Defeito corrigido no núcleo.** A macro preenchia o sequencial do PRONAC até
cinco posições. A auditoria mostrou `anoprojeto=08`, `sequencial=7079` e PRONAC
textual `087079`; a regra antiga produzia `0807079`. Na série observada há
PRONACs de 5, 6 e 7 dígitos. A chave agora concatena ano de duas posições com o
sequencial como registrado e ganhou o teste singular `test_pronac_macros`.
`map_chave_projeto_rouanet` também passou a falhar fechado nos dois sentidos da
relação e a deduplicar cada fonte antes das agregações. Foi corrigida ainda a
referência inexistente `ap.projeto_aprovado` em `dim_projeto_rouanet`.

**Modelos acrescentados.** `fct_pagamento_profissional_rouanet` preserva o
valor pago e a identidade necessária ao futuro join de perfil; é restrito,
proibido para RAG e continua `Disabled`. `brg_projeto_local_execucao` preserva
o local de realização sem replicar/ratear valor nem classificar
vulnerabilidade; também continua `Disabled`.

**Medições somente leitura.** A view detalhada tem 4.182.025 pagamentos e todos
recuperam PRONAC pela ponte `idpronac` auditada. O recorte bruto de 2024 tem
602.515 linhas e R$ 2.977.665.843,84, acima dos cerca de 567 mil/R$ 2,8 bilhões
da FGV; o gold precisa do recorte metodológico. `sac__abrangencia` tem 860.304
linhas/ids, mas a ponte atual recupera PRONAC para 45.022 linhas e somente
4.269 projetos; cobertura insuficiente para a Meta 4.

**Validação.** `dbt compile` passou; `test_pronac_macros` passou no banco;
`sqlfmt` e `sqlfluff` passaram nos SQLs alterados; suíte completa: 187 passed,
3 skipped. O manifest foi regenerado com dbt 1.10.22 (627 modelos) e
`make docs-collect` atualizou o acervo.

**Materialização pendente.** O usuário do `.env` possui leitura, mas recebeu
`permission denied for schema salic` no `dbt build`. A escrita é da DAG. Depois
do deploy desta branch, disparar `minc_cosmos_dag`; manter os oito modelos
`Disabled` até revisar resultados, cobertura, duplicidades e reconciliações do
run. F13 ainda precisa do mestre/perfil temporal e F14 precisa de município
oficial, ponte ampla de projeto, fontes de vulnerabilidade e regra multicidade.

---

## 19. Governança declarativa do MinC no OpenMetadata — entrega da F6

### 2026-10-18 · Claude · Implementação e simulação (nada escrito na instância)

**Pedido do Lucas.** Catálogo com tipagem, linhagem, modelos dbt, descrições
de tabela e coluna, como o dado é disponibilizado (CPF com ou sem pontuação),
testes, produtos de dados, domínios, glossário, ontologia, classificação,
métricas, operação em massa de coluna, observabilidade com incidentes.
Decisões dele nesta sessão: simulação primeiro e ele aplica; `dbt test` na DAG
do OM; alerta só dentro do OpenMetadata; comentários de código dizem o que é e
por quê, sem comparar com outro repositório.

**Retrato de antes** (auditoria live, `MinC.dbanalytics`): 0% das tabelas com
produto ou permissão de uso; Rouanet com 50% de domínio e tier; 66 casos de
teste silver/gold, todos sem resultado; schemas `cotas` e `mapas` ausentes do
catálogo; 547 tabelas no catálogo sem declaração no dbt (legado `bronze` e
parte do `salic_bronze`).

**O que entrou.**

- `dbt/minc/governance/`: `dominios.yml` (5 domínios existentes + `Cultura.Indicadores`,
  6 produtos por pasta dbt e source, camada por ativo, tier/certificação/uso
  por camada, classificação `FormatoMinC`, 6 propriedades `minc*`),
  `servicos.yml`, `schemas.yml` (23 schemas), `formatos.yml` (15 formatos, 16
  regras em massa), `termos_minc.yml` (relações tipadas + aplicação por tabela
  e coluna), `metricas.yml`, `qualidade.yml`, `contratos.yml`.
- `helpers/openmetadata/governanca/`: `catalogo.py` (fail-closed, do
  manifest), `cliente.py` (paginação, retry, displayName separado),
  `governanca.py`, `metricas.py`, `qualidade.py`, `auditoria.py`.
- DAG: `reaplicar_governanca` (`all_done`) → `postgres_testsuite` →
  `classificar_incidentes`, depois das recipes. `dbt test` no
  `dbt_artifacts.py`. Flags `OM_SYNC_GOVERNANCA`, `OM_INGEST_TESTSUITE`,
  `OM_DBT_RUN_TESTS`.
- `make openmetadata`, `openmetadata-{governanca,metricas,qualidade}`,
  `governance-audit-om`. ADR 0013 (proposto).

**Simulação (só leitura) contra a instância, com `DB_DW_DBNAME=dbanalytics`:**
governança 31 criações e 2.474 ajustes em 1.227 tabelas, 0 falhas; métricas
12 criações, 0 falhas; qualidade 27 criações (17 testes nativos, 7 suítes, 3
contratos), 0 falhas. Os números de qualidade crescem na primeira execução da
DAG: as silvers do SALIC, a silver do `mapas` e o `cotas` ainda não estão no
catálogo, e a governança não cria teste para tabela que não existe.

**Verificado.** `pytest` completo passa (31 testes novos em
`test_governanca_openmetadata.py`, mais 1 em `test_openmetadata_packaging.py`);
`black`, `ruff` e `mypy` limpos nos arquivos novos.

**NÃO verificado.**
- Nenhuma escrita: o `--confirmar` não rodou. Duas execuções seguidas devem
  terminar com `atualizados=0`; isso só se confirma depois da primeira.
- O PATCH de `/severity` em `testCaseIncidentStatus` não foi exercitado contra
  a instância.
- A DAG não foi parseada num Airflow 3 real.
- O `dbt test` dentro da DAG não rodou (exige o banco).

**Próximo passo.** O Lucas roda `make openmetadata` (simulação) e depois
`make openmetadata OPENMETADATA_ARGS=--confirmar`; em seguida
`make governance-audit-om` para o retrato de depois. Corrigir o
`OM_DATABASE` do `.env` local.

### 2026-10-19 · Claude · Aplicado na instância (`--confirmar`)

**Autorização.** O Lucas liberou a escrita na instância nesta sessão ("faz
essas mudanças e roda"). Tudo rodou com `DB_DW_DBNAME=dbanalytics`, porque o
`.env` local dele ainda tem `OM_DATABASE=minc` (o database velho).

**Ordem e resultado.** governança → métricas → qualidade → nova simulação
para conferir convergência → auditoria. Falhas finais: 0. Simulação depois da
última aplicação: 1 ajuste em 3.570 (transitório; relido, estava conforme).
Métricas (12) e qualidade (89 testes nativos, 7 suítes, 3 contratos) com 0
ajustes na segunda passada.

**Retrato de depois** (auditoria live, comparada com o de antes):

| | antes | depois |
|---|---|---|
| produto de dados | 0% | 100% em todo produto catalogado |
| permissão de uso (`Uso.*`) | 0% | 100% |
| certificação Rouanet / execução federativa / indicadores | 50% / 52% / 50% | 100% |
| domínio Rouanet | 50% | 100% |
| linhagem de coluna silver/gold | 59 de 66 | 608 de 617 (conector dbt) |
| casos de teste silver/gold | 66, sem resultado | 186, sem resultado |
| métricas / contratos / suítes | 0 / 0 / 0 | 12 / 3 / 7 |

Os testes seguem sem resultado porque o `dbt test` e a suíte nativa só rodam
na DAG desta branch, que ainda não foi implantada.

**Entrou durante a aplicação.**
- `DocumentacaoMinC.NaoVerificada` (tabela e coluna) e
  `DocumentacaoMinC.ColunasNaoVerificadas` (tabela): a marca `[NÃO VERIFICADO]`
  e `[não documentado no dicionário…]` do YAML vira etiqueta filtrável, e a
  auditoria conta. Pedido do Lucas.
- `termsOfUse` do contrato vai como `{content}`; texto puro nunca convergia.
- escrita espera até 300 s e, esgotada, vira falha sem derrubar o sync (um
  PATCH de 64 colunas etiquetadas passou de 90 s já aplicado no servidor).
- FK só para tabela referenciada que já está no catálogo
  (`salic.dim_projeto_rouanet` ausente derrubava o patch de 4 golds).

**Armadilha confirmada: a DAG da `main` desfaz parte disto a cada execução.**
Às 00:00 o `sync_glossary` da `main` regravou o glossário e as relações
tipadas voltaram a `relatedTo`; às 02:41 uma ingestão dbt apagou a
certificação de 134 tabelas que a governança já tinha certificado. Nada se
perde (a governança refaz), mas só para de acontecer com esta branch
implantada, porque é a DAG dela que roda a governança por último.

**Pendente.**
- merge e implantação da branch (sem isso, a reversão acima se repete);
- primeira execução da DAG com `OM_DBT_RUN_TESTS` e `OM_INGEST_TESTSUITE`,
  para os 186 casos ganharem resultado e o Incident Manager funcionar;
- o PATCH de `/severity` em `testCaseIncidentStatus` segue sem exercício
  real, porque não houve falha de teste ainda;
- 84 ativos declarados no dbt ainda não materializados no catálogo (quase
  todo o `cotas`, e golds novas do Rouanet como `dim_projeto_rouanet`);
- corrigir `OM_DATABASE=minc` no `.env` local.
