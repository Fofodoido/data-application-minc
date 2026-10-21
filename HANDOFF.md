# HANDOFF — camada silver do SALIC (Lei Rouanet)

**Branch:** `feat/dbt-salic-silver` · **Base:** `main@d404531` · **Escrito em:** 2026-09-04

Este documento existe para quem — pessoa ou agente — vai continuar o trabalho das
silvers do SALIC sem ter acompanhado o que veio antes. Ele é uma **foto datada**,
não o registro vivo: o registro vivo é
[`docs/openmetadata/MEMORY.md`](docs/openmetadata/MEMORY.md), que tem protocolo
próprio e deve ser atualizado a cada entrega.

---

## Comece por aqui

```bash
make setup && make test     # esperado: 187 passed, 3 skipped
```

**Leia nesta ordem, antes de escrever qualquer linha:**

1. [`docs/adr/0006-silvers-salic-gate-a0-parcial.md`](docs/adr/0006-silvers-salic-gate-a0-parcial.md)
   — as regras de negócio que já foram decididas, e as que continuam abertas.
2. [`docs/openmetadata/MEMORY.md`](docs/openmetadata/MEMORY.md) **§0** (protocolo),
   **§17** (lacunas) e **§18** (última auditoria live).
3. Os `schema.yml` de [`dbt/minc/models/salic_dbt/core/`](dbt/minc/models/salic_dbt/core/)
   — cada modelo documenta grão, universo, limitações e o que foi deliberadamente
   deixado de fora, com o porquê.

> **§0 da MEMORY.md é protocolo, não sugestão.** Reivindique uma frente antes de
> começar, marque o que entregou, e **nunca apague entrada de diário** — corrija
> com entrada nova. Várias IAs trabalham neste arquivo em paralelo.

---

## Estado em uma linha

A fundação está pronta e testada: **571 modelos bronze + 8 silver**, com
documentação por coluna, classificação de PII e 187 testes offline passando.
**Nenhum modelo silver foi materializado ainda** e nenhum gold existe. O que trava
não é engenharia — são duas decisões de negócio e uma de arquitetura.

---

## O que já está implementado

### Camada silver — 8 modelos em `dbt/minc/models/salic_dbt/`

**`core/` — a fundação que todo o resto consome**

| Modelo | Grão | RAG |
|---|---|---|
| `map_chave_projeto_rouanet` | 1 PRONAC | elegível após validação |
| `dim_projeto_rouanet` | 1 PRONAC | elegível após validação |
| `brg_projeto_proponente_rouanet` | 1 PRONAC × agente | **proibido** — zona restrita |
| `fct_captacao_rouanet` | 1 recibo de captação | elegível após validação |
| `fct_aprovacao_rouanet` | 1 registro de aprovação | elegível após validação |
| `fct_evento_acesso_rouanet` | 1 evento datado | elegível após validação |

**`meta3/`** — `fct_pagamento_profissional_rouanet`: 1 pagamento × comprovante ×
item × prestador. Restrito, proibido para RAG.

**`meta4/`** — `brg_projeto_local_execucao`: 1 registro de abrangência. **Não**
rateia valor nem classifica vulnerabilidade, de propósito.

Todos com `status: Disabled` e **sem** `Certification.Silver`. A regra do plano é
que gate não verificado não certifica ativo — não mude isso sem passar pelos gates.

### Decisões de modelagem que você precisa entender antes de mexer

- **O `map_chave_projeto_rouanet` não estava no plano original.** Existe porque o
  SALIC identifica o mesmo projeto por três chaves (`pronac`, `idpronac`,
  `idprojeto`) e nenhuma tabela da bronze v2 carrega as três na mesma linha. Ele é
  **fail-closed**: PRONAC ambíguo sai com o id `NULL` e a flag `*_ambiguo` em
  `true`, em vez de um `min()` que escolhe sozinho. Um join que não acontece é
  visível; um join com o id errado não é.
- **A identidade fica isolada na ponte.** Dimensão e fatos não carregam documento,
  nome nem `logon` — é o que os deixa candidatos a publicação. Em captação, o
  CPF/CNPJ do mecenas dá lugar a `tipo_pessoa_mecenas`, derivado só do
  comprimento. Em aprovação, `resumoaprovacao` foi omitido: texto livre de
  analista é o vetor por onde PII entra numa tabela sem nenhuma classificação
  automática marcar.
- **`fct_evento_acesso_rouanet` tem dois tipos de evento, não três.** Falta o
  registro porque `PreProjeto`/`Projetos` v2 não estavam ingeridas. O que existe
  hoje mede primeiro acesso **a recurso**, não primeiro contato com o sistema.
  Derivar registro de `dtsituacao` seria errado — aquilo é a última mudança de
  situação.

### Macros de chave, e um defeito já corrigido

[`dbt/minc/macros/salic/chaves_salic.sql`](dbt/minc/macros/salic/chaves_salic.sql)
centraliza a normalização de PRONAC.

> ⚠️ **Correção de 03/09 — não reintroduza.** A versão original preenchia o
> sequencial com zeros até 5 posições. A auditoria mostrou `anoprojeto=08` +
> `sequencial=7079` → PRONAC real `087079`, enquanto a regra antiga produzia
> `0807079`. **A série tem PRONACs de 5, 6 e 7 dígitos.** A regra atual concatena
> sem padding e tem teste singular em `dbt/minc/tests/test_pronac_macros.sql`.

### Ingestão OpenMetadata

- **`markDeletedTables: false`** em `postgres_metadata.yaml`. O default do conector
  é `true` e **apaga catálogo**: rodar contra banco incompleto marca como deletado
  tudo que o catálogo tem e o banco não, sem aviso. Com 571 views, uma carga
  parcial custaria o catálogo inteiro. Duas guardas de teste: uma exige a chave
  ali, outra proíbe declará-la nas demais recipes (onde não tem efeito).
- **O glossário é a primeira task da DAG.** Era módulo órfão. Referência a FQN de
  termo inexistente **não falha a ingestão** — é descartada em silêncio e o ativo
  chega ao catálogo sem o vínculo.
- **Flags:** `OM_INGEST_POSTGRES`, `OM_INGEST_DBT`, `OM_INGEST_GLOSSARY`. O
  **profiler está desligado por padrão** — ele publica min, max e distribuição,
  que são estatísticas reveladoras num banco com CPF, CNPJ e dados de raça e
  deficiência. Religue com `OM_INGEST_PROFILER=true` só depois de verificar as
  exclusões de coluna sensível.

### Guardas de governança

[`tests/test_salic_silver_governance.py`](tests/test_salic_silver_governance.py)
verifica offline: documentação e `data_type` por coluna, ausência do marcador
`[NÃO VERIFICADO]`, classificação de PII por heurística de nome (com dispensa
declarada no próprio YAML, não na regex do teste), RAG fail-closed, FQN de
glossário existente na fonte declarativa, silver nunca lendo `source()` direto, e
modelo publicável nunca lendo modelo restrito.

O escopo exclui **por camada** (`CAMADAS_NAO_SILVER`), não por lista de pastas —
`meta5` e `gold` entram sozinhas quando existirem.

### Ferramenta de auditoria

[`scripts/auditar_fontes_silver_salic.py`](scripts/auditar_fontes_silver_salic.py)
procura no banco, por conceito, as fontes que faltam. Read-only,
`statement_timeout`, sem impressão de credencial, aceita os dois contratos de
`.env` do repositório. Precisa de VPN.

---

## O que falta

### 🔴 Bloqueio 1 — a Meta 3 não tem fonte de autodeclaração

**Não existe coluna de raça, etnia, pertencimento indígena, quilombola ou
deficiência em nenhuma das 1.121 tabelas** dos schemas `bronze` e `salic_bronze`.
Busca por nome de coluna com sete padrões: zero para os cinco. As três ocorrências
de "gênero" são gênero musical e audiovisual.

O `agentes__perfil` **não é autodeclaração**: cobre 8.139 de 995.851 agentes
(0,8%) e seus valores são códigos institucionais. Os 5.563 registros do código
`48` batem com o número de municípios — quase certamente prefeituras.

**Consequência: os indicadores de diversidade da Meta 3 não são calculáveis a
partir do SALIC.** A autodeclaração teria que vir de outra base (Mapa da Cultura,
cadastro gov.br) por cruzamento de CPF/CNPJ, ou ser coletada.

Isso precisa subir para quem definiu a meta. É a diferença entre "o indicador está
atrasado" e "o indicador não tem fonte" — **não tente resolver com engenharia.**

*Ressalva de método: a busca foi por nome de coluna, não por conteúdo. As
candidatas óbvias foram inspecionadas e nenhuma carrega autodeclaração.*

### 🔴 Bloqueio 2 — dois schemas, e o dbt só olha para um

O banco tem **1.121 tabelas** entre `bronze` (ingestão v1) e `salic_bronze`
(v2, por Trino). **208 existem apenas na v1** — e é lá que está o banco `Agentes`
inteiro. A v2 trouxe do `Agentes` só 15 views.

Os 571 modelos bronze leem `salic_bronze`. O
[`sources_sac_legado.yml`](dbt/minc/models/salic_dbt/bronze/sources_sac_legado.yml)
aponta para `bronze`, mas com a instrução explícita de *"não declare tabela nova
aqui"*. **Resultado: o dado está no banco e é invisível para o dbt.**

Medido em **02/09** (schema `bronze`):

| Tabela | Linhas | O que destrava |
|---|---|---|
| `agentes__agentes` | 995.851 | mestre de agentes; tem `dtcadastro`, o evento que falta na Meta 5 |
| `agentes__endereconacional` | 948.544 agentes (95,2%) | residência do proponente |
| `agentes__municipios` | 5.568 | **`dim_municipio_ibge`** — falta só a flag de capital (27 linhas, seed) |
| `sac__projetos` | 276.887 (34 colunas) | cadastro oficial; traz `idpronac` **e** `idprojeto` na mesma linha |
| `sac__situacao` | 296 | resolve a dívida de "código sem descrição" |

**Decisão de arquitetura necessária:** (a) reingerir essas tabelas pela v2, ou
(b) rever a política do `sources_sac_legado.yml` e declarar como source da v1 o
que a v2 não traz. A (b) destrava três modelos no mesmo dia; a (a) é mais limpa a
longo prazo. O que **não** dá é manter a regra atual e seguir chamando de "falta
fonte".

> **Lead mais valioso para a Meta 4.** A ADR 0006 registra que a ponte atual
> recupera PRONAC para apenas **4.269 projetos** — cobertura insuficiente para
> publicar. Numa medição de **02/09** (**não reconferida**, VPN fora no dia 04),
> `bronze.sac__projetos` resolvia **155.113** dos `idprojeto` da abrangência.
> **Reconferir isso é o primeiro passo da Meta 4** — pode ser a diferença entre
> publicar e não publicar o indicador.

### 🟡 Bloqueio 3 — gate A0 ainda aberto

Seis decisões continuam pendentes, listadas na ADR 0006: universo, numerador e
denominador de cada gold; rateio ou apenas cobertura para projeto multicidade;
fonte e regra computável de cada categoria territorial; conceito de primeiro
acesso e horizonte histórico; política de publicação, limiar de supressão e
aprovação do DPO; e se os percentuais da PNAB entram como comparação não
normativa.

### Modelos que faltam — 6 de 14

| Modelo | Bloqueio |
|---|---|
| `dim_municipio_ibge` | **nenhum de dados** — a fonte existe; falta seed de capitais |
| `dim_agente_perfil_rouanet_scd` | Bloqueio 1 — sem autodeclaração no SALIC |
| `brg_territorio_classificacao` | falta fonte e limiar homologados por categoria da IN nº 10 |
| `fct_execucao_municipal_rouanet` | depende dos anteriores + regra de multicidade |
| `fct_proponente_ano_rouanet` | definição de primeiro acesso + chave HMAC (segredo não existe) |
| `dim_meta_alvo_rouanet` | seed com base legal e vigência |

Mais os **7 golds** e seus `exposures`, que dependem inteiramente do A0.

### Infraestrutura e governança

- **Materialização nunca rodou.** O usuário do `.env` tem leitura, mas o
  `dbt build` deu `permission denied for schema salic` — a escrita é da DAG.
  **Depois do deploy desta branch, disparar `minc_cosmos_dag`**, e revisar
  resultados, cobertura, duplicidades e reconciliações antes de tirar qualquer
  modelo de `Disabled`.
- **Overlay REST de governança** não existe: `status`, certificação, produto e
  classificação de uso **não são ingeridos pelo conector dbt**. Hoje o catálogo
  não sabe que os modelos estão `Disabled` — isso é verdade só no repositório.
- **Exportador RAG fail-closed** não existe. A política está escrita e testada
  offline; quem a aplica ainda não foi escrito.
- **Reconciliação com `salic/silver_legado_sac_captacao_proponente`** (ex-`eventos_fomento_rouanet`, que lê a v1) pendente, e sem
  plano de aposentadoria da v1.
- **Segredo do HMAC-SHA256** para a chave de identidade entre bases. Hash simples
  de CPF/CNPJ está descartado: é enumerável.

---

## Próximos passos, em ordem

1. **Materializar.** Deploy da branch e disparar `minc_cosmos_dag`. É o que
   responde a maior parte das hipóteses de uma vez só.
2. **Reconferir a ponte da Meta 4** com `bronze.sac__projetos` (4.269 → 155.113?).
3. **Decidir (a) ou (b)** do Bloqueio 2. Reunião curta; destrava três modelos.
4. **Levar o achado da Meta 3** para quem definiu a meta.
5. **`dim_municipio_ibge` + seed de capitais.** Pequeno, isolado, e metade da
   Meta 4 depende dele.
6. **Fechar o A0** para liberar os golds.

---

## Armadilhas deste repositório que já custaram tempo

Estão todas em `docs/openmetadata/MEMORY.md` §7. As que mais atrapalham aqui:

- **O conector dbt não cria tabela.** Ele anexa metadado a tabela que já existe.
  Modelo dbt sem tabela materializada não aparece no OpenMetadata, e nenhuma
  configuração muda isso — falta `dbt run`, não recipe.
- **`lpad` do Postgres trunca em silêncio.** `lpad('2023', 2, '0')` devolve `'20'`.
  Toda montagem de chave precisa do guarda de regex **antes** do `lpad`.
- **O `manifest.json` não é mais versionado.** Desde 06/09/2026 o Cosmos monta a
  DAG por `dbt ls` e os timeouts de parse sobem no `docker-compose.yml` — não há
  mais `make dbt-manifest` nem artefato para commitar junto. Ver ADR 0007.
- **A partir do dbt 1.10, `meta` no topo do modelo E em `config.meta` aborta o
  parse.** Em modelo vai sob `config`; em coluna e source segue no topo.
- **Quem altera modelo ou DAG roda `make docs-collect`** e commita o acervo no
  mesmo PR, senão o site publicado segue descrevendo o estado anterior sem
  nenhum sinal de que está velho.
