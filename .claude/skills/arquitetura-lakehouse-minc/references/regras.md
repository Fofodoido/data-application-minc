# Regras dos ADRs 0010 e 0015, e como cada uma se verifica

A fonte é o [ADR 0010](../../../../docs/adr/0010-camadas-e-schemas-do-data-lakehouse.md),
com as partes que o [ADR 0015](../../../../docs/adr/0015-produtos-em-data-products-e-gold-lendo-gold.md)
substitui.
Esta tabela só diz **qual regra o `verificar.py` aplica, qual é leitura
manual e qual não é verificável no ambiente local**. Regra mudou? Muda o ADR
primeiro (ADR novo, não edição), depois o script, depois esta lista.

| # | Regra | Verificação | Como |
|---|---|---|---|
| R1 | Pasta de fonte só tem `bronze_*` e `silver_*`; `intermediate/` só `int_*`; produto é subpasta de `data_products/` e não tem prefixo de camada. Arquivos planos, sem subpasta (ADR 0015, decisão 1) | automática | nome e profundidade do caminho |
| R2 | Bronze lê exatamente uma `source()`, nenhum `ref()`, e não tem `join`, `where`, `case`, `cast`, `::`, `distinct`, `union`, `group by` | automática | regex no SQL sem comentários |
| R3 | Silver não lê `source()`; só faz `ref()` a bronze ou silver **da mesma pasta** | automática | refs resolvidos contra o índice do projeto |
| R4 | Intermediate não lê `source()`; só faz `ref()` a silver ou intermediate | automática | idem |
| R5 | Gold não lê `source()` nem bronze; só `ref()` a silver, intermediate ou gold do mesmo produto (ADR 0015, decisão 4) | automática | idem |
| R6 | Valor ausente fica nulo, nunca vira falso | **aviso** automático | `coalesce(..., false)`; o script não lê semântica, só o padrão |
| R7 | `+schema` da pasta é o nome da pasta (`intermediate` para a Intermediate) | automática | `dbt_project.yml` |
| R8 | Regra de negócio final só na Gold. O que é regra de negócio, inclusive o pareamento de estorno, está no ADR 0015, decisão 3 | manual | leia o `where`, os `case` e os `join` das silvers e intermediates do PR; na dúvida, é Gold |
| R9 | Bronze guarda histórico permanente e metadados de ingestão | manual | hoje a bronze é recarregada com `DROP`; vale a partir da stack de produção |
| R10 | Dimensão comum (tempo, território) é Intermediate | manual | nome e refs |
| R11 | Gold não muda de forma incompatível sem versionamento | manual | diff do `schema.yml` da gold |
| R12 | Entidade preserva o identificador da origem (`<banco>_<tabela>` no SALIC) | manual | compare com `sources*.yml` |
| R13 | Raw é imutável e vive em `raw/<fonte>/<entidade>/` no MinIO | **não verificável localmente** | ambiente local grava no Postgres |
| R14 | Todo consumo passa pelo Trino; Ranger anonimiza na consulta | **não verificável localmente** | ADR 0009 |
| R15 | O dbt roda pelo Trino (`dbt-trino`) | **não verificável localmente** | profile local é `dbt-postgres` |
| R16 | CPF, CNPJ e qualquer documento ficam como texto na Silver, com o zero à esquerda. Nunca `integer` nem `bigint` | manual | procure `::int`, `::bigint` e `cast(... as int...)` em colunas de documento |
| R17 | Id e código numérico vindos da fonte são `bigint`, não `integer`. No BB Ágil, `codigo_programa` chega a 14 dígitos e `referencenumber` a 15 | manual | procure `::integer` e `as integer`; confira o maior valor no dado real |
| R18 | Data em texto passa pela macro `data_texto`, que transforma data inexistente em `NULL` em vez de derrubar o modelo. Nunca `to_date` ou `::date` direto | manual | procure `to_date(` e `::date` nas silvers |
| R19 | Teste rebaixado de `error` para `warn` (`severity: warn`) tem a justificativa no PR: por que a linha que falha é aceitável e quem a trata depois | manual | diff dos `schema.yml` do PR |

## O que o script não sabe

- **Semântica.** R2 é por palavra-chave. Uma bronze com `where` legítimo não
  existe pela regra, mas um `case` dentro de um comentário já foi removido
  antes da busca. Se o script apontar e você discordar, o ADR vence: bronze
  não filtra.
- **Modelos `enabled=false`.** Entram na contagem. São dívida como os outros.
- **Alias.** O ADR não usa `alias`. Se um modelo tem, o nome do arquivo já
  está errado e R1 pega.
- **Colisão de nome entre fontes.** É o dbt que acusa, no `dbt parse`. A
  exceção (fonte no nome da entidade) está no ADR.
