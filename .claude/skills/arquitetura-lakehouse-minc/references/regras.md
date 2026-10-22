# Regras do ADR 0010, e como cada uma se verifica

A fonte é o [ADR 0010](../../../../docs/adr/0010-camadas-e-schemas-do-data-lakehouse.md).
Esta tabela só diz **qual regra o `verificar.py` aplica, qual é leitura
manual e qual não é verificável no ambiente local**. Regra mudou? Muda o ADR
primeiro (ADR novo, não edição), depois o script, depois esta lista.

| # | Regra | Verificação | Como |
|---|---|---|---|
| R1 | Pasta de fonte só tem `bronze_*` e `silver_*`; `intermediate/` só `int_*`; pasta de produto não tem prefixo de camada. Arquivos planos, sem subpasta | automática | nome e profundidade do caminho |
| R2 | Bronze lê exatamente uma `source()`, nenhum `ref()`, e não tem `join`, `where`, `case`, `cast`, `::`, `distinct`, `union`, `group by` | automática | regex no SQL sem comentários |
| R3 | Silver não lê `source()`; só faz `ref()` a bronze ou silver **da mesma pasta** | automática | refs resolvidos contra o índice do projeto; `ref()` é lido em qualquer posição, inclusive dentro de `{% set %}` |
| R4 | Intermediate não lê `source()`; só faz `ref()` a silver ou intermediate | automática | idem |
| R5 | Gold não lê `source()` nem bronze; só `ref()` a silver ou intermediate | automática | idem |
| R6 | Valor ausente fica nulo, nunca vira falso | **aviso** automático | `coalesce(..., false)`; o script não lê semântica, só o padrão |
| R7 | `+schema` da pasta é o nome da pasta (`intermediate` para a Intermediate) | automática | `dbt_project.yml` |
| R8 | Regra de negócio final só na Gold (filtro que define "quem conta") | **aviso** automático + manual | heurística aponta em silver/intermediate: classificação (`CASE … THEN '<rótulo>'`), corte agregado (`HAVING`), escopo por lista (`IN ('…')`). É aviso: o script não distingue classificação de limpeza, então confira à mão. Padrões como estorno (`INTERSECT`) e `status = 'Pago'` a heurística não pega — leia o `where` também |
| R9 | Bronze guarda histórico permanente e metadados de ingestão | manual | hoje a bronze é recarregada com `DROP`; vale a partir da stack de produção |
| R10 | Dimensão comum (tempo, território) é Intermediate | manual | nome e refs |
| R11 | Gold não muda de forma incompatível sem versionamento | manual | diff do `schema.yml` da gold |
| R12 | Entidade preserva o identificador da origem (`<banco>_<tabela>` no SALIC) | manual | compare com `sources*.yml` |
| R13 | Raw é imutável e vive em `raw/<fonte>/<entidade>/` no MinIO | **não verificável localmente** | ambiente local grava no Postgres |
| R14 | Todo consumo passa pelo Trino; Ranger anonimiza na consulta | **não verificável localmente** | ADR 0009 |
| R15 | O dbt roda pelo Trino (`dbt-trino`) | **não verificável localmente** | profile local é `dbt-postgres` |

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
