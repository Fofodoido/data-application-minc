# Migrar modelos para a estrutura do ADR 0010

Lote pequeno, uma fonte por vez, e o número não muda. O que muda é pasta,
nome e schema.

## Antes de mover qualquer arquivo

```bash
cd dbt/minc && dbt ls --select <modelo>+ --output name     # quem depende dele
python3 ../../.claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py --tudo \
  | grep '<pasta de hoje>'                                  # destino sugerido
```

Anote a contagem de linhas do modelo (ou de cada modelo do lote) no banco de
desenvolvimento. É o número que o PR vai comparar.

## O que cada pasta de hoje vira

A tabela está no ADR 0010, seção "Consequências". Em resumo: o que só tipa é
Silver; o que cruza fontes é Intermediate; o que está em `gold/` é Gold do
`cultura_em_numeros`; a bronze SALIC de hoje (589 modelos que tipam) é Silver,
e a Bronze passa a ser cópia fiel ou `source`.

Decida a camada pelo que o SQL faz. `stg_agentes_pf` filtra e normaliza →
Silver. `eventos_fomento` unia BB Ágil, SALIC e Ancine e aplicava as regras
da Meta 5: virou dois, o cruzamento em `int_ancine_bbagil_salic` e a regra em
`data_products/cultura_em_numeros/eixo2_meta5_evento_fomento`. Um modelo que
faz as duas coisas vira dois.

## Os seis passos, nesta ordem

1. **Pasta e `+schema`.** Crie `models/<fonte>/` (ou
   `models/data_products/<produto>/`, com o bloco aninhado em `data_products:`)
   e declare em `dbt_project.yml`:
   ```yaml
   <fonte>:
     +schema: <fonte>
   ```
   Materialização continua por pasta; se a fonte precisa de `table` só nas
   silvers, use `+materialized` com seletor por prefixo, não subpasta.
2. **`git mv` e renomeie** para `bronze_<entidade>.sql` / `silver_<entidade>.sql`
   / `int_<f1>_<f2>.sql` / `<nome>.sql`. Tire o `config(alias=...)`.
3. **Atualize os `ref()`** nos dependentes (saída do `dbt ls` acima) e mova o
   bloco do modelo no `schema.yml` para o `schema.yml` da pasta nova.
4. **Rode o verificador na pasta destino** e o `dbt parse`:
   ```bash
   python3 .claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py dbt/minc/models/<fonte>
   cd dbt/minc && dbt parse
   ```
5. **Toque os cinco leitores da convenção de pastas.** Nenhum deles dá erro
   se ficar para trás; eles só passam a descrever a estrutura antiga.

   | Leitor | O que fazer |
   |---|---|
   | `tests/test_salic_silver_governance.py` | `SALIC_DIR` e `CAMADAS_NAO_SILVER` apontam para `salic_dbt` e `bronze`. Até o teste ser generalizado, aponte para a pasta nova ou a governança deixa de cobrir o que migrou |
   | `docs-pages/tooling/collectors/dbt_models.py` | `CAMADAS` e o sufixo `_dbt` em `_classificar`; sem ajuste, a pasta nova vira domínio "outros". Depois rode `make docs-collect` |
   | `helpers/openmetadata/recipes/postgres_*.yaml` | o schema novo entra no `schemaFilterPattern` das três recipes; `tests/test_openmetadata_packaging.py` acusa se faltar |
   | `scripts/gerar_*.py` e `.claude/skills/bronze-salic-dbt/scripts/gerar_modelos.py` | `DESTINO` e `{s}_bronze` fixos; se o gerador for rodar de novo, aponte para a pasta nova |
   | `dbt/minc/macros/get_custom_schema.sql` | hoje é o padrão do dbt e o `+schema` manda; só muda se algum dia o schema passar a ser derivado do caminho |

6. **Confira o número.** `dbt run --select <modelo>` e compare a contagem com
   a anotada antes. Diferente? Pare: o `ref()` mudou de alvo ou a camada foi
   mal escolhida. Registre o antes/depois no PR.

## O que não fazer

- **Não misture migração com mudança de regra.** Se ao mover você descobrir
  que a normalização de documento está diferente da macro, é outro PR, com
  outra contagem.
- **Não deixe a pasta velha pela metade** por mais de um PR. Pasta com dois
  padrões é pior que pasta com um padrão errado.
- **Não leia a ingestão SALIC v1** (schema `bronze`, `sources_sac_legado.yml`)
  em modelo novo nem migrado: desde 21/10/2026 nenhum modelo lê de lá, e a v2
  (`salic_bronze`) é a oficial (ADR 0005).
