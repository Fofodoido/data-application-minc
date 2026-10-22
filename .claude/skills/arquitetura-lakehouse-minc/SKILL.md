---
name: arquitetura-lakehouse-minc
description: >-
  Use quando for criar, mover ou renomear modelo dbt em `dbt/minc/models`,
  quando precisar decidir em que camada (bronze, silver, intermediate, gold),
  pasta ou schema uma transformação entra, quando o usuário perguntar se o
  código "está seguindo a arquitetura", pedir para "verificar a arquitetura",
  "migrar para a estrutura aprovada", "refatorar conforme o ADR", ou citar o
  Data Lakehouse, o ADR 0009 ou o ADR 0010. Também antes de abrir PR que toque
  modelo dbt.
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Arquitetura do Data Lakehouse do MinC

A arquitetura aprovada está em dois ADRs, e eles são a fonte das regras —
não este arquivo:

- [ADR 0009](../../../docs/adr/0009-data-lakehouse-extracao-airflow-camadas-minio-acesso-trino.md):
  componentes e fluxo (Airflow extrai, raw no MinIO, Trino carrega e serve,
  Ranger anonimiza na consulta).
- [ADR 0010](../../../docs/adr/0010-camadas-e-schemas-do-data-lakehouse.md):
  as cinco camadas, pastas, nomes e schemas. **Leia antes de criar qualquer
  modelo.**

Esta skill tem dois modos. Os dois começam pelo script.

## O princípio

**As violações vêm do script. A narrativa vem de você.**

Hoje nenhum dos 712 modelos segue a nomenclatura aprovada. Contar isso de
cabeça, ou dizer "está conforme" depois de olhar três arquivos, é o erro que
esta skill existe para impedir. Você não afirma conformidade nem violação que
não esteja na saída do `verificar.py`. Se o script não cobre a regra (as
manuais e as não verificáveis em `references/regras.md`), você diz que é
leitura sua, não apuração.

## Modo 1 — Verificar

```bash
python3 .claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py            # mudados vs main local
python3 .claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py --tudo     # inventário inteiro
python3 .claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py <pasta>    # um recorte
```

O padrão compara com a `main` **local**: rode `git fetch && git merge-base`
ou atualize a `main` antes, senão o diff inclui commits que já estão lá.

O script sai com 1 se há violação no escopo. Ele imprime, por regra, cada
arquivo e o motivo. Com `--tudo` imprime também um **destino sugerido** por
arquivo fora do padrão — é mecânico (olha refs e sources), e a camada certa
depende do que o SQL faz. Quem migra decide.

Depois de rodar:

1. Leia a saída inteira.
2. Para cada violação que vai reportar, abra o arquivo e diga **o que** ele
   faz que o coloca na camada errada, não só que o nome está fora.
3. Regras manuais (R8 em diante em `references/regras.md`): confira à mão e
   escreva "leitura minha" ao lado.
4. Entregue nesta ordem: contagem do script, violações por regra com um
   exemplo cada, o que é leitura sua, e o que não dá para verificar
   localmente (raw imutável, Ranger, MinIO).

Código novo não pode acrescentar violação. O inventário do `--tudo` é o
backlog da migração, não um bug para consertar no PR da vez.

## Modo 2 — Codar ou migrar dentro da estrutura

Decida a camada pelo que o modelo **faz**, não pelo que ele é chamado hoje:

| O modelo… | Camada | Arquivo |
|---|---|---|
| copia uma tabela do raw, sem cast, filtro ou join | Bronze | `<fonte>/bronze_<entidade>.sql` |
| tipa, limpa, deduplica, normaliza, lendo só a própria fonte | Silver | `<fonte>/silver_<entidade>.sql` |
| cruza duas ou mais fontes, ou é dimensão comum (tempo, território) | Intermediate | `intermediate/int_<f1>_..._<fN>.sql`, fontes em ordem alfabética |
| aplica a regra de negócio final de um produto | Gold | `data_products/<produto>/<nome>.sql`, convenção do produto |

Se o modelo faz duas dessas coisas, são dois modelos.

Regra de negócio é tudo que decide **quem conta** para um indicador: escopo
de mecanismo, classificação em política, exclusão de ente público, piso de
valor, definição de novo entrante. Isso é Gold, mesmo quando várias golds
precisam da mesma regra — nesse caso ela vira uma gold-base do produto que as
outras leem (Gold pode ler Gold do mesmo produto). Silver só limpa (lixo de
parsing, estorno, deduplicação); Intermediate só cruza, e leva para a Gold as
colunas brutas de que a regra precisa.

Entidade é o que identifica a tabela na origem. No SALIC, `<banco>_<tabela>`
(`agentes_agentes`); no BB Ágil, o nome da tabela extraída. Produto novo é uma
subpasta de `data_products/`, com o `+schema` aninhado em `data_products:` no
`dbt_project.yml`; os leitores da convenção o reconhecem pela pasta, sem lista.

Pasta nova exige `+schema` igual ao nome da pasta no `dbt_project.yml`, e
exige tocar os cinco leitores da convenção de pastas. A receita, com a ordem e
o que conferir em cada um, está em **`references/migracao.md`**. Pular um
deles não dá erro: a governança deixa de cobrir, o site publica a estrutura
velha, o OpenMetadata não vê o schema.

Para **migrar** um modelo existente: siga `references/migracao.md` lote a
lote, rode o `verificar.py` na pasta destino antes de commitar, e registre a
contagem de linhas antes e depois no PR — nome e pasta mudam, número não.

## Onde mexer, por sintoma

| O que você quer | Onde |
|---|---|
| Saber a regra exata de uma camada | ADR 0010, seção "As cinco camadas" |
| Saber por que não é por domínio de meta | ADR 0010, "O que foi descartado" |
| Schema de uma pasta | `dbt/minc/dbt_project.yml`, chave `+schema` |
| Lista de produtos de dados | as subpastas de `dbt/minc/models/data_products/` |
| Regra que o script não verifica | `references/regras.md`, marcadas manual |
| O que uma pasta de hoje vira | ADR 0010, tabela em "Consequências" |
| Mover uma pasta sem quebrar governança, site e catálogo | `references/migracao.md` |

## O limite do que dá para afirmar

> ✅ "O `verificar.py` aponta 59 modelos em `mapas_dbt/silver` lendo
> `source()`; pelo ADR 0010 a silver lê a bronze por `ref()`, e a bronze do
> Mapas hoje é só `source`, então falta o modelo bronze."
>
> ❌ "O projeto está 92% conforme." — número que não saiu do script.

Regras que dependem de MinIO, Iceberg e Ranger (raw imutável, acesso só pelo
Trino, anonimização na consulta) **não são verificáveis no ambiente local**,
que roda em Postgres. Diga isso; não diga "conforme".

## Antes de considerar pronto

- [ ] `verificar.py` rodou no escopo do PR e saiu com 0
- [ ] Toda violação citada no relatório está na saída do script
- [ ] O que é leitura sua está marcado como leitura sua
- [ ] Pasta nova tem `+schema` e passou pelos cinco pontos de `references/migracao.md`
- [ ] Modelo migrado tem contagem antes/depois registrada no PR
- [ ] `make docs-collect` rodou, se modelo ou pasta mudou (CLAUDE.md)
