# Guia de documentacao dos modelos dbt

Como escrever `schema.yml` para que o modelo apareca completo no OpenMetadata.

> **Versoes deste repositorio:** `dbt-core 1.10.23` e
> `openmetadata-ingestion 1.13.3.2`. O guia equivalente do
> `data-application-cidades` foi escrito para dbt 1.7 — copiar de la sem
> adaptar gera avisos de depreciacao. As diferencas estao em
> [dbt 1.10](#o-que-muda-no-dbt-110).

## Onde fica

Ao lado dos `.sql` que descreve, sempre com o nome `schema.yml`:

```text
models/
  cultura_em_numeros/
    eixo2_meta5_primeiro_acesso_anual.sql
    schema.yml
```

O `name` do modelo e o nome do arquivo sem `.sql`.

## Exemplo completo

Tudo que o OpenMetadata le, num arquivo so:

```yaml
version: 2

models:
  - name: perfil_agentes_completo
    description: >
      Uma linha por agente cultural. Consolida o historico de acesso a fomento
      do LPG e do PNAB e classifica o agente entre primeiro acesso e recorrente.
      Recalculado a cada execucao; nao preserva historico de versoes.

    # Selecionaveis pelo dbt: `dbt build --select tag:gold`.
    # Chegam ao OpenMetadata como tags no namespace dbtTags.*
    config:
      tags:
        - gold
        - agentes
        - minc

      # Em MODELO o `meta` fica sob `config` -- declarar nos dois lugares
      # aborta o parse no dbt 1.10. Em coluna e em source continua no topo.
      meta:
        openmetadata:
          # Classificacao.Tag. Vira o Tier do ativo.
          tier: "Tier.Tier1"

          # Dono do ativo no OpenMetadata (usuario ou time ja existente la).
          owner: "arthrok"

          # Dominio de negocio. O dominio precisa existir no OpenMetadata.
          domain: "Cultura"

          # Tags livres. Certification.* recebe tratamento especial: o servidor
          # move para o campo `certification` do ativo, com state=Suggested.
          tags:
            - "Certification.Gold"

          # FQNs de termos do glossario, no formato <Glossario>.<Termo>.
          glossary:
            - "MinC.AgenteCultural"
            - "MinC.PNAB"

          # Propriedades customizadas. Precisam existir como Custom Property da
          # entidade Table no OpenMetadata antes da ingestao.
          customProperties:
            responsavel_negocio: "Meta 5 - agentes"

    columns:
      - name: identificador_unico
        description: >
          CPF ou CNPJ do agente, normalizado sem pontuacao. Chave de ligacao
          com as tabelas de contemplados e de pagamentos.
        data_type: text
        # meta.openmetadata funciona em coluna tambem -- e onde o glossario
        # rende mais, por ligar o termo de negocio ao campo concreto.
        meta:
          openmetadata:
            glossary:
              - "MinC.IdentificadorAgente"
        tests:
          - not_null
          - unique

      - name: tipo_proponente
        description: Natureza juridica do agente na inscricao.
        data_type: text
        tests:
          - accepted_values:
              arguments:
                values: ["pessoa_fisica", "pessoa_juridica", "coletivo"]

      - name: programa_fomento
        description: Politica que originou o acesso, LPG ou PNAB.
        data_type: text
        tests:
          - accepted_values:
              arguments:
                values: ["lpg", "pnab"]

      - name: perfil_classificacao
        description: >
          Classificacao do agente conforme o historico: `primeiro_acesso` quando
          nao ha registro anterior de fomento, `recorrente` caso contrario.
        data_type: text

      - name: historico_acesso_bruto
        description: Texto original do historico, preservado para auditoria.
        data_type: text

      - name: status_origem
        description: Situacao do registro no sistema de origem.
        data_type: text

    # Teste de tabela: mesma indentacao de `columns`, nao de `name`.
    tests:
      - row_count_match:
          arguments:
            source_table: agentes.lpg_agentes_pf
            target_table: agentes.perfil_agentes_completo
```

## As cinco chaves de `meta.openmetadata`

O contrato e fechado — esta definido em `DbtMetaOpenmetadata`, no proprio
conector (`metadata/ingestion/source/database/dbt/models.py`):

| chave | tipo | o que faz |
| --- | --- | --- |
| `tier` | string | Tier do ativo. Formato `Classificacao.Tag`. |
| `domain` | string | Dominio de negocio. Precisa existir no OpenMetadata. |
| `glossary` | lista | FQNs de termos. Vale em modelo **e** em coluna. |
| `customProperties` | dicionario | Propriedades customizadas ja declaradas na entidade Table. |
| `tags` | lista | Tags livres, no formato `Classificacao.Tag`. |

`owner` nao esta nesse modelo, mas **e lido**: o conector busca primeiro em
`meta.openmetadata.owner` e, se nao achar, cai em `meta.owner`. Prefira o
primeiro; o segundo e o formato antigo.

### Convencao de tier e certificacao

| camada | `tier` | tag de certificacao |
| --- | --- | --- |
| bronze | `Tier.Tier3` | `Certification.Bronze` |
| silver | `Tier.Tier2` | `Certification.Silver` |
| gold | `Tier.Tier1` | `Certification.Gold` |

Uma tag `Certification.*` nao permanece como tag: o servidor a move para o campo
`certification` do ativo, com `labelType=Automated` e `state=Suggested`.
Confirmar a certificacao exige PATCH na API — o conector dbt nao faz isso.

## Indentacao

Dois espacos por nivel, nunca tabulacao. A regra que resolve quase todo erro:

- `tests` alinhado com `name` **testa a coluna**;
- `tests` alinhado com `columns` **testa a tabela**.

```yaml
models:
  - name: meu_modelo          # 2 espacos
    columns:                  # 4 espacos
      - name: minha_coluna    # 6 espacos
        tests:                # 8 espacos -> teste DA COLUNA
          - not_null
    tests:                    # 4 espacos -> teste DA TABELA
      - row_count_match:
          arguments:
            source_table: a.b
            target_table: c.d
```

### Textos

- `>` junta as linhas num paragrafo so;
- `|` preserva as quebras (listas Markdown);
- use aspas quando o texto tiver `:`, `#`, `{}`, `[]`, ou parecer booleano/data.

## O que muda no dbt 1.10

**Argumentos de teste generico vao sob `arguments:`.** A forma antiga ainda
funciona, mas emite `MissingArgumentsPropertyInGenericTestDeprecation`.

```yaml
# dbt 1.7 (como esta no data-application-cidades)
- accepted_values: {values: ["lpg", "pnab"]}

# dbt 1.10
- accepted_values:
    arguments:
      values: ["lpg", "pnab"]
```

**Em modelo, `meta` mora sob `config`.** Declarar `meta` no topo do modelo *e*
em `config.meta` aborta o parse:

```text
found meta dictionary in 'config' dictionary and as top-level key.
```

Como todo modelo daqui ja tem `config.meta.status`, a anotacao do OpenMetadata
vai junto, em `config.meta.openmetadata`. No manifest da no mesmo: `node.meta`
e lido de `config.meta`, que e o que o conector consome. **Coluna e source nao
mudam** — la o `meta` continua no topo. `scripts/anotar_openmetadata.py` ja
grava assim e migra o que estiver no formato antigo.

**`tests:` continua valido.** A documentacao mais recente prefere `data_tests:`,
mas `tests:` nao emite depreciacao nesta versao. Se migrar, migre o projeto
inteiro de uma vez; nao misture as duas formas.

Verifique com:

```bash
dbt parse --profiles-dir . --no-partial-parse --show-all-deprecations
```

## O que o OpenMetadata le sem passar por `meta`

- `description` de modelo e coluna, desde que a recipe tenha
  `dbtUpdateDescriptions: true`;
- `config.tags`, que viram tags em `dbtTags.*`;
- `tests`, que viram testes de qualidade, com resultado vindo do
  `run_results.json`;
- `constraints` do dbt (`type`, `name`, `expression`), unica via declarativa de
  PK/FK — o DW nao tem constraint fisica;
- `exposures`, que entram no catalogo com linhagem;
- `data_type` da coluna, usado quando o `catalog.json` nao resolve o tipo.

## Checklist

- [ ] `version: 2` na primeira linha.
- [ ] Dois espacos, nenhuma tabulacao.
- [ ] Todo `name` corresponde a um modelo ou coluna real.
- [ ] A descricao do modelo diz o que **uma linha** representa.
- [ ] Toda coluna do `select` final tem descricao que nao repete o nome.
- [ ] Tags operacionais em `config.tags`; metadados em
      `config.meta.openmetadata` (modelo) ou `meta.openmetadata` (coluna, source).
- [ ] Argumentos de teste generico sob `arguments:`.
- [ ] `dbt parse` sem erro e sem depreciacao nova.
