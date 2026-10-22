{% macro origem_da_bronze(bronze) %}
    {#-
      A tabela de origem (a `source`) que uma bronze copia, como Relation.

      Para a silver que descobre colunas no information_schema. A bronze é
      `select *` da source, então as colunas são as mesmas; mas a bronze é
      modelo e só existe depois do primeiro `dbt run`, e a source existe
      sempre. Olhar a bronze fazia `dbt compile` e `dbt docs generate` falharem
      num ambiente novo. Pelo grafo, a silver continua sem chamar source() — a
      regra do ADR 0010 — e a pergunta vai para a tabela que está no banco.

      Só faz sentido com execute=true: em `dbt parse` o grafo ainda não está
      montado, e quem chama deve cair no ref().
    -#}
    {%- set no = (
        graph.nodes.values()
        | selectattr("resource_type", "equalto", "model")
        | selectattr("name", "equalto", bronze)
        | list
    ) -%}
    {%- if no | length != 1 or no[0].sources | length != 1 -%}
        {{
            exceptions.raise_compiler_error(
                "origem_da_bronze: '"
                ~ bronze
                ~ "' não é um modelo habilitado que lê "
                ~ "exatamente uma source."
            )
        }}
    {%- endif -%}
    {%- set fonte, tabela = no[0].sources[0] -%}
    {%- set src = (
        graph.sources.values()
        | selectattr("source_name", "equalto", fonte)
        | selectattr("name", "equalto", tabela)
        | first
    ) -%}
    {{
        return(
            api.Relation.create(
                database=src.database, schema=src.schema, identifier=src.identifier
            )
        )
    }}
{% endmacro %}
