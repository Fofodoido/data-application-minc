{{ config(materialized="table") }}

-- Silver — uma linha por captação de projeto audiovisual registrada pela
-- Ancine (ancine.consulta, via bronze_consulta), tipada e com o documento do
-- proponente normalizado. Todos os mecanismos ficam; escolher quais contam
-- para um indicador é da Gold de cada produto.
--
-- A tabela tem a mesma estrutura da captação da Rouanet: `cnpj_proponente` é
-- quem RECEBEU e `cnpj_investidor` é quem aportou.
--
-- NOMES DE COLUNA: a tabela é carregada FORA deste pipeline, direto da planilha
-- da Ancine, e os cabeçalhos entram no banco como estão lá — com maiúscula,
-- espaço e acento: "Mecanismo", "CNPJ Proponente", "Data Captação", "Valor da
-- Captação", "No SALIC". Uma delas, " UF Proponente", ainda começa com espaço.
-- O Postgres rebaixa identificador sem aspas para minúscula, então ler
-- `mecanismo` direto falha com `column "mecanismo" does not exist`. O CTE
-- `origem` resolve cada nome real via information_schema (macro
-- coalesce_por_nome) e devolve os nomes em snake_case — e continua valendo se
-- a planilha for recarregada com o cabeçalho normalizado.
--
-- Os nomes são procurados na tabela de origem, e não na bronze_consulta: a
-- bronze é modelo, e num ambiente novo ela ainda não existe na hora do
-- `dbt compile` (macro origem_da_bronze).
--
-- ARMADILHAS DE FORMATO, as duas medidas nesta tabela:
-- • data_captacao: TEXT no padrão AMERICANO M/D/YYYY. '7/12/2007' é 12 de
-- julho, não 7 de dezembro. Ordenar como texto mente.
-- • valor_de_captacao: TEXT no padrão brasileiro, '50.000,00'.
-- Linha fora desses padrões fica de fora: sem data ou sem valor, a captação
-- não é utilizável.
--
-- DOCUMENTO: a tabela é quase toda CNPJ (31.528 linhas de 14 dígitos) e tem 4
-- linhas com CPF. Proponente pessoa física é, na prática, invisível aqui.
{% set src = ref("bronze_consulta") %}
{% set colunas_de = origem_da_bronze("bronze_consulta") if execute else src %}
{% set col = {
    "mecanismo": coalesce_por_nome(colunas_de, ["mecanismo"]),
    "cnpj_proponente": coalesce_por_nome(
        colunas_de, ["cnpj proponente", "cnpj_proponente"]
    ),
    "nome_proponente": coalesce_por_nome(
        colunas_de, ["nome proponente", "nome_proponente"]
    ),
    "data_captacao": coalesce_por_nome(
        colunas_de, ["data captação", "data captacao", "data_captacao"]
    ),
    "valor_de_captacao": coalesce_por_nome(
        colunas_de,
        ["valor da captação", "valor da captacao", "valor_de_captacao"],
    ),
    "no_salic": coalesce_por_nome(colunas_de, ["no salic", "no_salic"]),
} %}
{#
  Sem esta guarda o modo de falha é o pior possível. coalesce_por_nome degrada
  para `cast(null as text)` quando não encontra a coluna: o WHERE não casaria
  nada e o modelo entregaria ZERO linhas, sem nenhum erro. Falhar na
  compilação é o comportamento correto.

  Só roda com execute=true: em `dbt parse` o adapter não consulta o banco e
  todas as colunas voltariam "não resolvidas" por construção.
#}
{% if execute %}
    {% set nao_resolvidas = [] %}
    {% for nome, expressao in col.items() %}
        {% if "cast(null as text)" in expressao %}
            {% do nao_resolvidas.append(nome) %}
        {% endif %}
    {% endfor %}
    {% if nao_resolvidas %}
        {{
            exceptions.raise_compiler_error(
                "silver_consulta: não achei em ancine.consulta a(s) coluna(s) "
                ~ nao_resolvidas
                | join(", ")
                ~ ". Veja os cabeçalhos reais com: SELECT column_name FROM "
                ~ "information_schema.columns WHERE table_schema = 'ancine' "
                ~ "AND table_name = 'consulta';"
            )
        }}
    {% endif %}
{% endif %}

with
    origem as (
        select
            {{ col["mecanismo"] }} as mecanismo,
            {{ col["cnpj_proponente"] }} as cnpj_proponente,
            {{ col["nome_proponente"] }} as nome_proponente,
            {{ col["data_captacao"] }} as data_captacao,
            {{ col["valor_de_captacao"] }} as valor_de_captacao,
            {{ col["no_salic"] }} as no_salic
        from {{ src }}
    ),

    tipado as (
        select
            mecanismo,
            case
                when
                    length(regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g')) <= 11
                then
                    lpad(
                        regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'), 11, '0'
                    )
                else
                    lpad(
                        regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'), 14, '0'
                    )
            end as proponente_documento,
            nullif(trim(nome_proponente), '') as proponente_nome,
            case
                when data_captacao ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                then to_date(data_captacao, 'FMMM/FMDD/YYYY')
            end as data_captacao,
            case
                when valor_de_captacao ~ '^-?[0-9.]+,[0-9]{2}$'
                then replace(replace(valor_de_captacao, '.', ''), ',', '.')::numeric
            end as valor_captacao,
            nullif(trim(no_salic), '') as no_salic
        from origem
        where
            cnpj_proponente is not null
            and length(regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'))
            between 8 and 14
    )

select
    proponente_documento,
    proponente_nome,
    mecanismo,
    data_captacao,
    valor_captacao,
    no_salic
from tipado
where
    proponente_documento !~ '^0+$'
    and data_captacao is not null
    and valor_captacao is not null
