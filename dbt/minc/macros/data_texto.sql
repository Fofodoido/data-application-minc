{% macro data_texto(col, ordem='DMY') %}
    {#-
      Data em texto com barra ('7/12/2007', '07/12/2007') para date, sem
      derrubar o modelo. ordem é 'DMY' (padrão brasileiro) ou 'MDY' (o
      americano da planilha da Ancine).

      O to_date do Postgres é estrito: '2/30/2015' ou '13/1/2015' param o run
      com `date/time field value out of range`. Aqui o que não é data de
      calendário vira NULL, como nas macros bronze_*. Os ramos do CASE são
      avaliados em ordem, e é isso que garante que make_date só recebe mês e
      ano válidos. Não usa pg_input_is_valid porque ele só existe a partir do
      Postgres 16, e o Compose local roda o 15.
    -#}
    {%- if ordem == 'DMY' -%}
        {%- set p_dia, p_mes, formato = 1, 2, 'FMDD/FMMM/YYYY' -%}
    {%- elif ordem == 'MDY' -%}
        {%- set p_dia, p_mes, formato = 2, 1, 'FMMM/FMDD/YYYY' -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("data_texto: ordem deve ser 'DMY' ou 'MDY'") }}
    {%- endif -%}
    {%- set t = "trim(" ~ col ~ ")" -%}
    {%- set dia = "split_part(" ~ t ~ ", '/', " ~ p_dia ~ ")::int" -%}
    {%- set mes = "split_part(" ~ t ~ ", '/', " ~ p_mes ~ ")::int" -%}
    {%- set ano = "split_part(" ~ t ~ ", '/', 3)::int" -%}
    case
        when {{ t }} !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' then null
        when {{ mes }} not between 1 and 12 or {{ ano }} < 1 then null
        when
            {{ dia }} not between 1 and extract(
                day from make_date({{ ano }}, {{ mes }}, 1) + interval '1 month - 1 day'
            )
        then null
        else to_date({{ t }}, '{{ formato }}')
    end
{%- endmacro %}
