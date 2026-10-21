{{ config(enabled=false, materialized='view') }}

-- DESABILITADO. Depende de relatorio_gestao.planilha_contemplados_lpg e
-- planilha_contemplados_pnab_ciclo_1, que não existem no banco: elas só nascem
-- da cadeia de anexos do TransfereGov (api_planos_acao_dag →
-- api_relatorios_gestao_dag → api_anexos_relatorios_dag →
-- download_anexos_transferegov_dag → extracao_anexos_dag), toda com trigger
-- manual e nenhuma delas rodada aqui.
--
-- Por que ficou fora do caminho da Meta 5: as perguntas 1, 2.1, 2.2 e 2.3 são
-- respondidas por eixo2_meta5_primeiro_acesso_anual,
-- eixo2_meta5_trajetoria_fomento_direto_rouanet e
-- eixo2_meta5_conversao_direto_rouanet_coorte, e nenhum dos três encosta em
-- contemplados — a linhagem deles é plano de ação → BB Ágil, mais SALIC e
-- Ancine. O único
-- dependente deste modelo é int_relatorio_gestao_primeiro_acesso_contemplados
-- (e, depois dele, eixo2_meta5_primeiro_acesso_contemplados), desabilitados junto.
--
-- O que se perde enquanto estiver assim: a aferição de cobertura. Este par de
-- modelos é o que compara "quem o edital selecionou" com "quem apareceu no
-- extrato" — a distância medida era ~46% na LPG e ~89% no PNAB, e é o que
-- sustenta a ressalva de que o indicador enxerga só quem recebeu por conta do
-- Banco do Brasil. Sem eles o número da Meta 5 continua correto, mas a
-- limitação dele deixa de ser mensurável.
--
-- Para reativar: rode a cadeia de anexos acima e troque para enabled=true aqui,
-- nas duas bronze_planilha_contemplados_* desta pasta, em
-- int_relatorio_gestao_primeiro_acesso_contemplados e em
-- eixo2_meta5_primeiro_acesso_contemplados.

-- Silver — unifica os identificadores (CPF/CNPJ normalizados) das listas
-- oficiais de contemplados de edital, LPG + PNAB, para que
-- int_relatorio_gestao_primeiro_acesso_contemplados possa restringir o
-- indicador a quem foi de fato selecionado.
--
-- As bronzes entram por ref() montado no laço abaixo; a linha depends_on
-- deixa a linhagem explícita para o dbt e para quem lê o arquivo.
-- depends_on: {{ ref('bronze_planilha_contemplados_lpg') }}
-- depends_on: {{ ref('bronze_planilha_contemplados_pnab_ciclo_1') }}
--
-- COLUNAS "FANTASMA": a ingestão dinâmica de planilhas (extracao_planilhas.py)
-- não normaliza espaços nem caracteres invisíveis nos nomes de coluna, então
-- o mesmo cabeçalho em arquivos diferentes vira colunas distintas no
-- Postgres, cada uma com parte dos dados. O que existe hoje, de fato:
--   planilha_contemplados_lpg              "cpf ou cnpj", "cpf ou cnpj<NBSP>",
--                                          "cnpj<NBSP>"
--   planilha_contemplados_pnab_ciclo_1     "cpf", "cnpj", "cnpj<SP>",
--                                          "cpf/ cnpj", "cpf ou cnpj<NBSP>"
-- (<NBSP> = U+00A0, espaço não separável — não é o espaço comum.)
--
-- As duas listas do PNAB (geral e Cultura Viva) viraram fatias da mesma
-- tabela, separadas por `tabela_origem`. Aqui não é preciso filtrar por
-- fatia: as duas são PNAB e o resultado é DISTINCT, então varrer as colunas
-- de documento da tabela inteira produz exatamente o mesmo conjunto.
--
-- Por isso a resolução é dinâmica via information_schema, e não uma lista
-- fixa de nomes. O padrão anterior, ILIKE '%cpf%cnpj%', exigia "cpf" ANTES de
-- "cnpj" na mesma string: não casava "cnpj" sozinho nem "cpf/ cnpj", e
-- deixava de fora ~2.100 contemplados do PNAB (a maioria deles COM pagamento
-- comprovado no extrato bancário) e as colunas só-CNPJ da LPG.
--
-- Colunas "cpf (anonimizado)" são excluídas de propósito: trazem
-- ***XXXXXX** em vez do documento, e não servem para cruzar com nada.
--
-- NORMALIZAÇÃO: só dígitos + LPAD para 11 (CPF) ou 14 (CNPJ). O LPAD é
-- obrigatório porque o lado bancário sempre normaliza assim; sem ele um CPF
-- que chegou com 10 dígitos (sem o zero à esquerda) nunca casa.

{% set fontes = [
    ('planilha_contemplados_lpg', 'LPG'),
    ('planilha_contemplados_pnab_ciclo_1', 'PNAB')
] %}

{# lista plana de (tabela, programa, coluna) para gerar um UNION ALL simples #}
{% set pares = [] %}
{% for tabela, programa in fontes %}
    {% set src = ref('bronze_' ~ tabela) %}
    {% set consulta %}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{{ src.schema }}'
          AND table_name = '{{ src.identifier }}'
          AND (column_name ILIKE '%cpf%' OR column_name ILIKE '%cnpj%')
          AND column_name NOT ILIKE '%anonimizado%'
        ORDER BY column_name
    {% endset %}
    {% if execute %}
        {% for col in run_query(consulta).columns[0].values() %}
            {% do pares.append((tabela, programa, col)) %}
        {% endfor %}
    {% endif %}
{% endfor %}

{#
  Sem esta guarda o modelo falha da pior maneira possível quando as tabelas de
  planilha não existem (banco onde as DAGs de conformidade ainda não rodaram):
  a varredura do information_schema devolve zero colunas, o UNION ALL sai vazio
  e o modelo compila `WITH bruto AS ()` — SQL inválido, que só quebra no
  `dbt run` com um erro de sintaxe que não diz nada sobre a causa.
#}
{% if execute and pares | length == 0 %}
    {{ exceptions.raise_compiler_error(
        "silver_planilha_contemplados: nenhuma coluna de CPF/CNPJ encontrada em "
        ~ fontes | map(attribute=0) | join(', ') ~ ". As tabelas de contemplados "
        ~ "existem no banco alvo? Rode as DAGs de relatorio_gestao antes deste modelo."
    ) }}
{% endif %}

WITH bruto AS (
    {% for tabela, programa, col in pares %}
    SELECT
        NULLIF(TRIM("{{ col }}"), '') AS doc_bruto,
        '{{ programa }}' AS programa_fomento
    FROM {{ ref('bronze_' ~ tabela) }}
    {%- if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

limpo AS (
    SELECT
        REGEXP_REPLACE(doc_bruto, '[^0-9]', '', 'g') AS digitos,
        programa_fomento
    FROM bruto
    WHERE doc_bruto IS NOT NULL
      -- linhas de cabeçalho repetido e placeholders de leitura de planilha
      AND LOWER(doc_bruto) NOT IN ('nan', 'none', 'null', 'cpf', 'cnpj', 'cpf ou cnpj', 'cpf/ cnpj')
)

SELECT DISTINCT
    CASE
        WHEN LENGTH(digitos) <= 11 THEN LPAD(digitos, 11, '0')
        ELSE LPAD(digitos, 14, '0')
    END AS id_normalizado,
    programa_fomento
FROM limpo
-- 9 dígitos é o menor CPF plausível já visto sem zeros à esquerda; abaixo
-- disso é lixo de parsing (número de ordem, ano, célula vazia lida como 0)
WHERE LENGTH(digitos) BETWEEN 9 AND 14
  AND digitos !~ '^0+$'
