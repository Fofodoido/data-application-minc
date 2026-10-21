{{ config(materialized='table') }}

-- Silver — unifica extrato-folha (silver_pagamento_extrato_bbagil, já sem as
-- transações com subtransactionquantity > 0) e sublançamentos pagos
-- (silver_pagamento_subtransacao_bbagil) em 1 linha por saída de dinheiro
-- para um beneficiário identificado.
--
-- Não classifica programa nem decide quem conta como agente cultural: isso é
-- regra de negócio e mora na Gold (ADR 0010). O código do programa sai como a
-- origem devolve (programa_curto, o governmentprogramname do BSC), e as
-- colunas de que os filtros de negócio precisam vêm junto: beneficiario_nome,
-- beneficiario_documento_bruto, descriptionname e tipo_lancamento, que diz de
-- qual dos dois lados a linha veio.
--
-- Cobertura: só cobre os programas extraídos pela extracao_bbagil_dag
-- (Variable transferegov_programas_ids).

WITH pagamentos AS (
    SELECT
        'extrato' AS tipo_lancamento,
        id_plano_acao,
        id_plano_acao_dado_bancario,
        programa_curto,
        beneficiario_documento_bruto,
        beneficiario_documento,
        beneficiario_nome,
        descriptionname,
        valor,
        data_pagamento
    FROM {{ ref('silver_pagamento_extrato_bbagil') }}

    UNION ALL

    -- o sublançamento não tem descrição própria: a de lançamento interno
    -- (aplicação, resgate, imposto) é atributo da linha do extrato
    SELECT
        'subtransacao' AS tipo_lancamento,
        id_plano_acao,
        id_plano_acao_dado_bancario,
        programa_curto,
        beneficiario_documento_bruto,
        beneficiario_documento,
        beneficiario_nome,
        NULL::TEXT AS descriptionname,
        valor,
        data_pagamento
    FROM {{ ref('silver_pagamento_subtransacao_bbagil') }}
)

SELECT *
FROM pagamentos
WHERE beneficiario_documento IS NOT NULL
  AND data_pagamento IS NOT NULL
