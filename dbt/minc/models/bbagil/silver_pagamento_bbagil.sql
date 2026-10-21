{{ config(materialized='table') }}

-- Silver — unifica os lançamentos do extrato-folha
-- (silver_pagamento_extrato_bbagil, já sem as transações com
-- subtransactionquantity > 0) e os sublançamentos
-- (silver_pagamento_subtransacao_bbagil) em 1 linha por movimento de dinheiro
-- com beneficiário informado.
--
-- Não decide o que é pagamento efetivo, não classifica programa nem decide
-- quem conta como agente cultural: isso é regra de negócio e mora na Gold
-- (ADR 0010). Por isso as linhas chegam como a limpeza deixou — crédito e
-- débito do extrato, sublançamento de qualquer status, data ou documento
-- normalizado nulos — e com as colunas de que a Gold precisa:
--   • para parear estorno: id_plano_acao_dado_bancario (a conta),
--     beneficiario_documento_bruto, valor e creditdebitindicator;
--   • para o recorte de elegibilidade: status_prestacao_contas;
--   • para quem conta: beneficiario_nome, descriptionname, programa_curto
--     (o governmentprogramname do BSC, sem classificar);
--   • tipo_lancamento, que diz de qual dos dois lados a linha veio.
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
        creditdebitindicator,
        NULL::TEXT AS status_prestacao_contas,
        valor,
        data_pagamento
    FROM {{ ref('silver_pagamento_extrato_bbagil') }}

    UNION ALL

    -- o sublançamento não tem descrição própria (a de lançamento interno —
    -- aplicação, resgate, imposto — é atributo da linha do extrato) nem
    -- indicador de débito/crédito: é sempre saída para o beneficiário
    SELECT
        'subtransacao' AS tipo_lancamento,
        id_plano_acao,
        id_plano_acao_dado_bancario,
        programa_curto,
        beneficiario_documento_bruto,
        beneficiario_documento,
        beneficiario_nome,
        NULL::TEXT AS descriptionname,
        NULL::TEXT AS creditdebitindicator,
        status_prestacao_contas,
        valor,
        data_pagamento
    FROM {{ ref('silver_pagamento_subtransacao_bbagil') }}
)

SELECT * FROM pagamentos
