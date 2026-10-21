{{ config(materialized='table') }}

-- Silver — sublançamentos pagos do BB Gestão Ágil, usados quando a
-- transação-mãe do extrato tem subtransactionquantity > 0 (o beneficiário
-- real do pagamento só aparece aqui, não na linha agregada do extrato). Lê as
-- silvers tipadas e só limpa:
--   1. descarta beneficiário ausente (documento nulo ou '0').
--   2. fica só o sublançamento com status 'Pago' (accountability): os outros
--      não chegaram ao beneficiário.
--   3. valor em módulo (subtransação pode vir negativa na fonte).
--
-- Repasse a ente público continua aqui, com beneficiario_nome, para a Gold
-- decidir (ADR 0010, regra de negócio só na Gold).
--
-- Contexto de programa (governmentprogramname) vem herdado da
-- transação-mãe via id_plano_acao + id_plano_acao_dado_bancario +
-- id_transacao_pai = id. A CONTA (id_plano_acao_dado_bancario) faz parte do
-- join de propósito: um plano de ação tem mais de uma conta (os da LPG têm
-- duas) e o `id` da transação não é único entre elas — sem a conta, o join
-- vira 1→N e cada sublançamento é contado duas vezes.
--
-- As colunas da silver_subtransacao_bbagil são todas texto (a tabela não pôde
-- ser perfilada), então as chaves do join e o valor ganham cast aqui, com a
-- mesma guarda das macros bronze_*.

SELECT
    {{ bronze_inteiro("s.id_plano_acao") }} AS id_plano_acao,
    {{ bronze_inteiro("s.id_plano_acao_dado_bancario") }} AS id_plano_acao_dado_bancario,
    e.governmentprogramname AS programa_curto,
    s.beneficiarydocumentid AS beneficiario_documento_bruto,
    CASE
        WHEN s.beneficiarypersontype = '1'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
        WHEN s.beneficiarypersontype = '2'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
    END AS beneficiario_documento,
    s.beneficiaryname AS beneficiario_nome,
    ABS({{ bronze_numerico("s.value") }}) AS valor,
    TO_DATE(s.paymentdate, 'DD/MM/YYYY') AS data_pagamento
FROM {{ ref('silver_subtransacao_bbagil') }} s
JOIN {{ ref('silver_extrato_bbagil') }} e
    ON {{ bronze_inteiro("s.id_plano_acao") }} = e.id_plano_acao
    AND {{ bronze_inteiro("s.id_plano_acao_dado_bancario") }} = e.id_plano_acao_dado_bancario
    AND {{ bronze_inteiro("s.id_transacao_pai", "bigint") }} = e.id
WHERE s.beneficiarydocumentid IS NOT NULL
  AND s.beneficiarydocumentid != '0'
  AND s.subtransactionaccountabilityname = 'Pago'
