{{ config(materialized='table') }}

-- Silver — sublançamentos do BB Gestão Ágil (bronze_subtransacao_bbagil), usados quando a
-- transação-mãe do extrato tem subtransactionquantity > 0 (o beneficiário
-- real do pagamento só aparece aqui, não na linha agregada do extrato).
-- Porta pra SQL os 5 filtros de
-- plugins/regras_negocio_bbagil.py::pipeline_filtro_subtransacoes:
--   1. beneficiário válido (documento != '0').
--   2. mantém só sublançamentos com status 'Pago' (accountability).
--   3. valor em módulo (subtransação pode vir negativa na fonte).
--   4. remove repasses entre entes públicos (mesmo critério do extrato).
--
-- Contexto de programa (governmentprogramname) vem herdado da
-- transação-mãe via id_plano_acao + id_plano_acao_dado_bancario +
-- id_transacao_pai = id. A CONTA (id_plano_acao_dado_bancario) faz parte do
-- join de propósito: um plano de ação tem mais de uma conta (os da LPG têm
-- duas) e o `id` da transação não é único entre elas — sem a conta, o join
-- vira 1→N e cada sublançamento é contado duas vezes, inflando `valor` em
-- silver_pagamento_bbagil e int_bbagil_salic_ancine_eventos_fomento.

SELECT
    s.id_plano_acao,
    s.id_plano_acao_dado_bancario,
    e.governmentprogramname AS programa_curto,
    CASE
        WHEN s.beneficiarypersontype = '1'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
        WHEN s.beneficiarypersontype = '2'
            THEN LPAD(REGEXP_REPLACE(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
    END AS beneficiario_documento,
    s.beneficiaryname AS beneficiario_nome,
    ABS(s.value::NUMERIC) AS valor,
    TO_DATE(s.paymentdate, 'DD/MM/YYYY') AS data_pagamento
FROM {{ ref('bronze_subtransacao_bbagil') }} s
JOIN {{ ref('bronze_extrato_bbagil') }} e
    ON s.id_plano_acao = e.id_plano_acao
    AND s.id_plano_acao_dado_bancario = e.id_plano_acao_dado_bancario
    AND s.id_transacao_pai = e.id
WHERE s.beneficiarydocumentid IS NOT NULL
  AND s.beneficiarydocumentid != '0'
  AND s.subtransactionaccountabilityname = 'Pago'
  AND NOT (
      UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'MUNICIPIO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'ESTADO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'FUNDO%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'SECRETARIA%'
      OR UPPER(COALESCE(s.beneficiaryname, '')) LIKE 'SEFAZ%'
  )
