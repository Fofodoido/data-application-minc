{{ config(materialized='table') }}

-- Silver — extrato bancário do BB Gestão Ágil (bronze_extrato_bbagil), filtrado para
-- manter só pagamentos reais a beneficiários finais. Porta pra SQL os 8
-- filtros sequenciais de plugins/regras_negocio_bbagil.py::pipeline_filtro_extrato
-- (módulo órfão em Python, mantido só como referência/histórico dessas regras):
--   1. beneficiário válido (documento != '0') e sem subtransações (as com
--      subtransactionquantity > 0 são tratadas em silver_pagamento_subtransacao_bbagil,
--      pra não contar o mesmo pagamento 2x).
--   2. remove transferências para o próprio Banco do Brasil (documento '191').
--   3. remove transações internas/impostos/aplicações automáticas (descriptionname).
--   4. remove devoluções de saldo ao Fundo Nacional de Cultura (CNPJ 3793086100189).
--   5. remove pares crédito/débito do mesmo plano de ação + documento + valor
--      (estorno no mesmo dia ou devolução futura de um débito anterior).
--   6. mantém só débitos (saída de dinheiro da conta do ente pro beneficiário).
--   7. remove repasses entre entes públicos (nome do beneficiário começa com
--      MUNICIPIO/ESTADO/FUNDO/SECRETARIA/SEFAZ — são repasses administrativos,
--      não pagamento a agente cultural).
--
-- beneficiario_documento normalizado via beneficiarypersontype (1=PF/CPF,
-- 2=PJ/CNPJ) + LPAD, porque uma fração relevante das linhas chega sem o
-- zero à esquerda (CNPJ com 12-13 dígitos, CPF com 10).

WITH base AS (
    SELECT
        id_plano_acao,
        id_plano_acao_dado_bancario,
        id,
        governmentprogramname AS programa_curto,
        beneficiarydocumentid AS beneficiario_documento_bruto,
        CASE
            WHEN beneficiarypersontype = '1'
                THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
            WHEN beneficiarypersontype = '2'
                THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        beneficiaryname AS beneficiario_nome,
        creditdebitindicator,
        value::NUMERIC AS valor,
        TO_DATE(bookingdate, 'DD/MM/YYYY') AS data_pagamento,
        descriptionname
    FROM {{ ref('bronze_extrato_bbagil') }}
    WHERE subtransactionquantity = '0'
      AND beneficiarydocumentid IS NOT NULL
      AND beneficiarydocumentid NOT IN ('0', '191')
),

-- Chaves (conta + documento bruto + valor) que aparecem como débito E
-- crédito — estorno/devolução. Descarta os dois lados do par.
--
-- A chave é a CONTA, não o plano de ação: um plano tem mais de uma conta (os
-- da LPG têm duas) e estorno acontece dentro de uma conta só. Casando no
-- nível do plano, um débito da conta A e um crédito da conta B viram "par" e
-- os DOIS lados são descartados — perda silenciosa de pagamento real, no
-- modelo que existe justamente para não perder.
chaves_estorno AS (
    SELECT id_plano_acao, id_plano_acao_dado_bancario, beneficiario_documento_bruto, valor
    FROM base
    WHERE creditdebitindicator = 'D'
    INTERSECT
    SELECT id_plano_acao, id_plano_acao_dado_bancario, beneficiario_documento_bruto, valor
    FROM base
    WHERE creditdebitindicator = 'C'
)

SELECT
    b.id_plano_acao,
    b.id_plano_acao_dado_bancario,
    b.id,
    b.programa_curto,
    b.beneficiario_documento,
    b.beneficiario_nome,
    b.valor,
    b.data_pagamento
FROM base b
LEFT JOIN chaves_estorno ce
    ON b.id_plano_acao = ce.id_plano_acao
    AND b.id_plano_acao_dado_bancario = ce.id_plano_acao_dado_bancario
    AND b.beneficiario_documento_bruto = ce.beneficiario_documento_bruto
    AND b.valor = ce.valor
WHERE b.creditdebitindicator = 'D'
  AND ce.id_plano_acao IS NULL
  AND b.beneficiario_documento IS NOT NULL
  AND b.beneficiario_documento_bruto != '3793086100189'
  AND b.descriptionname NOT IN (
      'BB-APLIC C.PRZ-APL.AUT', 'Resgate Automatico', 'Impostos',
      'ORDEM BANC CANCELADA', 'Ordem Bancaria', 'Resgate BB Fix',
      'CREDITO CONVENIO', 'Estorno Resgate Automatico'
  )
  AND NOT (
      UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'MUNICIPIO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'ESTADO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'FUNDO%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'SECRETARIA%'
      OR UPPER(COALESCE(b.beneficiario_nome, '')) LIKE 'SEFAZ%'
  )
