{{ config(materialized='table') }}

-- Silver — saídas de dinheiro do extrato bancário do BB Gestão Ágil, uma linha
-- por lançamento a débito que chegou a um beneficiário identificado. Lê a
-- silver tipada (silver_extrato_bbagil) e só limpa:
--   1. fica só o lançamento sem subtransações (subtransactionquantity = 0).
--      Os que têm subtransação são abertos em silver_pagamento_subtransacao_bbagil;
--      mantê-los aqui contaria o mesmo dinheiro duas vezes.
--   2. descarta beneficiário ausente (documento nulo ou '0'), que é lixo da
--      origem, não pagamento.
--   3. descarta os dois lados de um estorno: crédito e débito da mesma conta,
--      com o mesmo documento e o mesmo valor.
--   4. fica só o débito, que é a saída de dinheiro da conta.
--
-- Quem CONTA como pagamento a agente cultural não é decidido aqui (ADR 0010,
-- regra de negócio só na Gold). Repasse a ente público, devolução ao Fundo
-- Nacional de Cultura, transferência ao próprio Banco do Brasil e lançamentos
-- internos (aplicação, resgate, imposto) continuam nesta tabela, com
-- beneficiario_nome, beneficiario_documento_bruto e descriptionname para quem
-- for filtrar. Os critérios são os de
-- plugins/regras_negocio_bbagil.py::pipeline_filtro_extrato.
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
            WHEN beneficiarypersontype = 1
                THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
            WHEN beneficiarypersontype = 2
                THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        beneficiaryname AS beneficiario_nome,
        creditdebitindicator,
        descriptionname,
        value AS valor,
        TO_DATE(bookingdate, 'DD/MM/YYYY') AS data_pagamento
    FROM {{ ref('silver_extrato_bbagil') }}
    WHERE subtransactionquantity = 0
      AND beneficiarydocumentid IS NOT NULL
      AND beneficiarydocumentid != '0'
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
    b.beneficiario_documento_bruto,
    b.beneficiario_documento,
    b.beneficiario_nome,
    b.descriptionname,
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
