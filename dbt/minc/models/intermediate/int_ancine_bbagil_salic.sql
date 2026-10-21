{{ config(materialized='table') }}

-- Intermediate — junta numa forma só os eventos em que um agente recebeu
-- recurso, vindos de três fontes:
--   BB_AGIL  saída de dinheiro das contas dos planos de ação (silver_pagamento_bbagil)
--   SALIC    recibo de captação da Lei Rouanet (silver_legado_sac_captacao_proponente)
--   ANCINE   captação de projeto audiovisual (silver_consulta)
--
-- Grão: 1 linha por evento da fonte. Nada é descartado nem classificado aqui
-- (ADR 0010): quais eventos contam, a que política cada um pertence e o piso
-- de valor são regra de negócio e ficam na Gold. Por isso cada linha leva,
-- além do comum (documento, data, valor), o que a fonte tem para essa
-- decisão — programa_bruto, beneficiario_nome, beneficiario_documento_bruto,
-- descriptionname, tipo_lancamento —, nulo onde a fonte não tem.

WITH bbagil AS (
    SELECT
        'BB_AGIL' AS fonte,
        programa_curto AS programa_bruto,
        beneficiario_documento,
        beneficiario_documento_bruto,
        beneficiario_nome,
        data_pagamento AS data_evento,
        valor,
        tipo_lancamento,
        descriptionname,
        id_plano_acao,
        NULL::TEXT AS pronac,
        NULL::TEXT AS no_salic
    FROM {{ ref('silver_pagamento_bbagil') }}
),

salic AS (
    SELECT
        'SALIC' AS fonte,
        NULL::TEXT AS programa_bruto,
        proponente_documento AS beneficiario_documento,
        NULL::TEXT AS beneficiario_documento_bruto,
        proponente_nome AS beneficiario_nome,
        data_recibo AS data_evento,
        valor_captado AS valor,
        NULL::TEXT AS tipo_lancamento,
        NULL::TEXT AS descriptionname,
        NULL::INTEGER AS id_plano_acao,
        pronac,
        NULL::TEXT AS no_salic
    FROM {{ ref('silver_legado_sac_captacao_proponente') }}
),

ancine AS (
    SELECT
        'ANCINE' AS fonte,
        mecanismo AS programa_bruto,
        proponente_documento AS beneficiario_documento,
        NULL::TEXT AS beneficiario_documento_bruto,
        proponente_nome AS beneficiario_nome,
        data_captacao AS data_evento,
        valor_captacao AS valor,
        NULL::TEXT AS tipo_lancamento,
        NULL::TEXT AS descriptionname,
        NULL::INTEGER AS id_plano_acao,
        NULL::TEXT AS pronac,
        no_salic
    FROM {{ ref('silver_consulta') }}
),

unificado AS (
    SELECT * FROM bbagil
    UNION ALL
    SELECT * FROM salic
    UNION ALL
    SELECT * FROM ancine
)

SELECT
    fonte,
    programa_bruto,
    beneficiario_documento,
    beneficiario_documento_bruto,
    beneficiario_nome,
    CASE
        WHEN LENGTH(beneficiario_documento) = 11 THEN 'PF'
        ELSE 'PJ'
    END AS tipo_pessoa,
    data_evento,
    EXTRACT(YEAR FROM data_evento)::INT AS ano_evento,
    valor,
    tipo_lancamento,
    descriptionname,
    id_plano_acao,
    pronac,
    no_salic
FROM unificado
WHERE beneficiario_documento IS NOT NULL
  AND data_evento IS NOT NULL
