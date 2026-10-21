{{ config(materialized='table') }}

-- Silver — unifica extrato-folha (silver_pagamento_extrato_bbagil, já sem as
-- transações com subtransactionquantity > 0) e subtransações filtradas
-- (silver_pagamento_subtransacao_bbagil) em 1 linha por pagamento real a um
-- beneficiário. Deriva programa_fomento (LPG/PNAB/LAB) a partir do código
-- curto do programa devolvido pelo BSC (governmentprogramname).
--
-- LAB é um valor próprio, não mais colapsado em PNAB: a pergunta 2.2 da
-- Meta 5 pede a participação de novos entrantes por mecanismo, com LAB,
-- LPG e PNAB separados. Os 8 códigos que o extrato realmente traz:
--   MINC-LPG-{MUNI,EST}-{AUD,OUTRAS}  -> LPG   (desde 2023-06)
--   MINC-PNAB-2023                    -> PNAB  (desde 2023-11)
--   SECULT-A BLANC-{MUN,EST-D,EST-R}  -> LAB   (desde 2023-01)
-- Atenção: esses códigos SECULT-A BLANC são a Lei Aldir Blanc II
-- (Lei 14.399/2022, fomento permanente executado via fundo a fundo).
-- A LAB I emergencial (2020-2021) esta fora do escopo da meta por decisao de
-- negocio de 20/08/2026: foi resposta emergencial de pandemia, nao fomento.
--
-- Cobertura: só cobre pagamentos feitos pelos programas extraídos pela
-- extracao_bbagil_dag (Variable transferegov_programas_ids). Programas
-- fora desse escopo caem em programa_fomento NULL e são descartados aqui
-- — hoje nenhum código presente no extrato cai nesse caso.

WITH pagamentos AS (
    SELECT id_plano_acao, programa_curto, beneficiario_documento, valor, data_pagamento
    FROM {{ ref('silver_pagamento_extrato_bbagil') }}

    UNION ALL

    SELECT id_plano_acao, programa_curto, beneficiario_documento, valor, data_pagamento
    FROM {{ ref('silver_pagamento_subtransacao_bbagil') }}
),

classificado AS (
    SELECT
        id_plano_acao,
        beneficiario_documento,
        valor,
        data_pagamento,
        -- O nome do programa no extrato do BB Agil nao segue convencao: a
        -- PNAB de 2023 vem como 'MINC-PNAB-2023' (hifen) e a de 2025 como
        -- 'MINC_PNAB2025_MUNIC' (underscore). Normalizamos separador e caixa
        -- antes de classificar (underscore E espaco viram hifen: a LAB vem como
        -- 'SECULT-A BLANC-MUN', com espaco). Sem isso os 95.658 pagamentos do ciclo 2
        -- caem no ELSE NULL e sao descartados em silencio pelo filtro final.
        CASE
            WHEN UPPER(TRANSLATE(programa_curto, '_ ', '--')) LIKE 'MINC-LPG%'
                THEN 'LPG'
            WHEN UPPER(TRANSLATE(programa_curto, '_ ', '--')) LIKE '%PNAB%'
                THEN 'PNAB'
            WHEN UPPER(TRANSLATE(programa_curto, '_ ', '--')) LIKE '%A-BLANC%'
                THEN 'LAB'
            ELSE NULL
        END AS programa_fomento
    FROM pagamentos
)

SELECT
    id_plano_acao,
    beneficiario_documento,
    programa_fomento,
    valor,
    data_pagamento
FROM classificado
WHERE beneficiario_documento IS NOT NULL
  AND data_pagamento IS NOT NULL
  AND programa_fomento IS NOT NULL
