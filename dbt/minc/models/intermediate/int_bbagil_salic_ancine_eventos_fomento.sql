{{ config(materialized='table') }}

-- Intermediate — espinha unificada da Meta 5: um evento por vez que um agente
-- cultural recebeu recurso de uma política federal de fomento, num formato
-- só, vindo de fontes diferentes. É a tabela que permite responder "este
-- agente já havia sido contemplado antes?" olhando ACIMA do mecanismo — que
-- é exatamente o que a pergunta pede ("novos agentes que não haviam sido
-- anteriormente contemplados", sem qualificar por programa).
--
-- Grão: 1 linha por (documento, mecanismo, data do evento, valor).
--
-- MECANISMOS E SUAS JANELAS DE OBSERVAÇÃO — a assimetria aqui é a limitação
-- mais importante do indicador, e está materializada na coluna
-- ano_inicio_observacao para que a gold possa marcar os anos censurados:
--   ROUANET      1993+  captação real (SALIC)         — praticamente completa
--   AUDIOVISUAL  2005+  Lei do Audiovisual, FUNCINES e editais Ancine
--   FSA          2011+  Fundo Setorial do Audiovisual (ancine.consulta)
--   LAB          2023+  movimento nas contas SECULT-A BLANC. A Aldir Blanc
--                       EMERGENCIAL (2020-21) esta FORA DO ESCOPO por decisao
--                       de negocio de 20/08/2026: foi resposta emergencial de
--                       pandemia, nao fomento. Sua ausencia nao e lacuna.
--   LPG          2023+  Lei Paulo Gustavo             — nasceu em 2023
--   PNAB         2023+  Política Nacional Aldir Blanc — nasceu em 2023
--
-- Consequência: um agente cujo único acesso anterior foi por fomento estadual
-- ou municipal aparece como novo entrante. Os dois exemplos que esta nota
-- trazia caíram em 20/08/2026: o FSA entrou na espinha via ancine.consulta, e
-- a Aldir Blanc emergencial saiu do escopo por decisão de negócio. "Novo entrante" significa "nenhuma evidência de acesso anterior
-- nas fontes disponíveis", não "verificamos que nunca acessou". O erro é
-- sempre para o mesmo lado — fonte que falta só transforma veterano em falso
-- novo entrante, nunca o contrário — então os percentuais desta série são
-- TETO, e cada fonte nova só pode derrubá-los.
--
-- PISO DE VALOR: o BB Ágil carrega resíduo de transação que não é repasse de
-- fomento de verdade (tarifas, ajustes). O piso de {{ var('limiar_valor_fomento', 375) }}
-- reais é aplicado sobre o TOTAL do agente naquele mecanismo, não por
-- transação — quem recebeu menos que isso não conta como atendido por aquele
-- mecanismo. Não se aplica à Rouanet: lá o evento é um recibo de captação
-- formal, não há esse tipo de ruído.

WITH eventos_bbagil AS (
    SELECT
        beneficiario_documento,
        programa_fomento,
        data_pagamento AS data_evento,
        valor
    FROM {{ ref('silver_pagamento_bbagil') }}
),

-- O piso é avaliado por (agente × mecanismo) para que a decisão "este agente
-- foi atendido por este mecanismo" seja uma só, e não varie transação a
-- transação. Avaliar por transação removeria parcelas pequenas de um repasse
-- legítimo e poderia adiantar artificialmente a data de entrada do agente.
bbagil_acima_piso AS (
    SELECT beneficiario_documento, programa_fomento
    FROM eventos_bbagil
    GROUP BY beneficiario_documento, programa_fomento
    HAVING SUM(valor) >= {{ var('limiar_valor_fomento', 375) }}
),

bbagil_elegivel AS (
    SELECT e.beneficiario_documento, e.programa_fomento, e.data_evento, e.valor
    FROM eventos_bbagil e
    INNER JOIN bbagil_acima_piso p
        ON e.beneficiario_documento = p.beneficiario_documento
        AND e.programa_fomento = p.programa_fomento
),

rouanet AS (
    SELECT beneficiario_documento, programa_fomento, data_evento, valor
    FROM {{ ref('silver_sac_captacao_proponente') }}
),

-- FSA e demais mecanismos federais do audiovisual. Como na Rouanet, o evento
-- já é uma captação formal: não passa pelo piso, que existe só para separar
-- repasse de resíduo no extrato bancário.
ancine AS (
    SELECT beneficiario_documento, programa_fomento, data_evento, valor
    FROM {{ ref('silver_consulta') }}
),

unificado AS (
    SELECT * FROM bbagil_elegivel
    UNION ALL
    SELECT * FROM rouanet
    UNION ALL
    SELECT * FROM ancine
)

SELECT
    beneficiario_documento,
    programa_fomento,
    data_evento,
    EXTRACT(YEAR FROM data_evento)::INT AS ano_evento,
    valor,
    CASE
        WHEN programa_fomento = 'ROUANET' THEN 'SALIC'
        WHEN programa_fomento IN ('FSA', 'AUDIOVISUAL') THEN 'ANCINE'
        ELSE 'BB_AGIL'
    END AS fonte,
    -- primeiro ano em que a fonte daquele mecanismo consegue enxergar
    -- qualquer coisa; a gold usa isto para marcar o ano censurado, onde todo
    -- mundo parece estreante porque não existe passado observável
    CASE
        WHEN programa_fomento = 'ROUANET' THEN 1993
        WHEN programa_fomento = 'AUDIOVISUAL' THEN 2005
        WHEN programa_fomento = 'FSA' THEN 2011
        WHEN programa_fomento = 'LAB' THEN 2023
        WHEN programa_fomento = 'LPG' THEN 2023
        WHEN programa_fomento = 'PNAB' THEN 2023
    END AS ano_inicio_observacao,
    CASE
        WHEN LENGTH(beneficiario_documento) = 11 THEN 'PF'
        ELSE 'PJ'
    END AS tipo_pessoa
FROM unificado
WHERE beneficiario_documento IS NOT NULL
  AND data_evento IS NOT NULL
