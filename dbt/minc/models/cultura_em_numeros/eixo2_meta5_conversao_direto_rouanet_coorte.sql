{{ config(materialized='table') }}

-- Gold — a resposta da pergunta 2.3 da Meta 5 com o tempo de exposição
-- controlado: "há novos entrantes pelo fomento direto que depois acessaram a
-- Lei Rouanet?"
--
-- Grão: 1 linha por (coorte de entrada × porta de entrada × tipo de pessoa).
--
-- POR QUE ESTA TABELA EXISTE SEPARADA DA eixo2_meta5_trajetoria_*: a contagem bruta de
-- quem fez o percurso é baixa e seria lida como "quase ninguém migra". Só que
-- o fomento direto começou em 2023: quem entrou em 2026 teve meses para
-- captar Rouanet, quem entrou em 2023 teve três anos. Comparar as coortes sem
-- descontar essa diferença de exposição confunde "converte pouco" com
-- "entrou ontem". A coluna converteram_ate_12m nivela isso — é a mesma
-- janela de oportunidade para todas as coortes fechadas.
--
-- QUEM ENTRA NO DENOMINADOR: só agente cuja PORTA DE ENTRADA no fomento
-- federal foi LAB, LPG ou PNAB. Não basta ter passado pelo fomento direto
-- antes da Rouanet — quem já tinha aparecido na trilha da Ancine
-- (AUDIOVISUAL/FSA) antes de 2023 não é novo entrante pelo fomento direto, e
-- a pergunta 2.3 é sobre novo entrante. São 21 agentes nessa situação; eles
-- contam em DIRETO_DEPOIS_ROUANET na eixo2_meta5_trajetoria_* (que classifica percurso,
-- não novidade) e ficam de fora daqui.
--
-- LEITURA DO NUMERADOR: 'acessou a Rouanet' aqui é captação real de recurso,
-- a mesma definição de evento usada no resto da meta (ver
-- silver_legado_sac_captacao_proponente) — não é projeto protocolado nem aprovado. Um
-- agente que submeteu projeto e não captou não conta como convertido.

WITH agente AS (
    SELECT
        beneficiario_documento,
        tipo_pessoa,
        MAX(programa_fomento) FILTER (WHERE eh_porta_de_entrada) AS porta_de_entrada,
        MIN(data_primeiro_acesso_geral) AS data_entrada,
        MIN(data_primeiro_acesso_mecanismo)
            FILTER (WHERE programa_fomento = 'ROUANET') AS data_rouanet
    FROM {{ ref('int_bbagil_salic_ancine_primeiro_acesso') }}
    GROUP BY beneficiario_documento, tipo_pessoa
),

novos_entrantes_direto AS (
    SELECT
        *,
        -- a entrada é a porta, então data_entrada É a entrada no direto:
        -- basta a Rouanet vir estritamente depois
        (data_rouanet IS NOT NULL AND data_rouanet > data_entrada) AS converteu,
        data_rouanet - data_entrada AS dias_ate_rouanet
    FROM agente
    WHERE porta_de_entrada IN ('LAB', 'LPG', 'PNAB')
)

SELECT
    EXTRACT(YEAR FROM data_entrada)::INT AS coorte_entrada,
    porta_de_entrada,
    tipo_pessoa,
    COUNT(*) AS entrantes,
    COUNT(*) FILTER (WHERE converteu) AS converteram_rouanet,
    -- janela fixa: comparável entre coortes. A coorte mais recente ainda não
    -- completou 12 meses, então o valor dela é piso, não taxa final.
    COUNT(*) FILTER (WHERE converteu AND dias_ate_rouanet <= 365)
        AS converteram_ate_12m,
    ROUND(100.0 * COUNT(*) FILTER (WHERE converteu) / COUNT(*), 3)
        AS pct_converteram,
    ROUND(100.0 * COUNT(*) FILTER (WHERE converteu AND dias_ate_rouanet <= 365) / COUNT(*), 3)
        AS pct_converteram_ate_12m,
    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY dias_ate_rouanet)
        FILTER (WHERE converteu) AS mediana_dias_ate_rouanet,
    -- a coorte só está madura quando já teve 12 meses inteiros de exposição;
    -- sem esta marca a última linha da série parece uma queda de conversão
    (MAX(data_entrada) + 365 <= CURRENT_DATE) AS coorte_com_12m_completos
FROM novos_entrantes_direto
GROUP BY coorte_entrada, porta_de_entrada, tipo_pessoa
ORDER BY coorte_entrada, porta_de_entrada, tipo_pessoa
