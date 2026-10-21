{{ config(materialized='table') }}

-- Gold — responde à pergunta 2.3 da Meta 5: "há novos entrantes pelo fomento
-- direto que depois acessaram a Lei Rouanet?"
--
-- Grão: 1 linha por trajetória (categoria de percurso do agente entre o
-- fomento direto e a Rouanet), com a contagem de agentes em cada uma.
--
-- 'fomento direto' aqui é LAB/LPG/PNAB. A trilha da Ancine (FSA e
-- AUDIOVISUAL) entrou na espinha em 20/08/2026 e NÃO é fomento direto: quem
-- só aparece nela sai na categoria SO_AUDIOVISUAL, fora da pergunta 2.3.
--
-- POR QUE ESTA TABELA É O TESTE DO INDICADOR INTEIRO: a Rouanet enxerga
-- desde 1993, o fomento direto só desde 2023. Então o cruzamento mede duas
-- coisas de uma vez —
--   (a) a resposta da pergunta 2.3, na direção "entrou pelo direto, depois
--       captou Rouanet" (categoria DIRETO_DEPOIS_ROUANET); e
--   (b) quantos supostos "novos entrantes" do fomento direto a Rouanet
--       DESMENTE, na direção oposta (ROUANET_ANTES_DIRETO) — gente que já
--       acessava recurso público federal muito antes de 2023 e que, sem a
--       fonte da Rouanet, seria contada como agente novo.
-- (b) é a única medição direta que temos do tamanho do viés de fonte que
-- afeta todo o resto da meta.
--
-- TRAJETÓRIA ≠ NOVIDADE: DIRETO_DEPOIS_ROUANET diz que o agente tocou o
-- fomento direto antes da Rouanet, não que ele era novo no fomento federal.
-- Quem já aparecia na trilha da Ancine antes de 2023 e só depois pegou LPG e
-- Rouanet faz esse percurso sem ser novo entrante. Por isso a porta de
-- entrada é dimensão desta tabela e não um detalhe: a resposta estrita da
-- 2.3 são as linhas de DIRETO_DEPOIS_ROUANET com porta em LAB/LPG/PNAB. A
-- série com exposição controlada vive em
-- eixo2_meta5_conversao_direto_rouanet_coorte.

WITH agentes AS (
    SELECT
        beneficiario_documento,
        tipo_pessoa,
        MIN(data_primeiro_acesso_mecanismo)
            FILTER (WHERE programa_fomento IN ('LAB', 'LPG', 'PNAB')) AS entrada_direto,
        MIN(data_primeiro_acesso_mecanismo)
            FILTER (WHERE programa_fomento = 'ROUANET') AS entrada_rouanet,
        MAX(programa_fomento) FILTER (WHERE eh_porta_de_entrada) AS mecanismo_porta_entrada
    FROM {{ ref('int_bbagil_salic_ancine_primeiro_acesso') }}
    GROUP BY beneficiario_documento, tipo_pessoa
),

classificado AS (
    SELECT
        beneficiario_documento,
        tipo_pessoa,
        entrada_direto,
        entrada_rouanet,
        mecanismo_porta_entrada,
        CASE
            -- agente que só aparece na trilha da Ancine (FSA / audiovisual):
            -- não tem fomento direto nem Rouanet, então não participa da
            -- pergunta 2.3. Precisa de balde próprio — sem isto ele cairia no
            -- ELSE e seria contado como 'MESMO_DIA', que é o que aconteceu na
            -- primeira execução depois que a Ancine entrou (7 -> 1.197).
            WHEN entrada_direto IS NULL AND entrada_rouanet IS NULL
                THEN 'SO_AUDIOVISUAL'
            WHEN entrada_direto IS NOT NULL AND entrada_rouanet IS NULL
                THEN 'SO_DIRETO'
            WHEN entrada_direto IS NULL AND entrada_rouanet IS NOT NULL
                THEN 'SO_ROUANET'
            -- resposta afirmativa à 2.3: entrou pelo fomento direto e só
            -- depois chegou à Rouanet
            WHEN entrada_direto < entrada_rouanet
                THEN 'DIRETO_DEPOIS_ROUANET'
            -- o contrário: já era da Rouanet antes de aparecer no fomento
            -- direto. Sem SALIC, cada um destes seria um falso novo entrante.
            WHEN entrada_rouanet < entrada_direto
                THEN 'ROUANET_ANTES_DIRETO'
            ELSE 'MESMO_DIA'
        END AS trajetoria
    FROM agentes
)

SELECT
    trajetoria,
    tipo_pessoa,
    mecanismo_porta_entrada,
    COUNT(*) AS agentes,
    MIN(entrada_direto) AS primeira_entrada_direto,
    MAX(entrada_direto) AS ultima_entrada_direto,
    MIN(entrada_rouanet) AS primeira_entrada_rouanet,
    MAX(entrada_rouanet) AS ultima_entrada_rouanet,
    -- só faz sentido para quem passou pelos dois lados
    ROUND(AVG(
        CASE
            WHEN entrada_direto IS NOT NULL AND entrada_rouanet IS NOT NULL
                THEN ABS(entrada_rouanet - entrada_direto)
        END
    ), 1) AS media_dias_entre_mecanismos
FROM classificado
GROUP BY trajetoria, tipo_pessoa, mecanismo_porta_entrada
ORDER BY trajetoria, tipo_pessoa, mecanismo_porta_entrada
