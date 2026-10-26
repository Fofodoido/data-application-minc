{{ config(materialized='table') }}

-- Gold — grão fino da Meta 5: uma linha por (agente × mecanismo), dizendo
-- quando ele entrou naquele mecanismo e se aquela entrada foi a porta de
-- entrada dele no fomento federal como um todo. As golds de consumo da meta
-- derivam daqui, e é aqui que se olha para auditar um agente específico.
--
-- É Gold, e não Intermediate, porque não cruza base nenhuma: aplica a
-- definição de novo entrante (porta de entrada e desempate por mecanismo),
-- que é regra de negócio da meta (ADR 0010). Lê eixo2_meta5_evento_fomento,
-- Gold do mesmo produto.
--
-- A DEFINIÇÃO QUE IMPORTA: novo entrante é comparação CROSS-MECANISMO. O
-- primeiro acesso do agente é o menor evento dele em QUALQUER mecanismo
-- (ver data_primeiro_acesso_geral, particionada só por documento). Quem
-- captou Rouanet em 2010 e recebeu LPG em 2023 NÃO é novo entrante da LPG —
-- é justamente o caso que a pergunta quer separar, e que a versão anterior
-- deste indicador não conseguia enxergar porque só tinha as duas fontes do
-- fundo a fundo, ambas começando em 2023.
--
-- EMPATE: se o menor evento do agente cai no mesmo dia em dois mecanismos,
-- ambos seriam marcados como porta de entrada e o agente contaria duas vezes
-- na série anual. O desempate é determinístico por ordem de mecanismo, a
-- política mais antiga primeiro — ROUANET (1991) < AUDIOVISUAL (Lei 8.685/93)
-- < FSA (2006) < LAB (2020) < LPG (2022) < PNAB (2023) — via ROW_NUMBER,
-- garantindo exatamente uma porta de entrada por agente.

WITH por_agente_mecanismo AS (
    SELECT
        beneficiario_documento,
        programa_fomento,
        tipo_pessoa,
        MIN(data_evento) AS data_primeiro_acesso_mecanismo,
        MAX(data_evento) AS data_ultimo_acesso_mecanismo,
        MIN(ano_inicio_observacao) AS ano_inicio_observacao,
        COUNT(*) AS qtd_eventos,
        SUM(valor) AS valor_total
    FROM {{ ref('eixo2_meta5_evento_fomento') }}
    GROUP BY beneficiario_documento, programa_fomento, tipo_pessoa
),

com_geral AS (
    SELECT
        *,
        MIN(data_primeiro_acesso_mecanismo)
            OVER (PARTITION BY beneficiario_documento) AS data_primeiro_acesso_geral,
        COUNT(*) OVER (PARTITION BY beneficiario_documento) AS qtd_mecanismos_agente,
        ROW_NUMBER() OVER (
            PARTITION BY beneficiario_documento
            ORDER BY
                data_primeiro_acesso_mecanismo,
                CASE programa_fomento
                    WHEN 'ROUANET' THEN 1
                    WHEN 'AUDIOVISUAL' THEN 2
                    WHEN 'FSA' THEN 3
                    WHEN 'LAB' THEN 4
                    WHEN 'LPG' THEN 5
                    WHEN 'PNAB' THEN 6
                END
        ) AS ordem_entrada
    FROM por_agente_mecanismo
)

SELECT
    beneficiario_documento,
    programa_fomento,
    tipo_pessoa,
    data_primeiro_acesso_mecanismo,
    data_ultimo_acesso_mecanismo,
    EXTRACT(YEAR FROM data_primeiro_acesso_mecanismo)::INT AS ano_entrada_mecanismo,
    data_primeiro_acesso_geral,
    EXTRACT(YEAR FROM data_primeiro_acesso_geral)::INT AS ano_primeiro_acesso_geral,
    qtd_mecanismos_agente,
    qtd_eventos,
    valor_total,
    -- exatamente uma linha por agente tem TRUE: o mecanismo pelo qual ele
    -- entrou no fomento federal
    (ordem_entrada = 1) AS eh_porta_de_entrada,
    CASE WHEN ordem_entrada = 1 THEN 'Sim' ELSE 'Não' END AS categoria_primeiro_acesso,
    -- no primeiro ano observável de um mecanismo não existe passado para
    -- comparar; todo mundo parece estreante por construção. A gold anual usa
    -- esta marca para não vender artefato de janela como resultado.
    (EXTRACT(YEAR FROM data_primeiro_acesso_mecanismo)::INT = ano_inicio_observacao)
        AS entrada_em_ano_censurado
FROM com_geral
