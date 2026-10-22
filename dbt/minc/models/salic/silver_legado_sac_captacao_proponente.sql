{{ config(materialized='table') }}

-- Silver — um recibo de captação da Lei Rouanet por linha, com o proponente
-- que recebeu o recurso. Junta duas tabelas do próprio SALIC (o recibo e o
-- cadastro do projeto) e tipa; não decide quais recibos contam para um
-- indicador, o que é da Gold de cada produto.
--
-- LIGAÇÃO ENTRE AS TABELAS: sac__captacao tem o dinheiro e a data mas não tem
-- o proponente (``cgccpfmecena`` é o incentivador que aportou, não quem
-- recebeu). A chave para chegar nele é o PRONAC. Não use ``captacao.idprojeto``:
-- ele é 0 na maioria das linhas.
--
-- Usamos DUAS fontes de proponente, nesta ordem:
--   1. ``sac__projetos`` (cadastro oficial do projeto) — carregada em 19/08/2026
--      depois de meses falhando na ingestão. Cobre 100% dos recibos válidos.
--   2. ``sac__tbapiprojetorouanet`` (espelho da API pública) — cobre 99,86%.
--      Fica como reserva: se a carga de sac__projetos regredir, a série não cai.
--
-- As duas concordam em 100,00% dos 195.240 projetos que ambas resolvem — mesmo
-- CPF/CNPJ, sem uma única divergência. Foi essa checagem que validou a ponte,
-- que até 19/08 dependia só do espelho da API.
--
-- JANELA: recibos de 1993 em diante — praticamente toda a vigência da lei
-- (1991).

WITH proponente_bruto AS (
    -- prioridade 1: cadastro oficial do projeto
    SELECT
        1 AS prioridade,
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 14, '0')
        END AS proponente_documento,
        NULL::TEXT AS proponente_nome
    FROM {{ ref('bronze_legado_sac_projetos') }}
    WHERE anoprojeto IS NOT NULL
      AND sequencial IS NOT NULL
      AND cgccpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14

    UNION ALL

    -- prioridade 2: espelho da API, usado onde o cadastro não resolve
    SELECT
        2 AS prioridade,
        TRIM(nrpronac) AS pronac,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g'), 14, '0')
        END AS proponente_documento,
        NULLIF(TRIM(nmproponente), '') AS proponente_nome
    FROM {{ ref('bronze_legado_sac_tbapiprojetorouanet') }}
    WHERE nrpronac IS NOT NULL
      AND nrcnpjcpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
),

projeto_proponente AS (
    -- uma linha por PRONAC: o cadastro ganha da API quando os dois existem
    SELECT DISTINCT ON (pronac) pronac, proponente_documento, proponente_nome
    FROM proponente_bruto
    WHERE proponente_documento IS NOT NULL
      AND proponente_documento !~ '^0+$'
    ORDER BY pronac, prioridade
),

recibos AS (
    SELECT
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        dtrecibo::DATE AS data_recibo,
        captacaoreal::NUMERIC AS valor_captado
    FROM {{ ref('bronze_legado_sac_captacao') }}
    WHERE dtrecibo IS NOT NULL
      -- as duas colunas são TEXT na bronze; o formato já foi verificado como
      -- ISO/numérico em 100% das linhas, mas o guarda fica para a ingestão
      -- não quebrar o modelo silenciosamente se isso mudar
      AND dtrecibo ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      AND captacaoreal ~ '^-?[0-9]+(\.[0-9]+)?$'
)

SELECT
    p.proponente_documento,
    p.proponente_nome,
    r.data_recibo,
    r.valor_captado,
    r.pronac
FROM recibos r
INNER JOIN projeto_proponente p ON r.pronac = p.pronac
