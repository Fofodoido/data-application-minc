{{ config(materialized='table') }}

-- Silver — um evento por captação real de recurso na Lei Rouanet, no mesmo
-- formato dos pagamentos do BB Ágil, para entrar na espinha unificada
-- (int_bbagil_salic_ancine_eventos_fomento) que a Meta 5 usa para datar o
-- primeiro acesso.
--
-- POR QUE CAPTAÇÃO E NÃO APROVAÇÃO: o lado do fomento direto (LPG/PNAB/LAB)
-- mede dinheiro que efetivamente saiu da conta para o agente. Para a Rouanet
-- o equivalente é o recurso efetivamente captado junto ao incentivador, não a
-- publicação da portaria de aprovação — projeto aprovado que nunca captou não
-- deu acesso a recurso nenhum ao proponente (e é o desfecho de boa parte da
-- base: veja as situações "arquivado ... sem captação"). Misturar aprovação
-- com pagamento tornaria a comparação entre mecanismos incoerente.
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
-- (1991). Diferente do BB Ágil (2023+), a Rouanet aqui quase não tem censura
-- à esquerda, e é por isso que ela é a fonte que mais consegue desmentir um
-- falso "novo entrante" do fomento direto.

WITH proponente_bruto AS (
    -- prioridade 1: cadastro oficial do projeto
    SELECT
        1 AS prioridade,
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        NULL::TEXT AS beneficiario_nome
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
        END AS beneficiario_documento,
        NULLIF(TRIM(nmproponente), '') AS beneficiario_nome
    FROM {{ ref('bronze_legado_sac_tbapiprojetorouanet') }}
    WHERE nrpronac IS NOT NULL
      AND nrcnpjcpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(nrcnpjcpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
),

projeto_proponente AS (
    -- uma linha por PRONAC: o cadastro ganha da API quando os dois existem
    SELECT DISTINCT ON (pronac) pronac, beneficiario_documento, beneficiario_nome
    FROM proponente_bruto
    WHERE beneficiario_documento IS NOT NULL
      AND beneficiario_documento !~ '^0+$'
    ORDER BY pronac, prioridade
),

recibos AS (
    SELECT
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        dtrecibo::DATE AS data_evento,
        captacaoreal::NUMERIC AS valor
    FROM {{ ref('bronze_legado_sac_captacao') }}
    WHERE dtrecibo IS NOT NULL
      -- as duas colunas são TEXT na bronze; o formato já foi verificado como
      -- ISO/numérico em 100% das linhas, mas o guarda fica para a ingestão
      -- não quebrar o modelo silenciosamente se isso mudar
      AND dtrecibo ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      AND captacaoreal ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND captacaoreal::NUMERIC > 0
)

SELECT
    p.beneficiario_documento,
    p.beneficiario_nome,
    'ROUANET' AS programa_fomento,
    r.data_evento,
    r.valor,
    r.pronac
FROM recibos r
INNER JOIN projeto_proponente p ON r.pronac = p.pronac
WHERE p.beneficiario_documento IS NOT NULL
  -- descarta documentos degenerados (só zeros) que não identificam ninguém
  AND p.beneficiario_documento !~ '^0+$'
