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
-- FONTE: a ingestão SALIC v2 (salic_bronze, via Trino), pelas bronzes
-- bronze_sac_projetos e bronze_sac_captacao. O proponente vem só de
-- ``sac__projetos``, o cadastro oficial do projeto, que em 19/08/2026 cobria
-- 100% dos recibos válidos.
--
-- Até 21/10/2026 este modelo lia a ingestão v1 (schema `bronze`) e tinha o
-- espelho da API pública (``sac__tbapiprojetorouanet``) como reserva de
-- proponente. A reserva saiu na troca para a v2, que não ingere essa tabela:
-- as duas fontes concordavam em 100,00% dos 195.240 projetos que ambas
-- resolviam, e o cadastro sozinho já resolvia todos. Um recibo cujo PRONAC o
-- cadastro não resolva fica de fora pelo INNER JOIN abaixo, sem erro: é o
-- número a conferir depois de cada carga.
--
-- JANELA: recibos de 1993 em diante — praticamente toda a vigência da lei
-- (1991). Diferente do BB Ágil (2023+), a Rouanet aqui quase não tem censura
-- à esquerda, e é por isso que ela é a fonte que mais consegue desmentir um
-- falso "novo entrante" do fomento direto.

WITH proponente_bruto AS (
    SELECT
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 14, '0')
        END AS beneficiario_documento,
        -- o cadastro não traz o nome do proponente; a coluna fica pelo
        -- contrato do modelo
        NULL::TEXT AS beneficiario_nome
    FROM {{ ref('bronze_sac_projetos') }}
    WHERE anoprojeto IS NOT NULL
      AND sequencial IS NOT NULL
      AND cgccpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
),

projeto_proponente AS (
    -- uma linha por PRONAC
    SELECT DISTINCT ON (pronac) pronac, beneficiario_documento, beneficiario_nome
    FROM proponente_bruto
    WHERE beneficiario_documento IS NOT NULL
      AND beneficiario_documento !~ '^0+$'
    ORDER BY pronac, beneficiario_documento
),

recibos AS (
    SELECT
        TRIM(anoprojeto) || TRIM(sequencial) AS pronac,
        TRIM(dtrecibo)::DATE AS data_evento,
        TRIM(captacaoreal)::NUMERIC AS valor
    FROM {{ ref('bronze_sac_captacao') }}
    WHERE dtrecibo IS NOT NULL
      -- as duas colunas são TEXT na bronze. Os guardas são os mesmos das
      -- macros bronze_timestamp/bronze_numerico da v2 (prefixo ISO na data,
      -- ponto decimal no valor, com TRIM), para a ingestão não quebrar o
      -- modelo silenciosamente se o formato mudar
      AND TRIM(dtrecibo) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      AND TRIM(captacaoreal) ~ '^-?[0-9]+(\.[0-9]+)?$'
      AND TRIM(captacaoreal)::NUMERIC > 0
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
