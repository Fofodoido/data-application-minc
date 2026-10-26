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
-- PRONAC pela macro pronac_de_ano_sequencial, a mesma do fct_captacao_rouanet:
-- os dois lados do join e a reconciliação com o fato precisam da mesma chave.
-- Ano ou sequencial fora do padrão viram PRONAC nulo e o recibo sai.
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
-- cadastro não resolva fica de fora pelo INNER JOIN abaixo, sem erro; o teste
-- singular salic_recibo_sem_proponente avisa quando isso acontece, e o
-- salic_recibo_sem_proponente_em_massa trava o build se a perda for grande.
--
-- JANELA: recibos de 1993 em diante — praticamente toda a vigência da lei
-- (1991).

WITH proponente_bruto AS (
    SELECT
        {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} AS pronac,
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) <= 11
                THEN LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 11, '0')
            ELSE LPAD(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g'), 14, '0')
        END AS proponente_documento,
        -- o cadastro não traz o nome do proponente; a coluna fica pelo
        -- contrato do modelo
        NULL::TEXT AS proponente_nome
    FROM {{ ref('bronze_sac_projetos') }}
    WHERE cgccpf IS NOT NULL
      AND LENGTH(REGEXP_REPLACE(TRIM(cgccpf), '[^0-9]', '', 'g')) BETWEEN 8 AND 14
),

projeto_proponente AS (
    -- uma linha por PRONAC
    SELECT DISTINCT ON (pronac) pronac, proponente_documento, proponente_nome
    FROM proponente_bruto
    WHERE pronac IS NOT NULL
      AND proponente_documento !~ '^0+$'
    ORDER BY pronac, proponente_documento
),

recibos AS (
    SELECT
        {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} AS pronac,
        {{ bronze_timestamp("dtrecibo") }}::DATE AS data_recibo,
        {{ bronze_numerico("captacaoreal") }} AS valor_captado
    FROM {{ ref('bronze_sac_captacao') }}
)

SELECT
    p.proponente_documento,
    p.proponente_nome,
    r.data_recibo,
    r.valor_captado,
    r.pronac
FROM recibos r
INNER JOIN projeto_proponente p ON r.pronac = p.pronac
WHERE r.data_recibo IS NOT NULL
  AND r.valor_captado IS NOT NULL
