{{ config(materialized='table') }}

-- Gold — a espinha da Meta 5: um evento por vez que um agente cultural
-- recebeu recurso de uma política federal de fomento, já com a política
-- classificada. É a tabela que permite responder "este agente já havia sido
-- contemplado antes?" olhando ACIMA do mecanismo — que é exatamente o que a
-- pergunta pede ("novos agentes que não haviam sido anteriormente
-- contemplados", sem qualificar por programa).
--
-- Lê int_ancine_bbagil_salic, que só junta as fontes, e aplica aqui TODA a
-- regra de negócio — o que é pagamento efetivo e quem conta (ADR 0010,
-- regra de negócio só na Gold). As demais golds da meta derivam desta e de
-- eixo2_meta5_primeiro_acesso_agente.
--
-- Grão: 1 linha por (documento, mecanismo, data do evento, valor).
--
-- QUEM CONTA, POR FONTE
--
-- BB Ágil — pagamento a agente cultural. Primeiro, o que é pagamento
-- efetivo:
--   • no extrato, só o débito (a saída de dinheiro da conta), e fora os dois
--     lados de um estorno: débito e crédito da MESMA CONTA com o mesmo
--     documento e o mesmo valor. A chave é a conta, não o plano de ação: um
--     plano tem mais de uma conta (os da LPG têm duas) e estorno acontece
--     dentro de uma conta só. Casando no nível do plano, um débito da conta A
--     e um crédito da conta B viram "par" e os DOIS lados são descartados —
--     perda silenciosa de pagamento real. O pareamento roda antes de
--     descartar linha sem documento normalizado ou sem data, como rodava
--     quando morava na silver: uma perna sem data ainda anula a outra.
--   • no sublançamento, só o status 'Pago' (status_prestacao_contas): os
--     outros não chegaram ao beneficiário.
-- Depois, quem conta, portando os critérios de
-- plugins/regras_negocio_bbagil.py (módulo órfão, mantido como referência):
--   • fora repasse entre entes públicos: nome do beneficiário começando com
--     MUNICIPIO/ESTADO/FUNDO/SECRETARIA/SEFAZ é repasse administrativo, não
--     pagamento a agente. Vale para extrato e sublançamento.
--   • no extrato, fora a transferência para o próprio Banco do Brasil
--     (documento '191'), a devolução de saldo ao Fundo Nacional de Cultura
--     (CNPJ 3793086100189) e os lançamentos internos da conta (aplicação,
--     resgate, imposto, ordem cancelada — pela descriptionname).
--   • a política sai do código curto do programa (programa_bruto). Os códigos
--     que o extrato traz:
--       MINC-LPG-{MUNI,EST}-{AUD,OUTRAS}  -> LPG   (desde 2023-06)
--       MINC-PNAB-2023, MINC_PNAB2025_*   -> PNAB  (desde 2023-11)
--       SECULT-A BLANC-{MUN,EST-D,EST-R}  -> LAB   (desde 2023-01)
--     O nome não segue convenção (hífen, underscore, espaço), então separador
--     e caixa são normalizados antes. Sem isso os 95.658 pagamentos do ciclo 2
--     da PNAB caem fora em silêncio. Código que não casa sai: é movimento de
--     conta que não é repasse de política do MinC.
--   • LAB: os códigos SECULT-A BLANC são a Lei Aldir Blanc II (Lei
--     14.399/2022). A LAB I emergencial (2020-2021) está FORA DO ESCOPO por
--     decisão de negócio de 20/08/2026: foi resposta emergencial de pandemia,
--     não fomento. Sua ausência não é lacuna.
--   • PISO DE VALOR: o extrato carrega resíduo que não é repasse de fomento
--     (tarifas, ajustes). O piso de {{ var('limiar_valor_fomento', 375) }}
--     reais é aplicado sobre o TOTAL do agente naquele mecanismo, não por
--     transação, para que "foi atendido por este mecanismo" seja uma decisão
--     só. Avaliar por transação removeria parcelas pequenas de um repasse
--     legítimo e poderia adiantar a data de entrada do agente.
--
-- SALIC — recibo de captação da Rouanet com valor positivo. O evento é a
-- captação, não a aprovação: o fomento direto mede dinheiro que saiu da conta
-- para o agente, e o equivalente na Rouanet é o recurso captado junto ao
-- incentivador. Projeto aprovado que nunca captou não deu acesso a recurso.
--
-- ANCINE — decisão de negócio de 20/08/2026: entram o FSA e o fomento federal
-- ao audiovisual com repasse identificável ao proponente, com valor positivo.
-- Ficam de fora:
--   • ART25 e ART18: são Rouanet e já entram pelo SALIC, por fonte melhor;
--     incluí-los duplicaria evento.
--   • RENDIMENTOS (aplicação financeira), CONTRAPARTIDA (dinheiro do próprio
--     proponente) e OUTRAS FONTES (origem não identificada).
--   • LEI ESTADUAL e LEI MUNICIPAL: não são política federal.
--   • ART39 CONDECINE: tributo sobre o mercado, não fomento direto ao agente.
--     Medido: acrescentaria 46 proponentes e 2 correções.
-- São dois mecanismos, não um: 'FSA' responde ao que o documento da meta
-- nomeia; 'AUDIOVISUAL' carrega Lei do Audiovisual (ART1/ART3/1A/3A),
-- FUNCINES e os editais da Ancine. Rouanet e Ancine não passam pelo piso: o
-- evento já é uma captação formal.
--
-- JANELAS DE OBSERVAÇÃO — a assimetria é a limitação mais importante do
-- indicador, e fica em ano_inicio_observacao para a gold anual marcar os
-- anos censurados:
--   ROUANET      1993+  captação real (SALIC)         — praticamente completa
--   AUDIOVISUAL  2005+  Lei do Audiovisual, FUNCINES e editais Ancine
--   FSA          2011+  Fundo Setorial do Audiovisual
--   LAB          2023+  Aldir Blanc II
--   LPG          2023+  Lei Paulo Gustavo             — nasceu em 2023
--   PNAB         2023+  Política Nacional Aldir Blanc — nasceu em 2023
--
-- "Novo entrante" significa "nenhuma evidência de acesso anterior nas fontes
-- disponíveis", não "verificamos que nunca acessou". Fonte que falta só
-- transforma veterano em falso novo entrante, nunca o contrário — então os
-- percentuais desta série são TETO, e cada fonte nova só pode derrubá-los.

WITH eventos AS (
    SELECT * FROM {{ ref('int_ancine_bbagil_salic') }}
),

-- chaves (conta + documento bruto + valor) que aparecem no extrato como
-- débito E crédito: estorno/devolução. INTERSECT, e não join, para que valor
-- nulo também case com valor nulo, como casava na silver
chaves_estorno AS (
    SELECT id_plano_acao, id_plano_acao_dado_bancario, beneficiario_documento_bruto, valor
    FROM eventos
    WHERE fonte = 'BB_AGIL' AND tipo_lancamento = 'extrato' AND creditdebitindicator = 'D'
    INTERSECT
    SELECT id_plano_acao, id_plano_acao_dado_bancario, beneficiario_documento_bruto, valor
    FROM eventos
    WHERE fonte = 'BB_AGIL' AND tipo_lancamento = 'extrato' AND creditdebitindicator = 'C'
),

pagamento_efetivo AS (
    SELECT e.*
    FROM eventos e
    LEFT JOIN chaves_estorno ce
        ON e.tipo_lancamento = 'extrato'
        AND e.id_plano_acao = ce.id_plano_acao
        AND e.id_plano_acao_dado_bancario = ce.id_plano_acao_dado_bancario
        AND e.beneficiario_documento_bruto = ce.beneficiario_documento_bruto
        AND e.valor = ce.valor
    WHERE
        (
            e.fonte <> 'BB_AGIL'
            OR (
                e.tipo_lancamento = 'extrato'
                AND e.creditdebitindicator = 'D'
                AND ce.id_plano_acao IS NULL
            )
            OR (e.tipo_lancamento = 'subtransacao' AND e.status_prestacao_contas = 'Pago')
        )
        -- só depois de parear: sem documento, data ou valor o evento não é
        -- atribuível a um agente, a um ano nem ao piso
        AND e.beneficiario_documento IS NOT NULL
        AND e.data_evento IS NOT NULL
        AND e.valor IS NOT NULL
),

elegivel AS (
    SELECT
        beneficiario_documento,
        tipo_pessoa,
        data_evento,
        ano_evento,
        valor,
        fonte,
        CASE
            WHEN fonte = 'SALIC' THEN 'ROUANET'
            WHEN fonte = 'ANCINE' AND programa_bruto = 'FSA' THEN 'FSA'
            WHEN fonte = 'ANCINE' AND programa_bruto IN (
                'ART1',
                'ART3',
                'ART 1A',
                'ART 3A',
                'ART41 (FUNCINES)',
                'EDITAL ANCINE',
                'EDITAL ANCINE (PAR)',
                'OUTROS EDITAIS'
            ) THEN 'AUDIOVISUAL'
            WHEN fonte = 'BB_AGIL'
                AND UPPER(TRANSLATE(programa_bruto, '_ ', '--')) LIKE 'MINC-LPG%'
                THEN 'LPG'
            WHEN fonte = 'BB_AGIL'
                AND UPPER(TRANSLATE(programa_bruto, '_ ', '--')) LIKE '%PNAB%'
                THEN 'PNAB'
            WHEN fonte = 'BB_AGIL'
                AND UPPER(TRANSLATE(programa_bruto, '_ ', '--')) LIKE '%A-BLANC%'
                THEN 'LAB'
        END AS programa_fomento
    FROM pagamento_efetivo
    WHERE
        (fonte IN ('SALIC', 'ANCINE') AND valor > 0)
        OR (
            fonte = 'BB_AGIL'
            AND NOT (
                UPPER(COALESCE(beneficiario_nome, '')) LIKE 'MUNICIPIO%'
                OR UPPER(COALESCE(beneficiario_nome, '')) LIKE 'ESTADO%'
                OR UPPER(COALESCE(beneficiario_nome, '')) LIKE 'FUNDO%'
                OR UPPER(COALESCE(beneficiario_nome, '')) LIKE 'SECRETARIA%'
                OR UPPER(COALESCE(beneficiario_nome, '')) LIKE 'SEFAZ%'
            )
            AND (
                tipo_lancamento = 'subtransacao'
                -- descriptionname nula no extrato também sai, como na
                -- regra original (NOT IN com NULL não é verdadeiro)
                OR (
                    beneficiario_documento_bruto NOT IN ('191', '3793086100189')
                    AND descriptionname NOT IN (
                        'BB-APLIC C.PRZ-APL.AUT', 'Resgate Automatico', 'Impostos',
                        'ORDEM BANC CANCELADA', 'Ordem Bancaria', 'Resgate BB Fix',
                        'CREDITO CONVENIO', 'Estorno Resgate Automatico'
                    )
                )
            )
        )
),

classificado AS (
    SELECT * FROM elegivel WHERE programa_fomento IS NOT NULL
),

bbagil_acima_piso AS (
    SELECT beneficiario_documento, programa_fomento
    FROM classificado
    WHERE fonte = 'BB_AGIL'
    GROUP BY beneficiario_documento, programa_fomento
    HAVING SUM(valor) >= {{ var('limiar_valor_fomento', 375) }}
)

SELECT
    c.beneficiario_documento,
    c.programa_fomento,
    c.data_evento,
    c.ano_evento,
    c.valor,
    c.fonte,
    -- primeiro ano em que a fonte daquele mecanismo consegue enxergar
    -- qualquer coisa; a gold anual usa isto para marcar o ano censurado,
    -- onde todo mundo parece estreante porque não existe passado observável
    CASE
        WHEN c.programa_fomento = 'ROUANET' THEN 1993
        WHEN c.programa_fomento = 'AUDIOVISUAL' THEN 2005
        WHEN c.programa_fomento = 'FSA' THEN 2011
        WHEN c.programa_fomento IN ('LAB', 'LPG', 'PNAB') THEN 2023
    END AS ano_inicio_observacao,
    c.tipo_pessoa
FROM classificado c
LEFT JOIN bbagil_acima_piso p
    ON c.beneficiario_documento = p.beneficiario_documento
    AND c.programa_fomento = p.programa_fomento
WHERE c.fonte <> 'BB_AGIL' OR p.beneficiario_documento IS NOT NULL
