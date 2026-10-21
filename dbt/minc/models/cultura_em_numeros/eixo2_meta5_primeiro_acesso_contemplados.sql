{{ config(enabled=false, materialized='table') }}

-- DESABILITADO junto com int_relatorio_gestao_primeiro_acesso_contemplados,
-- seu único insumo, e com silver_planilha_contemplados — ver lá o motivo por
-- extenso e a cadeia de DAGs necessária para reativar.
--
-- Em uma linha: as listas oficiais de contemplados só existem como planilha
-- anexada ao relatório de gestão do TransfereGov, e essa cadeia não rodou
-- neste banco. Nenhum dos três gold que respondem à Meta 5 depende deste
-- modelo — ele confere o indicador, não o produz.

-- Gold — o mesmo indicador de novos entrantes de
-- eixo2_meta5_primeiro_acesso_anual, mas restrito a quem aparece nas LISTAS
-- OFICIAIS DE CONTEMPLADOS dos editais (LPG e PNAB), em vez de a todo agente visto recebendo dinheiro. Serve para
-- a leitura "dos contemplados de edital, quantos eram novos", e para expor a
-- cobertura entre as duas populações.
--
-- SUBSTITUI o antigo modelo de mesmo nome, que respondia esta pergunta pela
-- resposta AUTODECLARADA do formulário de proponente. Aquele mapeamento Sim/Não
-- nunca pôde ser confirmado contra o formulário original; aqui a classificação
-- vem de evidência de repasse, cruzando LPG, PNAB, LAB, Rouanet e audiovisual.
-- O grão mudou: era (programa × categoria × contemplado), hoje é
-- (programa × categoria), e a coluna `contemplado` deixou de existir porque
-- todo mundo nesta tabela é contemplado.
--
-- Grão: 1 linha por (programa × categoria de primeiro acesso).
--
-- ATENÇÃO À COBERTURA: a interseção entre lista de contemplados e extrato
-- bancário é parcial — em torno de 46% na LPG e 89% no PNAB. A causa foi
-- medida e NÃO é atraso de pagamento nem filtro agressivo: o BB Ágil só
-- enxerga contas administradas pelo Banco do Brasil, e o município muitas
-- vezes declara ter pago um contemplado cujo pagamento não passou por lá.
-- Logo esta tabela descreve bem a fatia coberta, e não deve ser lida como
-- retrato completo da política. Contemplado sem evento observável fica de
-- fora — não há fallback autodeclarado, e inverter "novo entrante" a partir de
-- um mapeamento que nunca pôde ser confirmado contra o formulário original
-- seria pior que não responder.
--
-- A classificação vem de int_bbagil_salic_ancine_primeiro_acesso, então já é
-- CROSS-MECANISMO e já considera a Rouanet desde 1993 — um contemplado da LPG que captou
-- Rouanet em 2010 aparece aqui corretamente como "Não".

-- O cruzamento com a lista de contemplados é feito na intermediate; aqui só
-- se agrega.
WITH acesso_contemplado AS (
    SELECT programa_fomento, beneficiario_documento, categoria_primeiro_acesso
    FROM {{ ref('int_relatorio_gestao_primeiro_acesso_contemplados') }}
)

SELECT
    programa_fomento,
    categoria_primeiro_acesso,
    COUNT(DISTINCT beneficiario_documento) AS total_proponentes,
    ROUND(
        COUNT(DISTINCT beneficiario_documento)::NUMERIC
        / SUM(COUNT(DISTINCT beneficiario_documento)) OVER (PARTITION BY programa_fomento)
        * 100, 2
    ) AS percentual
FROM acesso_contemplado
GROUP BY programa_fomento, categoria_primeiro_acesso
ORDER BY programa_fomento, total_proponentes DESC
