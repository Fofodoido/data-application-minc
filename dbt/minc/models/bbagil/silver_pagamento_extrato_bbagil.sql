{{ config(materialized='table') }}

-- Silver — lançamentos do extrato bancário do BB Gestão Ágil com beneficiário
-- informado, débito e crédito, uma linha por lançamento. Lê a silver tipada
-- (silver_extrato_bbagil) e só limpa:
--   1. fica só o lançamento sem subtransações (subtransactionquantity = 0).
--      Os que têm subtransação são abertos em silver_pagamento_subtransacao_bbagil;
--      mantê-los aqui contaria o mesmo dinheiro duas vezes.
--   2. descarta beneficiário ausente (documento nulo ou '0'), que é lixo da
--      origem, não pagamento.
--
-- O que conta como pagamento efetivo NÃO é decidido aqui (ADR 0010, regra de
-- negócio só na Gold): o par débito/crédito de estorno e o recorte de só
-- débito são aplicados em eixo2_meta5_evento_fomento. Por isso o crédito
-- continua nesta tabela, com creditdebitindicator e as colunas da chave do
-- pareamento (conta, documento bruto, valor). Também continuam aqui repasse a
-- ente público, devolução ao Fundo Nacional de Cultura, transferência ao
-- próprio Banco do Brasil e lançamentos internos (aplicação, resgate,
-- imposto).
--
-- data_pagamento e valor podem vir nulos — data vazia (a silver tipada
-- transforma '' em NULL) ou que não é data de calendário (macro data_texto),
-- valor fora do padrão numérico. A linha fica: ela ainda pode ser o outro
-- lado de um estorno, e o pareamento é na Gold. Quem descarta a linha sem
-- data é a Gold, depois de parear. O mesmo vale para beneficiario_documento
-- nulo (beneficiarypersontype fora de 1/2).
--
-- beneficiario_documento normalizado via beneficiarypersontype (1=PF/CPF,
-- 2=PJ/CNPJ) + LPAD, porque uma fração relevante das linhas chega sem o
-- zero à esquerda (CNPJ com 12-13 dígitos, CPF com 10).

SELECT
    id_plano_acao,
    id_plano_acao_dado_bancario,
    id,
    governmentprogramname AS programa_curto,
    beneficiarydocumentid AS beneficiario_documento_bruto,
    CASE
        WHEN beneficiarypersontype = 1
            THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
        WHEN beneficiarypersontype = 2
            THEN LPAD(REGEXP_REPLACE(beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
    END AS beneficiario_documento,
    beneficiaryname AS beneficiario_nome,
    creditdebitindicator,
    descriptionname,
    value AS valor,
    {{ data_texto('bookingdate') }} AS data_pagamento
FROM {{ ref('silver_extrato_bbagil') }}
WHERE subtransactionquantity = 0
  AND beneficiarydocumentid IS NOT NULL
  AND beneficiarydocumentid != '0'
