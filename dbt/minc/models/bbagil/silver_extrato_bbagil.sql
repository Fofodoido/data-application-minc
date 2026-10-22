-- Silver bbagil — extrato_bbagil.
-- Origem: bronze_extrato_bbagil, cópia fiel de bbagil.extrato_bbagil, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 42 colunas: 22 tipadas, 20 mantidas como texto.
-- beneficiarydocumentid fica texto mesmo casando 100% com inteiro: é CPF/CNPJ,
-- identificador e não número. Como integer perdia o zero à esquerda e estourava
-- a faixa do tipo em todo CPF acima de 2.147.483.647.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_texto("errors") }} as errors,
    {{ bronze_texto("error") }} as error,
    {{ bronze_inteiro("governmentprogramcode") }} as governmentprogramcode,
    {{ bronze_texto("governmentprogramname") }} as governmentprogramname,
    {{ bronze_texto("governmentsubprogramcode") }} as governmentsubprogramcode,
    {{ bronze_texto("governmentsubprogramname") }} as governmentsubprogramname,
    {{ bronze_inteiro("id_plano_acao") }} as id_plano_acao,
    {{ bronze_inteiro("id_programa") }} as id_programa,
    {{ bronze_inteiro("codigo_programa", "bigint") }} as codigo_programa,
    {{ bronze_texto("nome_programa") }} as nome_programa,
    {{ bronze_inteiro("id_plano_acao_dado_bancario") }} as id_plano_acao_dado_bancario,
    {{ bronze_texto("id_agencia_conta") }} as id_agencia_conta,
    {{ bronze_inteiro("cod_ibge") }} as cod_ibge,
    {{ bronze_data("periodo_inicial") }} as periodo_inicial,
    {{ bronze_data("periodo_final") }} as periodo_final,
    {{ bronze_inteiro("id", "bigint") }} as id,
    {{ bronze_texto("bookingdate") }} as bookingdate,
    {{ bronze_inteiro("orderindex") }} as orderindex,
    {{ bronze_texto("valuedate") }} as valuedate,
    {{ bronze_inteiro("referencenumber", "bigint") }} as referencenumber,
    {{ bronze_numerico("value") }} as value,
    {{ bronze_texto("accountbalance") }} as accountbalance,
    {{ bronze_inteiro("descriptioncode") }} as descriptioncode,
    {{ bronze_texto("descriptionname") }} as descriptionname,
    {{ bronze_inteiro("descriptionbatchnumber") }} as descriptionbatchnumber,
    {{ bronze_texto("creditdebitindicator") }} as creditdebitindicator,
    {{ bronze_inteiro("beneficiarybankidentifiercode") }} as beneficiarybankidentifiercode,
    {{ bronze_inteiro("beneficiarybranchcode") }} as beneficiarybranchcode,
    {{ bronze_inteiro("beneficiaryaccountnumber") }} as beneficiaryaccountnumber,
    {{ bronze_inteiro("beneficiarypersontype") }} as beneficiarypersontype,
    {{ bronze_texto("beneficiarydocumentid") }} as beneficiarydocumentid,
    {{ bronze_texto("beneficiaryname") }} as beneficiaryname,
    {{ bronze_booleano("pendingexpenseconciliation") }} as pendingexpenseconciliation,
    {{ bronze_booleano("attachedexpensedocumentindicator") }} as attachedexpensedocumentindicator,
    {{ bronze_inteiro("expensecategorycode") }} as expensecategorycode,
    {{ bronze_texto("expenseidentificationstatus") }} as expenseidentificationstatus,
    {{ bronze_inteiro("subtransactionquantity") }} as subtransactionquantity,
    {{ bronze_texto("bankorderpurposecode") }} as bankorderpurposecode,
    {{ bronze_texto("bankorderrulecode") }} as bankorderrulecode,
    {{ bronze_texto("bankorderpurposedescription") }} as bankorderpurposedescription,
    {{ bronze_texto("expensescategory") }} as expensescategory,
    {{ bronze_texto("expensesdocuments") }} as expensesdocuments
from {{ ref("bronze_extrato_bbagil") }}
