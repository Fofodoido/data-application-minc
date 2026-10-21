-- Silver bbagil — subtransacao_bbagil.
-- Origem: bronze_subtransacao_bbagil, cópia fiel de bbagil.subtransacao_bbagil, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 33 colunas: 0 tipadas, 33 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_texto("errors") }} as errors,
    {{ bronze_texto("error") }} as error,
    {{ bronze_texto("governmentprogramcode") }} as governmentprogramcode,
    {{ bronze_texto("governmentprogramname") }} as governmentprogramname,
    {{ bronze_texto("governmentsubprogramcode") }} as governmentsubprogramcode,
    {{ bronze_texto("governmentsubprogramname") }} as governmentsubprogramname,
    {{ bronze_texto("id_plano_acao") }} as id_plano_acao,
    {{ bronze_texto("id_transacao_pai") }} as id_transacao_pai,
    {{ bronze_texto("id_programa") }} as id_programa,
    {{ bronze_texto("id_plano_acao_dado_bancario") }} as id_plano_acao_dado_bancario,
    {{ bronze_texto("id_agencia_conta") }} as id_agencia_conta,
    {{ bronze_texto("cod_ibge") }} as cod_ibge,
    {{ bronze_texto("id") }} as id,
    {{ bronze_texto("codesubtransactionstate") }} as codesubtransactionstate,
    {{ bronze_texto("paymentstatus") }} as paymentstatus,
    {{ bronze_texto("paymentdate") }} as paymentdate,
    {{ bronze_texto("value") }} as value,
    {{ bronze_texto("beneficiarybankidentifiercode") }} as beneficiarybankidentifiercode,
    {{ bronze_texto("beneficiarybranchcode") }} as beneficiarybranchcode,
    {{ bronze_texto("beneficiaryaccountnumber") }} as beneficiaryaccountnumber,
    {{ bronze_texto("beneficiarypersontype") }} as beneficiarypersontype,
    {{ bronze_texto("beneficiarydocumentid") }} as beneficiarydocumentid,
    {{ bronze_texto("beneficiaryname") }} as beneficiaryname,
    {{ bronze_texto("attachedexpensedocumentindicator") }} as attachedexpensedocumentindicator,
    {{ bronze_texto("expensecategorycode") }} as expensecategorycode,
    {{ bronze_texto("subtransactionaccountabilityindicator") }} as subtransactionaccountabilityindicator,
    {{ bronze_texto("subtransactionaccountabilityname") }} as subtransactionaccountabilityname,
    {{ bronze_texto("bankorderpurposecode") }} as bankorderpurposecode,
    {{ bronze_texto("bankorderrulecode") }} as bankorderrulecode,
    {{ bronze_texto("bankorderpurposedescription") }} as bankorderpurposedescription,
    {{ bronze_texto("expensesequentialnumber") }} as expensesequentialnumber,
    {{ bronze_texto("expensescategory") }} as expensescategory,
    {{ bronze_texto("expensesdocuments") }} as expensesdocuments
from {{ ref("bronze_subtransacao_bbagil") }}
