-- Silver ibge_sidra — localidades.
-- Origem: bronze_localidades, cópia fiel de ibge_sidra.localidades, onde tudo chega
-- como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 2 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("agregado_id") }} as agregado_id,
    {{ bronze_texto("nivel") }} as nivel,
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("nome") }} as nome
from {{ ref("bronze_localidades") }}
