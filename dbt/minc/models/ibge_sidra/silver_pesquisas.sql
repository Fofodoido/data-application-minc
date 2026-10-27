-- Silver ibge_sidra — pesquisas.
-- Origem: bronze_pesquisas, cópia fiel de ibge_sidra.pesquisas, onde tudo chega como
-- text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 3 colunas: 0 tipadas, 3 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_texto("id") }} as id,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("agregados") }} as agregados
from {{ ref("bronze_pesquisas") }}
