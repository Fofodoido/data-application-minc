-- Silver ibge_sidra — periodos.
-- Origem: bronze_periodos, cópia fiel de ibge_sidra.periodos, onde tudo chega como text
-- da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 4 colunas: 2 tipadas, 2 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("agregado_id") }} as agregado_id,
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("literals") }} as literals,
    {{ bronze_texto("modificacao") }} as modificacao
from {{ ref("bronze_periodos") }}
