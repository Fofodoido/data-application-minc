-- Silver ibge_sidra — metadados.
-- Origem: bronze_metadados, cópia fiel de ibge_sidra.metadados, onde tudo chega como
-- text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 10 colunas: 2 tipadas, 8 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("agregado_id") }} as agregado_id,
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("url") }} as url,
    {{ bronze_texto("pesquisa") }} as pesquisa,
    {{ bronze_texto("assunto") }} as assunto,
    {{ bronze_texto("periodicidade") }} as periodicidade,
    {{ bronze_texto("nivelterritorial") }} as nivelterritorial,
    {{ bronze_texto("variaveis") }} as variaveis,
    {{ bronze_texto("classificacoes") }} as classificacoes
from {{ ref("bronze_metadados") }}
