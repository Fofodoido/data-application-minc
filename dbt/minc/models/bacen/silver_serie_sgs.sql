-- Silver bacen — serie_sgs.
-- Origem: bronze_serie_sgs, cópia fiel de bacen.serie_sgs, onde tudo chega como text da
-- ingestão via API.
-- Tipar é o trabalho desta camada.
-- 5 colunas: 2 tipadas, 3 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_data("data") }} as data,
    {{ bronze_texto("serie") }} as serie,
    {{ bronze_numerico("valor") }} as valor,
    {{ bronze_texto("dt_ingest") }} as dt_ingest,
    {{ bronze_texto("codigo_serie") }} as codigo_serie
from {{ ref("bronze_serie_sgs") }}
