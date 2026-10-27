-- Bronze ibge_sidra — cópia fiel de ibge_sidra.pesquisas, como a ibge_agregados_dag
-- grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_pesquisas.
select * from {{ source("ibge_sidra", "pesquisas") }}  -- noqa: AM04
