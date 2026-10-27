-- Bronze ibge_sidra — cópia fiel de ibge_sidra.periodos, como a ibge_agregados_dag grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_periodos.
select * from {{ source("ibge_sidra", "periodos") }}  -- noqa: AM04
