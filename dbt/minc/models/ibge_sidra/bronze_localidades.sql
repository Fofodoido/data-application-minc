-- Bronze ibge_sidra — cópia fiel de ibge_sidra.localidades, como a ibge_agregados_dag
-- grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_localidades.
select * from {{ source("ibge_sidra", "localidades") }}
