-- Bronze ibge_sidra — cópia fiel de ibge_sidra.agregados_catalogo, como a
-- ibge_agregados_dag grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_agregados_catalogo.
select * from {{ source("ibge_sidra", "agregados_catalogo") }}
