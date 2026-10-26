-- Bronze bacen — cópia fiel de bacen.serie_sgs, como a bacen_sgs_ingest_dag grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_serie_sgs.
select * from {{ source("bacen", "serie_sgs") }}  -- noqa: AM04
