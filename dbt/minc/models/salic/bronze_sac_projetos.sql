-- Bronze salic — cópia fiel de salic_bronze.sac__projetos, da ingestão SALIC v2
-- (Trino, ADR 0005). Tudo text, sem cast nem filtro (ADR 0010).
select * from {{ source("bronze_sac", "sac__projetos") }}
