{{ config(materialized="table") }}

-- Bronze salic — cópia fiel de salic_bronze.sac__captacao, da ingestão SALIC v2
-- (Trino, ADR 0005). Tudo text, sem cast nem filtro (ADR 0010).
--
-- TABLE, e não a view padrão da pasta: a ingestão v2 recria a tabela de origem
-- com DROP TABLE pelo Trino, que não tem CASCADE, e o Postgres recusa o DROP
-- enquanto houver view dependente — a carga quebraria. Como cópia, a bronze
-- não prende a origem.
select *
from {{ source("bronze_sac", "sac__captacao") }}
