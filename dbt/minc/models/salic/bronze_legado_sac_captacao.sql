{{ config(materialized="table") }}

-- TABLE, e não a view padrão da pasta: a ingestão v1 (salic_ingestion, @daily)
-- recria a tabela de origem com DROP TABLE sem CASCADE, e o Postgres recusa o
-- DROP enquanto houver view dependente — a carga quebraria. Como cópia, a
-- bronze não prende a origem.
-- Bronze salic — cópia fiel de bronze.sac__captacao, da ingestão SALIC v1 (legada).
-- O `legado_` no nome é a exceção de entidade do ADR 0010: a bronze da v2,
-- quando o salic_dbt migrar, vai ocupar bronze_sac_captacao. Sai junto com
-- silver_legado_sac_captacao_proponente, quando ela passar a ler a v2.
select *
from {{ source("bronze_sac_legado", "sac__captacao") }}
