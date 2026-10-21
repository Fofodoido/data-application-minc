-- Bronze salic — cópia fiel de bronze.sac__tbapiprojetorouanet, da ingestão SALIC v1
-- (legada).
-- O `legado_` no nome é a exceção de entidade do ADR 0010: a bronze da v2,
-- quando o salic_dbt migrar, vai ocupar bronze_sac_tbapiprojetorouanet. Sai junto com
-- silver_legado_sac_captacao_proponente, quando a reconciliação G3 com a v2
-- for feita (HANDOFF.md).
select * from {{ source("bronze_sac_legado", "sac__tbapiprojetorouanet") }}
