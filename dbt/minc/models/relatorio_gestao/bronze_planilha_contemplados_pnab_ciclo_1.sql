{{ config(enabled=false) }}

-- Bronze relatorio_gestao — cópia fiel de
-- relatorio_gestao.planilha_contemplados_pnab_ciclo_1.
-- DESABILITADO junto com silver_planilha_contemplados: a tabela só nasce da
-- cadeia de anexos do TransfereGov, que não rodou neste banco (ver lá).
select *
from {{ source("relatorio_gestao", "planilha_contemplados_pnab_ciclo_1") }}
