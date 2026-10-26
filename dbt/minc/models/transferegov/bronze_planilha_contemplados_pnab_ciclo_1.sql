{{ config(enabled=false) }}

-- Bronze transferegov — cópia fiel de
-- relatorio_gestao.planilha_contemplados_pnab_ciclo_1.
-- A tabela é uma planilha anexada pelo ente ao relatório de gestão do plano de
-- ação no TransfereGov Fundo a Fundo, lida pela extracao_anexos_dag; o schema
-- `relatorio_gestao` é onde essa DAG grava.
-- DESABILITADO junto com silver_planilha_contemplados: a tabela só nasce da
-- cadeia de anexos do TransfereGov, que não rodou neste banco (ver lá).
select *
from {{ source("relatorio_gestao", "planilha_contemplados_pnab_ciclo_1") }}
