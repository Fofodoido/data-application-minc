{{ config(enabled=false, materialized='table') }}

-- DESABILITADO junto com silver_planilha_contemplados, seu insumo do lado do
-- edital — ver lá o motivo e a cadeia de DAGs necessária para reativar.
-- Intermediate — cruza as listas oficiais de contemplados (relatorio_gestao)
-- com a classificação de primeiro acesso (BB Ágil + SALIC + Ancine), uma linha
-- por (agente × mecanismo) contemplado. Separado da gold
-- eixo2_meta5_primeiro_acesso_contemplados porque cruzamento entre fontes é
-- trabalho da intermediate (ADR 0010); a gold só agrega.
with
    contemplados as (
        select distinct id_normalizado, programa_fomento
        from {{ ref('silver_planilha_contemplados') }}
    )

select pa.programa_fomento, pa.beneficiario_documento, pa.categoria_primeiro_acesso
from {{ ref('int_bbagil_salic_ancine_primeiro_acesso') }} pa
inner join
    contemplados c
    on pa.beneficiario_documento = c.id_normalizado
    and pa.programa_fomento = c.programa_fomento
