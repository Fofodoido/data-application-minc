{{ config(enabled=false, materialized="table") }}

-- DESABILITADO junto com silver_planilha_contemplados, seu insumo do lado do
-- edital — ver lá o motivo e a cadeia de DAGs necessária para reativar.
--
-- Intermediate — os eventos de int_ancine_bbagil_salic, cada um marcado com
-- as listas oficiais de contemplados de edital (TransfereGov,
-- silver_planilha_contemplados) em que o documento aparece. Só cruza: a
-- regra de primeiro acesso dos contemplados fica na Gold
-- (eixo2_meta5_primeiro_acesso_contemplados).
--
-- Grão: o mesmo de int_ancine_bbagil_salic, 1 linha por evento. A lista entra
-- como uma coluna por programa, e não como linha, para que um documento nas
-- duas listas não duplique o evento. A marca é verdadeira ou nula, nunca
-- falsa: não estar na planilha é ausência de evidência (ADR 0010, regra 1).
with
    contemplados as (
        select
            id_normalizado,
            case when bool_or(programa_fomento = 'LPG') then true end as contemplado_lpg,
            case when bool_or(programa_fomento = 'PNAB') then true end as contemplado_pnab
        from {{ ref("silver_planilha_contemplados") }}
        group by id_normalizado
    )

select e.*, c.contemplado_lpg, c.contemplado_pnab
from {{ ref("int_ancine_bbagil_salic") }} e
left join contemplados c on e.beneficiario_documento = c.id_normalizado
