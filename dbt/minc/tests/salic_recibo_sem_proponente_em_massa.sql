{{ config(severity="error") }}

-- O pior caso do salic_recibo_sem_proponente: se a cópia v2 de sac__projetos
-- vier vazia ou parcial, o INNER JOIN da silver_sac_captacao_proponente
-- derruba recibos em massa. Lá o aviso por PRONAC dispara em tudo e o
-- `dbt build` passa mesmo assim, com a série da Rouanet quebrada em silêncio.
-- Este teste trava o build quando a perda passa de
-- {{ var("limiar_recibo_sem_proponente", 0.01) }} (fração dos recibos com data
-- e valor utilizáveis). Abaixo disso, é só o aviso.
with
    recibos as (
        select count(*) as qtd
        from {{ ref("bronze_sac_captacao") }}
        where
            {{ bronze_timestamp("dtrecibo") }} is not null
            and {{ bronze_numerico("captacaoreal") }} is not null
    ),

    resolvidos as (
        select count(*) as qtd from {{ ref("silver_sac_captacao_proponente") }}
    )

select
    r.qtd as qtd_recibos,
    r.qtd - s.qtd as qtd_sem_proponente,
    round((r.qtd - s.qtd)::numeric / nullif(r.qtd, 0), 4) as fracao_perdida
from recibos r
cross join resolvidos s
where
    r.qtd = 0
    or (r.qtd - s.qtd)::numeric / r.qtd > {{ var("limiar_recibo_sem_proponente", 0.01) }}
