{{ config(severity="warn") }}

-- Recibo de captação que some da silver_sac_captacao_proponente sem erro.
-- O proponente vem só do cadastro (sac__projetos), e o INNER JOIN descarta o
-- recibo cujo PRONAC o cadastro não resolve. Em 19/08/2026 eram zero; se a
-- carga do cadastro regredir, a Rouanet perde evento em silêncio e veterano
-- vira falso novo entrante. Um aviso por PRONAC, com quantos recibos e quanto
-- dinheiro ficaram de fora — a conferência pós-carga, automatizada.
--
-- Conta só recibo com data e valor utilizáveis: o que a silver descarta por
-- formato não é perda de proponente.
with
    recibos as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            {{ bronze_numerico("captacaoreal") }} as valor_captado
        from {{ ref("bronze_sac_captacao") }}
        where
            {{ bronze_timestamp("dtrecibo") }} is not null
            and {{ bronze_numerico("captacaoreal") }} is not null
    ),

    resolvidos as (
        select distinct pronac from {{ ref("silver_sac_captacao_proponente") }}
    )

select r.pronac, count(*) as qtd_recibos, sum(r.valor_captado) as valor_captado
from recibos r
left join resolvidos s on r.pronac = s.pronac
where s.pronac is null
group by r.pronac
