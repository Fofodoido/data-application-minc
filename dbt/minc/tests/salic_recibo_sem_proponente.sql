{{ config(severity="warn") }}

-- Recibo de captação que some da silver_sac_captacao_proponente sem erro.
-- O proponente vem só do cadastro (sac__projetos), e o INNER JOIN descarta o
-- recibo cujo PRONAC o cadastro não resolve. Em 19/08/2026 eram zero; se a
-- carga do cadastro regredir, a Rouanet perde evento em silêncio e veterano
-- vira falso novo entrante. Um aviso por PRONAC, com quantos recibos e quanto
-- dinheiro ficaram de fora — a conferência pós-carga, automatizada.
--
-- Duas causas, separadas em `causa` para não caçar no cadastro um problema
-- que está no recibo:
-- • pronac_malformado: anoprojeto/sequencial do recibo fora do padrão, e a
-- macro devolve PRONAC nulo. A chave mostra os valores crus.
-- • ausente_do_cadastro: PRONAC bem formado que sac__projetos não resolve.
--
-- Conta só recibo com data e valor utilizáveis: o que a silver descarta por
-- formato não é perda de proponente. A perda em massa (cadastro vazio ou
-- parcial) é erro, no salic_recibo_sem_proponente_em_massa.
with
    recibos as (
        select
            {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
            coalesce(anoprojeto, '∅') || '/' || coalesce(sequencial, '∅') as ano_sequencial,
            {{ bronze_numerico("captacaoreal") }} as valor_captado
        from {{ ref("bronze_sac_captacao") }}
        where
            {{ bronze_timestamp("dtrecibo") }} is not null
            and {{ bronze_numerico("captacaoreal") }} is not null
    ),

    resolvidos as (
        select distinct pronac from {{ ref("silver_sac_captacao_proponente") }}
    )

select
    case
        when r.pronac is null then 'pronac_malformado' else 'ausente_do_cadastro'
    end as causa,
    coalesce(r.pronac, r.ano_sequencial) as chave,
    count(*) as qtd_recibos,
    sum(r.valor_captado) as valor_captado
from recibos r
left join resolvidos s on r.pronac = s.pronac
where s.pronac is null
group by 1, 2
