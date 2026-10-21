{{ config(materialized="table") }}

-- Silver — um evento por captação de projeto audiovisual registrada pela
-- Ancine, no mesmo formato dos pagamentos do BB Ágil e dos recibos da Rouanet,
-- para entrar na espinha unificada (int_bbagil_salic_ancine_eventos_fomento).
--
-- POR QUE ESTA FONTE ENTRA: a pergunta da Meta 5 cita o FSA entre os
-- mecanismos, e por muito tempo se registrou que a fonte dele era "uma
-- planilha da Ancine fora do banco". Não é: `ancine.consulta` está carregada,
-- com 57.860 linhas e 17 mecanismos, de 2005 a 2026. Ela tem a mesma estrutura
-- da captação da Rouanet — `cnpj_proponente` é quem RECEBEU e
-- `cnpj_investidor` é quem aportou.
--
-- NOMES DE COLUNA: a tabela é carregada FORA deste pipeline, direto da planilha
-- da Ancine, e os cabeçalhos entram no banco como estão lá — com maiúscula,
-- espaço e acento: "Mecanismo", "CNPJ Proponente", "Data Captação", "Valor da
-- Captação", "No SALIC". Uma delas, " UF Proponente", ainda começa com espaço.
-- O Postgres rebaixa identificador sem aspas para minúscula, então ler
-- `mecanismo` direto falha com `column "mecanismo" does not exist`. O CTE
-- `origem` resolve cada nome real via information_schema (macro
-- coalesce_por_nome, a mesma usada nos contemplados da LPG) e devolve os nomes
-- canônicos em snake_case, para que a regra de negócio abaixo não precise saber
-- de nada disso — e continue valendo se a planilha for recarregada com o
-- cabeçalho normalizado.
--
-- ESCOPO (decisão de negócio, tomada em 20/08/2026): entram o FSA e o fomento
-- federal ao audiovisual com repasse identificável ao proponente. Ficam de fora:
-- • ART25 e ART18: são Rouanet e já entram pelo SALIC, por fonte melhor;
-- incluí-los aqui duplicaria evento.
-- • RENDIMENTOS: rendimento de aplicação financeira, não é repasse a agente.
-- • CONTRAPARTIDA: dinheiro do próprio proponente.
-- • OUTRAS FONTES: origem não identificada.
-- • LEI ESTADUAL e LEI MUNICIPAL: não são política federal, que é o recorte da
-- pergunta.
-- • ART39 CONDECINE: tributo sobre o mercado audiovisual, não fomento direto ao
-- agente. Medido: acrescentaria 46 proponentes e apenas 2 correções — custo
-- de defesa alto, ganho nulo.
--
-- DOIS MECANISMOS, NÃO UM: 'FSA' responde literalmente ao que o documento da
-- meta nomeia; 'AUDIOVISUAL' carrega Lei do Audiovisual (ART1/ART3/1A/3A),
-- FUNCINES e os editais da Ancine. Colapsar tudo em "FSA" seria nome errado
-- para a maior parte das linhas.
--
-- ARMADILHAS DE FORMATO, as duas medidas nesta tabela:
-- • data_captacao: TEXT no padrão AMERICANO M/D/YYYY. '7/12/2007' é 12 de
-- julho, não 7 de dezembro. Ordenar como texto mente.
-- • valor_de_captacao: TEXT no padrão brasileiro, '50.000,00'.
-- Verificado no escopo acima: 0 datas e 0 valores fora do padrão, 0 negativos.
--
-- DOCUMENTO: a tabela é quase toda CNPJ (31.528 linhas de 14 dígitos) e tem 4
-- linhas com CPF. Proponente pessoa física é, na prática, invisível aqui — é a
-- limitação desta fonte, e ela empurra na mesma direção de todas as outras:
-- pode deixar de revelar um veterano, nunca inventar um.
{% set src = ref("bronze_consulta") %}
{% set col = {
    "mecanismo": coalesce_por_nome(src, ["mecanismo"]),
    "cnpj_proponente": coalesce_por_nome(
        src, ["cnpj proponente", "cnpj_proponente"]
    ),
    "nome_proponente": coalesce_por_nome(
        src, ["nome proponente", "nome_proponente"]
    ),
    "data_captacao": coalesce_por_nome(
        src, ["data captação", "data captacao", "data_captacao"]
    ),
    "valor_de_captacao": coalesce_por_nome(
        src, ["valor da captação", "valor da captacao", "valor_de_captacao"]
    ),
    "no_salic": coalesce_por_nome(src, ["no salic", "no_salic"]),
} %}
{#
  Sem esta guarda o modo de falha é o pior possível. coalesce_por_nome degrada
  para `cast(null as text)` quando não encontra a coluna: o WHERE não casaria
  nada, o modelo entregaria ZERO linhas e nenhum erro — a trilha do audiovisual
  sumiria da Meta 5 em silêncio, e o percentual de primeiro acesso subiria sem
  que ninguém soubesse por quê. Falhar na compilação é o comportamento correto.

  Só roda com execute=true: em `dbt parse` o adapter não consulta o banco e
  todas as colunas voltariam "não resolvidas" por construção.
#}
{% if execute %}
    {% set nao_resolvidas = [] %}
    {% for nome, expressao in col.items() %}
        {% if "cast(null as text)" in expressao %}
            {% do nao_resolvidas.append(nome) %}
        {% endif %}
    {% endfor %}
    {% if nao_resolvidas %}
        {{
            exceptions.raise_compiler_error(
                "silver_consulta: não achei em bronze_consulta (cópia de "
                ~ "ancine.consulta) a(s) coluna(s) "
                ~ nao_resolvidas
                | join(", ")
                ~ ". Veja os cabeçalhos reais com: SELECT column_name FROM "
                ~ "information_schema.columns WHERE table_schema = 'ancine' "
                ~ "AND table_name = 'consulta';"
            )
        }}
    {% endif %}
{% endif %}

with
    origem as (
        select
            {{ col["mecanismo"] }} as mecanismo,
            {{ col["cnpj_proponente"] }} as cnpj_proponente,
            {{ col["nome_proponente"] }} as nome_proponente,
            {{ col["data_captacao"] }} as data_captacao,
            {{ col["valor_de_captacao"] }} as valor_de_captacao,
            {{ col["no_salic"] }} as no_salic
        from {{ src }}
    ),

    bruto as (
        select
            case
                when mecanismo = 'FSA' then 'FSA' else 'AUDIOVISUAL'
            end as programa_fomento,
            case
                when
                    length(regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g')) <= 11
                then
                    lpad(
                        regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'), 11, '0'
                    )
                else
                    lpad(
                        regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'), 14, '0'
                    )
            end as beneficiario_documento,
            nullif(trim(nome_proponente), '') as beneficiario_nome,
            to_date(data_captacao, 'FMMM/FMDD/YYYY') as data_evento,
            replace(replace(valor_de_captacao, '.', ''), ',', '.')::numeric as valor,
            nullif(trim(no_salic), '') as no_salic
        from origem
        where
            mecanismo in (
                'FSA',
                'ART1',
                'ART3',
                'ART 1A',
                'ART 3A',
                'ART41 (FUNCINES)',
                'EDITAL ANCINE',
                'EDITAL ANCINE (PAR)',
                'OUTROS EDITAIS'
            )
            and cnpj_proponente is not null
            and data_captacao ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
            and valor_de_captacao ~ '^-?[0-9.]+,[0-9]{2}$'
            and length(regexp_replace(trim(cnpj_proponente), '[^0-9]', '', 'g'))
            between 8 and 14
    )

select
    beneficiario_documento,
    beneficiario_nome,
    programa_fomento,
    data_evento,
    valor,
    no_salic
from bruto
where
    beneficiario_documento !~ '^0+$'
    -- captação com valor zero ou negativo não deu acesso a recurso, mesmo
    -- critério aplicado à Rouanet
    and valor > 0
