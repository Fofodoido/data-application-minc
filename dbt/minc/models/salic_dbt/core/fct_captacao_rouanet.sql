-- Silver SALIC / core -- fato da captacao de recurso na Lei Rouanet.
--
-- GRAO: 1 linha por recibo de captacao. A chave e `id_captacao`.
--
-- O QUE E UMA CAPTACAO. Na Rouanet o MinC nao repassa dinheiro ao agente: o
-- projeto aprovado capta de um incentivador, que abate o valor do imposto
-- devido. O recibo de captacao e o registro de que o recurso efetivamente
-- entrou -- e por isso ele, e nao a portaria de aprovacao, e a medida de
-- "recurso captado" das Metas 3 e 5. Projeto aprovado que nunca captou nao deu
-- acesso a recurso nenhum ao proponente, e e o desfecho de boa parte da base.
--
-- CAPTACAO NAO E PAGAMENTO. O documento publico ligado ao FigJam registra o
-- cuidado explicitamente: captado, comprovado e pago sao tres medidas
-- diferentes, e um indicador nunca pode usar uma no numerador e outra no
-- denominador. Este fato responde por "captado". "Pago" vem da view detalhada
-- de pagamento a fornecedor, que e a Onda 2 e zona restrita.
--
-- O MECENAS FICA DE FORA, DE PROPOSITO. `cgccpfmecena` identifica o
-- incentivador que aportou. Ele nao entra: no lugar fica
-- `tipo_pessoa_mecenas`, derivado apenas do comprimento do documento, que
-- responde "pessoa fisica ou juridica" sem carregar o identificador. E o que
-- deixa este fato candidato a publicacao depois da validacao de seguranca --
-- com o documento dentro, ele seria restrito como a ponte de proponente.
-- Analise por incentivador, se for necessaria, pede um fato restrito proprio.
--
-- LINHAS PRESERVADAS. Valor zero ou negativo (estorno de recibo) continua na
-- tabela. Filtrar aqui esconderia estorno de quem soma, e a decisao de o que
-- entra em cada indicador e do gold, com a regra declarada. Sai apenas o que
-- nao identifica evento: recibo sem PRONAC resolvido, sem id ou sem data.
--
-- LIMITE CONHECIDO: a reconciliacao por ano/PRONAC com
-- `salic/silver_legado_sac_captacao_proponente` -- que le a bronze v1, no
-- schema `bronze`, e nao esta v2 -- e o gate G3 e ainda nao foi feita. Ate la
-- os dois convivem, e nenhum substitui o outro.
select
    idcaptacao as id_captacao,
    {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
    numerorecibo as numero_recibo,
    dtrecibo::date as data_recibo,
    extract(year from dtrecibo)::integer as ano_recibo,
    dtchegadarecibo::date as data_chegada_recibo,
    captacaoreal as valor_captado_reais,
    captacaoufir as valor_captado_ufir,
    tipoapoio as codigo_tipo_apoio,
    medidaprovisoria as medida_provisoria,
    {{ tipo_pessoa_documento("cgccpfmecena") }} as tipo_pessoa_mecenas,
    sitransferenciarecurso as situacao_transferencia_recurso,
    dttransferenciarecurso::date as data_transferencia_recurso,
    isbemservico as indicador_bem_servico
from {{ ref("sac__captacao") }}
where
    idcaptacao is not null
    and dtrecibo is not null
    and {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} is not null
