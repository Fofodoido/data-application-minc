-- Silver bbagil — controle_extracao_bbagil_subtransacoes.
-- Origem: bronze_controle_extracao_bbagil_subtransacoes, cópia fiel de bbagil.controle_extracao_bbagil_subtransacoes, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 6 colunas: 0 tipadas, 6 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_texto("id_plano_acao") }} as id_plano_acao,
    {{ bronze_texto("id_plano_acao_dado_bancario") }} as id_plano_acao_dado_bancario,
    {{ bronze_texto("id_transacao_pai") }} as id_transacao_pai,
    {{ bronze_texto("status") }} as status,
    {{ bronze_texto("qtd_subtransacoes") }} as qtd_subtransacoes,
    {{ bronze_texto("mensagem_erro") }} as mensagem_erro
from {{ ref("bronze_controle_extracao_bbagil_subtransacoes") }}
