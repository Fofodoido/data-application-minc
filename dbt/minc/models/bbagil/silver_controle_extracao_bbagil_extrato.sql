-- Silver bbagil — controle_extracao_bbagil_extrato.
-- Origem: bronze_controle_extracao_bbagil_extrato, cópia fiel de bbagil.controle_extracao_bbagil_extrato, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 7 colunas: 5 tipadas, 2 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("id_plano_acao") }} as id_plano_acao,
    {{ bronze_inteiro("id_plano_acao_dado_bancario") }} as id_plano_acao_dado_bancario,
    {{ bronze_data("periodo_inicial") }} as periodo_inicial,
    {{ bronze_data("periodo_final") }} as periodo_final,
    {{ bronze_texto("status") }} as status,
    {{ bronze_inteiro("qtd_transacoes") }} as qtd_transacoes,
    {{ bronze_texto("mensagem_erro") }} as mensagem_erro
from {{ ref("bronze_controle_extracao_bbagil_extrato") }}
