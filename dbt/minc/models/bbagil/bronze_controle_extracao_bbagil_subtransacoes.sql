-- Bronze bbagil — cópia fiel de bbagil.controle_extracao_bbagil_subtransacoes, como a
-- extracao_bbagil_dag grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_controle_extracao_bbagil_subtransacoes.
select * from {{ source("bbagil", "controle_extracao_bbagil_subtransacoes") }}
