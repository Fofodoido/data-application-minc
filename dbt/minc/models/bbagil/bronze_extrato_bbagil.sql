-- Bronze bbagil — cópia fiel de bbagil.extrato_bbagil, como a extracao_bbagil_dag grava.
-- Sem cast, filtro nem join (ADR 0010): tudo continua text. Quem tipa é
-- silver_extrato_bbagil.
select * from {{ source("bbagil", "extrato_bbagil") }}
