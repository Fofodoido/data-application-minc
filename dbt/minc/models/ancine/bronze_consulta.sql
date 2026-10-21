-- Bronze ancine — cópia fiel de ancine.consulta, como a planilha da Ancine foi
-- carregada (fora deste pipeline). Os cabeçalhos ficam como vieram, com
-- maiúscula, espaço e acento: quem resolve os nomes e tipa é silver_consulta.
select * from {{ source("ancine", "consulta") }}
