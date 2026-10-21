{{ config(materialized="table") }}

-- TABLE, e não a view padrão da pasta: ancine.consulta é carregada fora deste
-- pipeline, e uma recarga com DROP TABLE seria recusada pelo Postgres enquanto
-- houvesse view dependente. Como cópia, a bronze não prende a origem.
-- Bronze ancine — cópia fiel de ancine.consulta, como a planilha da Ancine foi
-- carregada (fora deste pipeline). Os cabeçalhos ficam como vieram, com
-- maiúscula, espaço e acento: quem resolve os nomes e tipa é silver_consulta.
select *
from {{ source("ancine", "consulta") }}
