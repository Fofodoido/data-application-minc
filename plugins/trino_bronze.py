"""Regras de escrita do SQL da ingestão SALIC bronze via Trino.

Este módulo não fala com banco nenhum: recebe metadado e devolve texto SQL. É
a metade da DAG ``salic_ingestion_trino`` que decide **o que** vai ser
executado — como a tabela é fatiada, como cada tipo do SQL Server vira texto,
que predicado recorta cada faixa. A outra metade, na DAG, só executa e registra.

A separação existe para esta metade poder ser testada sem Airflow, sem Trino e
sem VPN (ver ``tests/test_trino_bronze.py``). Errar o predicado de uma faixa é
o defeito mais caro possível aqui: não quebra nada, só carrega a tabela pela
metade — e ninguém percebe até o número aparecer errado num painel.
"""

from math import ceil
from typing import Any, Iterable

# Schema de destino no Postgres. É só o PADRÃO: o valor real vem da Variable
# ``salic_trino_bronze_schema`` e viaja no dict do alvo, porque em produção o
# banco pode ser compartilhado e o usuário do Trino só ter permissão num schema
# específico.
DEFAULT_BRONZE_SCHEMA = "bronze"

# Nome do catálogo do Trino que aponta para o data warehouse. É só o PADRÃO: o
# valor real vem da Variable ``salic_trino_target_catalog`` e viaja no dict do
# alvo, porque o nome do catálogo é uma escolha de quem administra o Trino — em
# produção ele pode não se chamar "dw".
DEFAULT_TARGET_CATALOG = "dw"

# Teto de fatias por tabela. Sem ele, uma tabela de 10 bilhões de linhas geraria
# milhares de queries e o custo de agendar cada uma passaria a dominar.
MAX_SLICES_PER_TABLE = 200

# Coluna técnica gravada em toda tabela bronze: o número da fatia que trouxe a
# linha. Existe por uma razão só, e é de arquitetura: repetir uma fatia exige
# apagar antes o que ela escreveu, e o Airflow **não fala com o Postgres** — o
# DELETE tem que sair pelo Trino. Recortar pela chave exigiria
# ``CAST("k" AS bigint)``, que o conector não empurra: o Trino recusa com
# "can not perform merge on the target table without primary keys". Comparar um
# inteiro simples empurra limpo. De quebra, dá para saber de qual fatia veio
# cada linha quando algo diverge.
SLICE_COLUMN = "_fatia"


# ── Quoting e literais ───────────────────────────────────────────────────────


def quote_ident(name: str) -> str:
    """Quota um identificador para o Trino (aspas duplas, aspas internas dobradas)."""
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    """Escreve *value* como literal SQL de texto."""
    return "'" + value.replace("'", "''") + "'"


def passthrough(catalog: str, tsql: str) -> str:
    """Envelopa T-SQL cru na table function ``query()`` do conector.

    É como se lê ``sys.partitions`` e ``sys.indexes``: o ``information_schema``
    exposto pelo Trino não conhece chave primária, coluna identity nem contagem
    de linhas — e é exatamente disso que o plano de fatias precisa.
    """
    return f"SELECT * FROM TABLE({catalog}.system.query(query => {sql_literal(tsql)}))"


# ── Conversão de tipos: tudo vira TEXT na bronze ─────────────────────────────


def cast_to_text(column: str, trino_type: str) -> str:
    """Expressão que converte *column* em ``varchar`` preservando o texto da v1.

    Quatro tipos não são um ``CAST`` direto:

    * ``boolean`` — o ``CAST`` do Trino daria ``'true'``; a v1, que passava por
      ``str()`` do Python, gravava ``'True'``.
    * ``varbinary`` — o Trino recusa o ``CAST`` para ``varchar``. Hexadecimal é
      o único formato textual que não perde o conteúdo (a v1 gravava a repr de
      ``bytes``, que não era recuperável).
    * ``json`` — o ``CAST`` só aceita um valor escalar; um objeto ou uma lista
      falha com ``INVALID_CAST_ARGUMENT``. ``json_format`` devolve o texto do
      JSON. Vem de origem PostgreSQL (``json`` e ``jsonb``): o conector do SQL
      Server não expõe esse tipo, então o SALIC não passa por aqui.
    * ``char(n)`` — sai sem o preenchimento de espaços à direita, porque é assim
      que o Trino converte ``char`` em ``varchar``. A v1 mantinha os espaços.
    """
    ident = quote_ident(column)
    base = trino_type.split("(")[0].strip().lower()

    if base == "boolean":
        return f"CASE WHEN {ident} THEN 'True' WHEN NOT {ident} THEN 'False' END"
    if base == "varbinary":
        return f"to_hex({ident})"
    if base == "json":
        return f"json_format({ident})"
    return f"CAST({ident} AS varchar)"


# ── Plano de fatias ──────────────────────────────────────────────────────────


def plan_slices(
    key_min: int | None,
    key_max: int | None,
    row_count: int,
    rows_per_slice: int,
) -> list[tuple[int | None, int | None]]:
    """Divide o domínio da chave em faixas ``[início, fim)``.

    ``None`` num extremo significa "sem limite daquele lado". Uma lista com um
    único ``(None, None)`` é a carga de uma vez só.

    A primeira faixa é aberta à esquerda e a última à direita **de propósito**.
    Assim o conjunto cobre o domínio inteiro mesmo que a origem, que é um banco
    vivo, ganhe linhas fora de ``[key_min, key_max]`` durante a carga. A faixa
    final também é quem recolhe as linhas de chave nula.
    """
    if key_min is None or key_max is None or rows_per_slice <= 0:
        return [(None, None)]
    if row_count <= rows_per_slice or key_max < key_min:
        return [(None, None)]

    wanted = ceil(row_count / rows_per_slice)
    span = key_max - key_min + 1
    n = min(wanted, MAX_SLICES_PER_TABLE, span)
    if n < 2:
        # Chave que não abre em duas faixas não tem o que fatiar — e uma faixa
        # única aqui deixaria buraco, por causa das pontas abertas.
        return [(None, None)]

    step = ceil(span / n)
    bounds: list[tuple[int | None, int | None]] = [(None, key_min + step)]
    for i in range(1, n):
        lo = key_min + i * step
        bounds.append((lo, key_min + (i + 1) * step if i < n - 1 else None))
    return bounds


def slice_predicate(key_column: str, lo: int | None, hi: int | None) -> str:
    """Predicado de uma faixa, no dialeto do Trino, sobre a tabela de origem.

    É este predicado que o conector empurra para o SQL Server — e é o que faz
    cada query ser curta o bastante para caber no ``remote query timeout``.
    """
    ident = quote_ident(key_column)
    if lo is None and hi is None:
        return ""
    if lo is None:
        return f"WHERE {ident} < {hi}"
    if hi is None:
        # Última faixa: também é o coletor das chaves nulas.
        return f"WHERE {ident} >= {lo} OR {ident} IS NULL"
    return f"WHERE {ident} >= {lo} AND {ident} < {hi}"


def slice_delete_clause(slice_index: int) -> str:
    """Recorte do que uma fatia escreveu, para poder repeti-la.

    Usa a coluna técnica ``_fatia`` em vez da chave da tabela. O porquê está em
    :data:`SLICE_COLUMN`: é a única forma de o DELETE ser empurrado pelo
    conector, e o DELETE precisa sair pelo Trino porque o Airflow não tem rota
    até o Postgres.
    """
    return f"WHERE {quote_ident(SLICE_COLUMN)} = {int(slice_index)}"


# ── Nomes ────────────────────────────────────────────────────────────────────


def bronze_table_name(database: str, table: str) -> str:
    """Nome da tabela na bronze: ``<banco>__<tabela>``, tudo em minúsculas.

    Mesmo formato da v1. As 561 fontes do dbt dependem dele.
    """
    return f"{database.lower()}__{table.lower()}"


def source_fqtn(target: dict) -> str:
    """Tabela de origem, qualificada com o catálogo do Trino."""
    return (
        f"{target['catalog']}.{quote_ident(target['schema'])}."
        f"{quote_ident(target['table'])}"
    )


def target_catalog(target: dict) -> str:
    """Catálogo de destino deste alvo, com o padrão quando não vier definido."""
    return target.get("target_catalog") or DEFAULT_TARGET_CATALOG


def bronze_schema(target: dict) -> str:
    """Schema de destino deste alvo, com o padrão quando não vier definido."""
    return target.get("bronze_schema") or DEFAULT_BRONZE_SCHEMA


def bronze_fqtn(target: dict) -> str:
    """Tabela de destino, qualificada com o catálogo do Trino."""
    return (
        f"{target_catalog(target)}.{bronze_schema(target)}."
        f"{quote_ident(target['bronze_table'])}"
    )


# ── Montagem dos comandos ────────────────────────────────────────────────────


def bronze_ddl(target: dict, columns: list[tuple[str, str]]) -> tuple[str, str]:
    """DROP e CREATE da tabela bronze, para o Trino executar.

    Todas as colunas saem ``varchar``, que o conector materializa como
    ``varchar`` no PostgreSQL. A v1 criava ``TEXT``; no PostgreSQL os dois são o
    mesmo tipo de armazenamento e o ``data_type`` declarado nas fontes do dbt é
    documentação, não contrato verificado — então a diferença não muda nada a
    jusante. O DDL sai pelo Trino porque o Airflow não tem rota até o Postgres.

    A última coluna é a técnica :data:`SLICE_COLUMN`.
    """
    table = bronze_fqtn(target)
    colunas = [f"{quote_ident(name.lower())} varchar" for name, _ in columns]
    colunas.append(f"{quote_ident(SLICE_COLUMN)} integer")
    return f"DROP TABLE IF EXISTS {table}", f"CREATE TABLE {table} ({', '.join(colunas)})"


def build_statements(
    target: dict,
    columns: list[tuple[str, str]],
    slices: list[tuple[int | None, int | None]],
) -> list[dict]:
    """Um INSERT por faixa, cada um acompanhado do DELETE que o torna repetível."""
    casts = [cast_to_text(name, dtype) for name, dtype in columns]
    insert_cols = [quote_ident(name.lower()) for name, _ in columns]
    insert_cols.append(quote_ident(SLICE_COLUMN))
    key = target.get("key_column")

    statements = []
    for index, (lo, hi) in enumerate(slices):
        where = slice_predicate(key, lo, hi) if key else ""
        select_list = ",\n       ".join([*casts, str(index)])
        statements.append(
            {
                "index": index,
                "range": f"[{lo}, {hi})",
                "insert": (
                    f"INSERT INTO {bronze_fqtn(target)} ({', '.join(insert_cols)})\n"
                    f"SELECT {select_list}\n"
                    f"FROM {source_fqtn(target)}\n{where}"
                ).rstrip(),
                "delete": (
                    f"DELETE FROM {bronze_fqtn(target)} {slice_delete_clause(index)}"
                ),
            }
        )
    return statements


# ── Leitura do metadado da origem ────────────────────────────────────────────


def metadata_key(schema: str, table: str) -> tuple[str, str]:
    """Chave de cruzamento entre o `information_schema` e o T-SQL da origem.

    Os dois lados nomeiam a mesma tabela de formas diferentes: o Trino expõe
    ``aberturadecontabancaria`` (minúsculo, porque
    ``case-insensitive-name-matching`` está ligado) e o ``sys.partitions``
    devolve ``AberturaDeContaBancaria``, o nome remoto real.

    Cruzar sem normalizar não casa **nada** — e o efeito não é um erro, é toda
    tabela ficar com contagem zero e sem chave, ou seja, o fatiamento inteiro
    desligado em silêncio. No SALIC isso valia 948 das 953 tabelas do SAC.
    """
    return (schema.lower(), table.lower())


def pick_key_columns(rows: Iterable[Any]) -> dict[tuple[str, str], str]:
    """Escolhe, por tabela, a coluna candidata de menor prioridade numérica."""
    best: dict[tuple[str, str], tuple[int, str]] = {}
    for schema, table, column, priority in rows:
        key = metadata_key(schema, table)
        if key not in best or int(priority) < best[key][0]:
            best[key] = (int(priority), column)
    return {key: column for key, (_, column) in best.items()}


def index_row_counts(rows: Iterable[Any]) -> dict[tuple[str, str], int]:
    """Indexa a contagem de linhas por tabela, com a chave normalizada."""
    return {metadata_key(r[0], r[1]): int(r[2] or 0) for r in rows}


def parse_only_tables(raw: str) -> set[tuple[str, str]]:
    """Converte o parâmetro ``only_tables`` em pares ``(banco, tabela)``."""
    pairs = set()
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        database, _, table = item.partition(".")
        pairs.add((database.strip().lower(), table.strip().lower()))
    return pairs


# ── T-SQL de metadados (roda no SQL Server, via passthrough) ─────────────────

# Contagem aproximada de TODAS as tabelas do schema numa query só, lendo o
# catálogo do servidor. Nenhuma varredura de tabela: com 561 tabelas, um
# COUNT(*) por tabela seria o plano inteiro em si.
TSQL_ROW_COUNTS = """
SELECT s.name AS schema_name,
       t.name AS table_name,
       CAST(SUM(p.rows) AS BIGINT) AS row_count
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
WHERE s.name = {schema}
GROUP BY s.name, t.name
"""

# Prioridade 1 = chave primária de coluna única e tipo inteiro. Prioridade 2 =
# coluna identity. As duas são indexadas na origem, então tanto o min()/max()
# quanto o predicado de faixa saem por busca em índice, não por varredura — é o
# que impede o fatiamento de custar mais caro que o problema que resolve.
TSQL_KEY_COLUMNS = """
SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name,
       1 AS priority
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE s.name = {schema}
  AND ty.name IN ('int', 'bigint', 'smallint')
  AND 1 = (SELECT COUNT(*) FROM sys.index_columns ic2
           WHERE ic2.object_id = i.object_id AND ic2.index_id = i.index_id)
UNION ALL
SELECT s.name, t.name, c.name, 2
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE s.name = {schema}
  AND c.is_identity = 1
  AND ty.name IN ('int', 'bigint', 'smallint')
"""
