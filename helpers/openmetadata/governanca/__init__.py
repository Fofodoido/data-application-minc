"""Governança declarativa do MinC no OpenMetadata.

Os conectores (recipes em `helpers/openmetadata/recipes/`) são a única fonte
da ESTRUTURA do catálogo: schema, tabela, coluna, descrição e linhagem. Este
pacote é a camada por cima, aplicada depois deles, com o que o conector não
expressa: produto de dados, permissão de uso, certificação, formato de coluna,
chaves, ontologia do glossário, métricas, testes nativos, contratos e
incidentes. Dois writers estruturais apagariam o trabalho um do outro — ver a
§12 de `docs/openmetadata/MEMORY.md`.

A declaração mora em `dbt/minc/governance/*.yml`, versionada junto do
projeto dbt que ela descreve.
"""
