#!/usr/bin/env python3
"""Aplica no OpenMetadata a governança declarada em `dbt/minc/governance/`.

Domínio, produto de dados, dono, tier, certificação, permissão de uso, formato
de coluna, chaves, propriedades e a ontologia do glossário. A lógica está em
`helpers/openmetadata/governanca/governanca.py`; este arquivo só a chama da
linha de comando. A DAG `openmetadata_ingestion_dag` chama a mesma função.

Três modos, pelo que houver no ambiente:

    sem OM_HOST/token     offline: relata o que está declarado
    com credencial        simulação: lê a instância e relata a diferença
    --confirmar           escreve

Uso:
    make openmetadata-governanca
    make openmetadata-governanca OPENMETADATA_ARGS=--confirmar
    poetry run python scripts/governance/sincronizar_governanca.py \
        --manifest build/openmetadata/dbt_target/manifest.json
"""

from __future__ import annotations

from _comum import preparar

from openmetadata.glossary import load_glossary
from openmetadata.governanca import config
from openmetadata.governanca.governanca import sincronizar


def main() -> int:
    preparado = preparar(__doc__.splitlines()[1])
    if preparado is None:
        return 1
    om, cat = preparado
    _, termos = load_glossary(str(config.GLOSSARIO))
    sincronizar(om, cat, termos)
    return om.relatar()


if __name__ == "__main__":
    raise SystemExit(main())
