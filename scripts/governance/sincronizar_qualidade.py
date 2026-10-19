#!/usr/bin/env python3
"""Testes nativos, suítes por produto e contratos de dados no OpenMetadata.

Os testes conferem o formato declarado de cada coluna, a tabela de consumo que
esvaziou e a coluna que mudou sem o schema.yml acompanhar. Os contratos cobrem
as tabelas Gold. Lógica em `helpers/openmetadata/governanca/qualidade.py`.

Uso:
    make openmetadata-qualidade
    make openmetadata-qualidade OPENMETADATA_ARGS=--confirmar
"""

from __future__ import annotations

from _comum import preparar

from openmetadata.governanca import qualidade


def main() -> int:
    preparado = preparar(__doc__.splitlines()[1])
    if preparado is None:
        return 1
    om, cat = preparado
    qualidade.sincronizar(om, cat)
    return om.relatar()


if __name__ == "__main__":
    raise SystemExit(main())
