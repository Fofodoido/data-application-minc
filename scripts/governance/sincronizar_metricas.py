#!/usr/bin/env python3
"""Publica as métricas do MetricFlow como métricas do OpenMetadata.

Nome, tipo, unidade, granularidade, expressão SQL, termos do glossário,
produto de dados e a linhagem da tabela de origem até a métrica. Lógica em
`helpers/openmetadata/governanca/metricas.py`.

Uso:
    make openmetadata-metricas
    make openmetadata-metricas OPENMETADATA_ARGS=--confirmar
"""

from __future__ import annotations

from _comum import preparar

from openmetadata.governanca import metricas


def main() -> int:
    preparado = preparar(__doc__.splitlines()[1])
    if preparado is None:
        return 1
    om, cat = preparado
    metricas.sincronizar(om, cat)
    return om.relatar()


if __name__ == "__main__":
    raise SystemExit(main())
