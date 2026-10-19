#!/usr/bin/env python3
"""Confere o que está na instância contra o que o repositório declara.

Cobertura por produto (descrição, domínio, produto, dono, tier, uso,
certificação, colunas, formato), linhagem de coluna, testes sem resultado,
incidentes abertos e métricas fora da ontologia. Só lê. Lógica em
`helpers/openmetadata/governanca/auditoria.py`.

Uso:
    make governance-audit-om
    make governance-audit-om OPENMETADATA_AUDIT="--strict --saida build/auditoria.md"
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ / "helpers"))

from openmetadata.governanca import auditoria, catalogo, config  # noqa: E402
from openmetadata.governanca.cliente import ClienteOM  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--manifest", help="manifest.json já gerado")
    parser.add_argument("--saida", help="grava o relatório em markdown neste caminho")
    parser.add_argument(
        "--strict", action="store_true", help="sai com erro se houver pendência"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    acesso = config.acesso()
    if not (acesso.url and acesso.token):
        print("A auditoria lê a instância: defina OM_HOST e o token no .env.")
        return 1
    if args.manifest:
        manifest = catalogo.carregar_manifest(args.manifest)
    else:
        with TemporaryDirectory(prefix="om_dbt_parse_") as tmp:
            manifest = catalogo.carregar_manifest(catalogo.gerar_manifest(Path(tmp)))
    cat = catalogo.montar(manifest)
    rel = auditoria.auditar(ClienteOM(acesso.url, acesso.token), cat)
    texto = rel.markdown()
    print(texto)
    if args.saida:
        Path(args.saida).parent.mkdir(parents=True, exist_ok=True)
        Path(args.saida).write_text(texto + "\n", encoding="utf-8")
    return 1 if args.strict and rel.pendencias else 0


if __name__ == "__main__":
    raise SystemExit(main())
