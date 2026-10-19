"""Montagem comum dos pontos de entrada de `scripts/governance/`.

A lógica está em `helpers/openmetadata/governanca/`; estes scripts só a chamam
da linha de comando, com os três modos: sem credencial (offline), com
credencial (simulação, só leitura) e `--confirmar` (escreve).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

RAIZ = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(RAIZ / "helpers"))

from openmetadata.governanca import catalogo, config  # noqa: E402
from openmetadata.governanca.catalogo import Catalogo  # noqa: E402
from openmetadata.governanca.cliente import ClienteOM  # noqa: E402


def preparar(descricao: str) -> tuple[ClienteOM, Catalogo] | None:
    """Lê argumentos, monta o catálogo e o cliente. None se a declaração falha."""
    parser = argparse.ArgumentParser(description=descricao)
    parser.add_argument("--confirmar", action="store_true", help="escreve de fato")
    parser.add_argument(
        "--manifest", help="manifest.json já gerado; sem ele, roda `dbt parse`"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.manifest:
        manifest = catalogo.carregar_manifest(args.manifest)
    else:
        with TemporaryDirectory(prefix="om_dbt_parse_") as tmp:
            manifest = catalogo.carregar_manifest(catalogo.gerar_manifest(Path(tmp)))
    cat = catalogo.montar(manifest)
    if cat.erros:
        print("Declaração inconsistente — nada é aplicado enquanto houver erro:")
        for erro in cat.erros:
            print(f"  - {erro}")
        return None
    acesso = config.acesso()
    om = ClienteOM(
        acesso.url,
        acesso.token,
        confirmar=args.confirmar,
        token_de_rotulos=acesso.token_de_rotulos,
    )
    om.cabecalho()
    return om, cat
