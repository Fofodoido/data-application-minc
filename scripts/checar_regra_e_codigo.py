#!/usr/bin/env python3
"""Recusa o PR que muda a regra de arquitetura junto com o código que ela confere.

A regra está nos ADRs (``docs/adr/``) e no ``verificar.py`` da skill
``arquitetura-lakehouse-minc``. O código é ``dbt/minc/models/``. Quando os dois
mudam no mesmo PR, a regra se ajusta ao código, e não o contrário. Por isso a
mudança de regra vai num PR próprio, aprovado antes (``docs/adr/GUIA.md``).

Uso no CI::

    python3 scripts/checar_regra_e_codigo.py origin/main

Sai com 1 e lista os arquivos quando o diff contra a base tem os dois lados.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Iterable

REGRA = (
    "docs/adr/",
    ".claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py",
)
CODIGO = ("dbt/minc/models/",)


def _casa(arquivo: str, prefixos: tuple[str, ...]) -> bool:
    return any(arquivo == p or arquivo.startswith(p) for p in prefixos)


def conflito(arquivos: Iterable[str]) -> tuple[list[str], list[str]]:
    """Arquivos de regra e de código do diff; os dois vazios ou um só é ok."""
    lista = sorted(set(arquivos))
    regra = [a for a in lista if _casa(a, REGRA)]
    codigo = [a for a in lista if _casa(a, CODIGO)]
    if regra and codigo:
        return regra, codigo
    return [], []


def arquivos_do_diff(base: str) -> list[str]:
    saida = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [linha for linha in saida.splitlines() if linha]


def main(argv: list[str]) -> int:
    base = argv[1] if len(argv) > 1 else "origin/main"
    regra, codigo = conflito(arquivos_do_diff(base))
    if not regra:
        print("ok: o PR não muda regra de arquitetura junto com dbt/minc/models/")
        return 0
    print("Este PR muda a regra de arquitetura e o código que ela confere.")
    print("Separe em dois PRs: primeiro o da regra, depois o do código.")
    print("Ver docs/adr/GUIA.md.\n")
    print("Regra:")
    for arquivo in regra:
        print(f"  {arquivo}")
    print("Código:")
    for arquivo in codigo:
        print(f"  {arquivo}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
