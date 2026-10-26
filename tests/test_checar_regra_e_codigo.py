"""O CI recusa o PR que muda a regra de arquitetura junto com os modelos dbt."""

from __future__ import annotations

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "scripts"))

from checar_regra_e_codigo import conflito  # noqa: E402

VERIFICAR = ".claude/skills/arquitetura-lakehouse-minc/scripts/verificar.py"
MODELO = "dbt/minc/models/bbagil/silver_extrato_bbagil.sql"


def test_adr_e_modelo_no_mesmo_pr_e_conflito() -> None:
    regra, codigo = conflito(
        ["docs/adr/0010-camadas-e-schemas-do-data-lakehouse.md", MODELO]
    )
    assert regra == ["docs/adr/0010-camadas-e-schemas-do-data-lakehouse.md"]
    assert codigo == [MODELO]


def test_verificar_e_modelo_no_mesmo_pr_e_conflito() -> None:
    regra, codigo = conflito([VERIFICAR, MODELO, "README.md"])
    assert regra == [VERIFICAR]
    assert codigo == [MODELO]


def test_so_regra_passa() -> None:
    assert conflito(["docs/adr/0015-x.md", "docs/adr/GUIA.md", VERIFICAR]) == ([], [])


def test_so_modelo_passa() -> None:
    assert conflito([MODELO, "dbt/minc/dbt_project.yml"]) == ([], [])


def test_outros_arquivos_da_skill_nao_contam_como_regra() -> None:
    skill = ".claude/skills/arquitetura-lakehouse-minc/SKILL.md"
    assert conflito([skill, MODELO]) == ([], [])


def test_adr_com_dag_passa() -> None:
    # O ADR 0014 entrou com o código da DAG do SICONFI: a guarda é só para dbt.
    dag = "dags/data_ingest/siconfi/siconfi_ingestion_dag.py"
    assert conflito(["docs/adr/0014-x.md", dag]) == ([], [])
