"""Geracao dos artefatos dbt que a recipe de dbt consome."""

import logging
import os
import shutil
import subprocess
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

DBT_COMMANDS = (
    ("deps",),
    ("docs", "generate"),
)

# `dbt test` vem DEPOIS de `docs generate`, e nao e detalhe: o `docs generate`
# tambem grava um `run_results.json`, so que sem resultado de teste nenhum. Era
# o que chegava ao conector -- os 5.800 casos de teste existiam no catalogo,
# todos sem resultado, e nenhum incidente podia abrir.
#
# O recorte padrao exclui a bronze do SALIC e as sources brutas dele: sao 4.883
# testes `not_null`/`unique` sobre views que leem 157 M de linhas, e roda-los a
# cada carga do catalogo custaria horas de banco. Ficam 495 testes. Para mudar,
# `OM_DBT_TEST_EXCLUDE` (separado por espaco) ou `OM_DBT_RUN_TESTS=false`.
DBT_TEST_EXCLUDE_PADRAO = (
    "path:models/salic_dbt/bronze",
    "source:bronze_agentes",
    "source:bronze_bdcorporativo",
    "source:bronze_controledeacesso",
    "source:bronze_sac",
    "source:bronze_sac_legado",
    "source:bronze_tabelas",
    "source:dados_salic",
)

# Sem manifest nao ha o que ingerir. Os outros dois enriquecem e podem faltar:
# no schema do OpenMetadata so dbtManifestFilePath e obrigatorio.
REQUIRED_DBT_ARTIFACTS = ("manifest.json",)
OPTIONAL_DBT_ARTIFACTS = ("catalog.json", "run_results.json")


def _dbt_test_args() -> list[str] | None:
    """Argumentos do `dbt test`, ou None quando os testes estao desligados."""
    bruto = os.environ.get("OM_DBT_RUN_TESTS", "true").strip().lower()
    if bruto not in {"1", "true", "yes", "on", "sim"}:
        return None
    exclude = os.environ.get("OM_DBT_TEST_EXCLUDE")
    excluidos = exclude.split() if exclude is not None else list(DBT_TEST_EXCLUDE_PADRAO)
    return ["test", *(["--exclude", *excluidos] if excluidos else [])]


def prepare_dbt_artifacts(project_dir: str, tmp_dirs: ExitStack) -> str:
    source_project_dir = Path(project_dir)

    if not source_project_dir.exists():
        raise FileNotFoundError(f"Projeto dbt não encontrado: {source_project_dir}")

    workdir = Path(tmp_dirs.enter_context(TemporaryDirectory(prefix="om_dbt_")))
    project_copy = workdir / "dbt_project"

    shutil.copytree(
        source_project_dir,
        project_copy,
        ignore=shutil.ignore_patterns(
            "target",
            "logs",
            "dbt_packages",
            ".venv",
            "__pycache__",
        ),
    )

    env = os.environ.copy()

    for dbt_command in DBT_COMMANDS:
        cmd = [
            "dbt",
            *dbt_command,
            "--project-dir",
            str(project_copy),
            "--profiles-dir",
            str(project_copy),
        ]
        logging.info(
            "[openmetadata.execution] Executando comando: %s",
            " ".join(cmd),
        )
        subprocess.run(
            cmd,
            cwd=str(project_copy),
            env=env,
            check=True,
        )

    teste = _dbt_test_args()
    if teste is not None:
        cmd = [
            "dbt",
            *teste,
            "--project-dir",
            str(project_copy),
            "--profiles-dir",
            str(project_copy),
        ]
        logging.info("[openmetadata.execution] Executando comando: %s", " ".join(cmd))
        # check=False de proposito: teste que falha e o dado que queremos levar
        # ao OpenMetadata, nao motivo para abortar. O codigo 1 do dbt quer
        # dizer "algum teste falhou"; acima disso e erro de execucao.
        resultado = subprocess.run(cmd, cwd=str(project_copy), env=env, check=False)
        if resultado.returncode > 1:
            raise RuntimeError(
                f"dbt test terminou com erro de execucao ({resultado.returncode})"
            )

    target_dir = project_copy / "target"

    for file_name in REQUIRED_DBT_ARTIFACTS:
        artifact = target_dir / file_name
        if not artifact.exists():
            raise FileNotFoundError(f"{file_name} não encontrado em {artifact}")

    ausentes = [
        nome for nome in OPTIONAL_DBT_ARTIFACTS if not (target_dir / nome).exists()
    ]
    if ausentes:
        logging.warning(
            "[openmetadata] Artefatos opcionais ausentes (%s). A ingestao segue "
            "com o manifest; o OpenMetadata perde tipos/estatisticas de execucao.",
            ", ".join(ausentes),
        )

    return str(target_dir)
