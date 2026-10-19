COMPOSE_FILE := infra/docker-compose.yml

# O Compose usa o .env para DUAS coisas distintas, e por padrao elas apontam
# para arquivos diferentes:
#   1. env_file:  injeta variaveis no container  -> resolvido pelo compose file
#   2. ${VAR}:    substitui dentro do proprio yml -> procurado no project dir
# Como o compose file vive em infra/, (2) procurava infra/.env, que nao existe,
# e toda ${VAR} caia no default calado. --env-file aponta (1) e (2) para o mesmo
# arquivo na raiz. Nao usar --project-directory: ele tambem reposiciona os
# caminhos dos volumes, que sao relativos a infra/.
COMPOSE := docker compose --env-file .env -f $(COMPOSE_FILE)

export PYTHONPATH := $(CURDIR)/dags:$(CURDIR)/plugins:$(CURDIR)/helpers
export MYPYPATH := $(CURDIR):$(CURDIR)/dags:$(CURDIR)/helpers:$(CURDIR)/plugins

.PHONY: setup lock dbt-manifest format lint lint-ci test compose-config \
        up up-trino down logs-airflow logs-trino trino-cli \
        docs-setup docs-collect docs-build docs-serve docs-clean \
        openmetadata openmetadata-governanca openmetadata-metricas \
        openmetadata-qualidade governance-audit-om

DOCS_DIR := docs-pages
DOCS_VENV := $(DOCS_DIR)/.venv
DOCS_PY := $(DOCS_VENV)/bin/python

setup:
	pip install poetry==1.8.5
	poetry config virtualenvs.in-project false
	poetry config warnings.export false
	poetry lock
	poetry install --no-root --with dev
	poetry export --without-hashes --format=requirements.txt > requirements.generated.txt
	bash setup-git-hooks.sh

lock:
	./infra/docker/airflow/compile-lock.sh

format:
	poetry run black .
	poetry run ruff check --fix .
	poetry run sqlfmt ./dbt

lint:
	poetry run black . --check
	poetry run ruff check .
	poetry run mypy . --explicit-package-bases --install-types --non-interactive
	poetry run sqlfmt ./dbt --check
	[ "${GITLAB_CI}" ] || poetry run sqlfluff lint ./dbt

lint-ci:
	poetry run sqlfmt ./dbt --check
	poetry run sqlfluff lint ./dbt --config .sqlfluff.ci --ignore templating

test:
	poetry run pytest tests

compose-config:
	$(COMPOSE) config

up:
	$(COMPOSE) up postgres airflow airflow-mcp

# Stack com o Trino, para a ingestão SALIC v2. Fora do `up` porque a imagem
# é grande e a JVM fica de pé consumindo memória mesmo sem ingestão rodando.
up-trino:
	$(COMPOSE) --profile trino up postgres airflow airflow-mcp trino

down:
	$(COMPOSE) down

logs-airflow:
	$(COMPOSE) logs airflow --tail=200

logs-trino:
	$(COMPOSE) logs trino --tail=200

# Console SQL do Trino — para conferir catálogo, listar schema da origem e
# rodar a query de uma fatia à mão quando a DAG falha.
trino-cli:
	$(COMPOSE) exec trino trino

# ─── Site de documentação ──────────────────────────────────────────────────
#
# Virtualenv próprio, com jinja2 e pyyaml apenas. Não precisa de Airflow, dbt
# nem Poetry — é o que permite a documentação ser construída por quem não tem o
# ambiente do pipeline montado, e pelo CI sem VPN.
#
# `docs-collect` usa rede (git, gh); `docs-build` não usa nada. A separação é de
# propósito: o build no CI nunca depende de rede nem de banco, e o diff de uma
# coleta mostra exatamente o que mudou no período.

$(DOCS_PY):
	python3 -m venv $(DOCS_VENV)
	$(DOCS_VENV)/bin/pip install --quiet --upgrade pip
	$(DOCS_VENV)/bin/pip install --quiet -r $(DOCS_DIR)/requirements.txt

docs-setup: $(DOCS_PY)

docs-collect: $(DOCS_PY)
	cd $(DOCS_DIR) && PYTHONPATH=. .venv/bin/python -m tooling.collect

docs-build: $(DOCS_PY)
	cd $(DOCS_DIR) && PYTHONPATH=. .venv/bin/python -m tooling.build

docs-serve: docs-build
	@echo "→ http://localhost:8000"
	cd $(DOCS_DIR)/site && python3 -m http.server 8000

docs-clean:
	rm -rf $(DOCS_DIR)/site $(DOCS_VENV)

# ── OpenMetadata: governança declarada em dbt/minc/governance/ ─────────────
#
# Sem OM_HOST e token no .env, roda offline e só relata o declarado. Com eles,
# simula: lê a instância e mostra a diferença, sem escrever nada. Escreve só
# com `OPENMETADATA_ARGS=--confirmar`. A DAG openmetadata_ingestion_dag aplica
# o mesmo, sempre depois das recipes.
#
# A ordem importa: produto e domínio antes da tabela que entra neles; métrica
# e teste depois da tabela de onde saem.
OPENMETADATA_ARGS ?=
OPENMETADATA_AUDIT ?=

openmetadata-governanca:
	poetry run python scripts/governance/sincronizar_governanca.py $(OPENMETADATA_ARGS)

openmetadata-metricas:
	poetry run python scripts/governance/sincronizar_metricas.py $(OPENMETADATA_ARGS)

openmetadata-qualidade:
	poetry run python scripts/governance/sincronizar_qualidade.py $(OPENMETADATA_ARGS)

openmetadata: openmetadata-governanca openmetadata-metricas openmetadata-qualidade

# Confere o que está NA INSTÂNCIA contra o que o repositório declara. Só lê.
# `OPENMETADATA_AUDIT=--strict` sai com erro se houver pendência.
governance-audit-om:
	poetry run python scripts/governance/auditar_openmetadata.py $(OPENMETADATA_AUDIT)
