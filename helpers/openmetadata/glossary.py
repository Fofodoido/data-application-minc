"""Validacao e criacao idempotente de glossarios no OpenMetadata.

Roda como primeira task da DAG porque o conector dbt precisa que os termos
referenciados em `meta.openmetadata.glossary` ja existam: referencia a FQN que
nao existe e descartada em silencio.

So CRIA o que falta. Atualizar o conteudo de um termo existente e aplicar as
relacoes tipadas e trabalho de `openmetadata.governanca.governanca`, que roda
por ultimo e compara antes de escrever. O PUT de termo so aceita relacao
generica (`relatedTo`): repetido a cada execucao, ele apagaria o tipo que o
PATCH da governanca acabou de pôr.
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import yaml

CSV_COLUMNS = (
    "parent",
    "name",
    "displayName",
    "description",
    "synonyms",
    "relatedTerms",
    "references",
    "tags",
)


def _split_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(";") if item.strip()]


def _parse_references(value: str, row_number: int) -> list[dict[str, str]]:
    items = _split_list(value)
    if len(items) % 2:
        raise ValueError(f"Linha {row_number}: references deve alternar nome e URL.")

    references = []
    for index in range(0, len(items), 2):
        name, endpoint = items[index : index + 2]
        parsed_url = urlparse(endpoint)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError(
                f"Linha {row_number}: URL de referencia invalida: {endpoint}"
            )
        references.append({"name": name, "endpoint": endpoint})
    return references


def _normalize_api_url(host_port: str) -> str:
    host_port = host_port.strip().rstrip("/")
    if not host_port:
        raise ValueError("OM_HOST nao pode ser vazio.")
    if host_port.endswith("/api/v1"):
        return host_port
    if host_port.endswith("/api"):
        return f"{host_port}/v1"
    return f"{host_port}/api/v1"


def _parse_row(row: dict, row_number: int, glossary_name: str) -> dict:
    """Valida e converte uma linha do CSV de termos."""
    if None in row:
        # Virgula fora de aspas: o csv parte o campo em dois e a descricao
        # chega truncada ao catalogo, sem erro nenhum.
        raise ValueError(
            f"Linha {row_number}: campos a mais. Ponha aspas no texto que tem virgula."
        )
    if row["relatedTerms"].strip():
        raise ValueError(
            f"Linha {row_number}: relatedTerms fica vazio. As relacoes entre "
            "termos, com tipo, ficam em dbt/minc/governance/termos_minc.yml."
        )
    name = row["name"].strip()
    description = row["description"].strip()
    parent = row["parent"].strip()
    if not name or not description:
        raise ValueError(f"Linha {row_number}: name e description sao obrigatorios.")
    if "." in name:
        raise ValueError(f"Linha {row_number}: name nao pode conter ponto: {name}")
    if parent and not parent.startswith(f"{glossary_name}."):
        raise ValueError(f"Linha {row_number}: parent deve pertencer a {glossary_name}.")
    return {
        "row_number": row_number,
        "fullyQualifiedName": f"{parent}.{name}" if parent else f"{glossary_name}.{name}",
        "parent": parent,
        "name": name,
        "displayName": row["displayName"].strip() or name,
        "description": description,
        "synonyms": _split_list(row["synonyms"]),
        "references": _parse_references(row["references"], row_number),
        "tags": _split_list(row["tags"]),
    }


def load_glossary(glossary_definition_path: str) -> tuple[dict, list[dict]]:
    """Carrega e valida a definicao do glossario e o CSV oficial de termos."""
    definition_path = Path(glossary_definition_path)
    if not definition_path.is_file():
        raise FileNotFoundError(
            f"Definicao de glossario nao encontrada: {definition_path}"
        )

    definition = yaml.safe_load(definition_path.read_text(encoding="utf-8"))
    if not isinstance(definition, dict):
        raise ValueError("A definicao do glossario deve ser um objeto YAML.")

    required_definition_fields = {
        "name",
        "displayName",
        "description",
        "mutuallyExclusive",
        "termsFile",
    }
    missing_definition_fields = required_definition_fields - definition.keys()
    if missing_definition_fields:
        raise ValueError(
            "Campos ausentes na definicao do glossario: "
            + ", ".join(sorted(missing_definition_fields))
        )

    glossary_name = str(definition["name"]).strip()
    terms_path = definition_path.parent / str(definition["termsFile"])
    if not terms_path.is_file():
        raise FileNotFoundError(f"CSV de termos nao encontrado: {terms_path}")

    with terms_path.open(encoding="utf-8", newline="") as terms_file:
        reader = csv.DictReader(terms_file)
        if tuple(reader.fieldnames or ()) != CSV_COLUMNS:
            raise ValueError(
                "Cabecalho invalido no CSV. Esperado: " + ",".join(CSV_COLUMNS)
            )

        terms = [
            _parse_row(row, row_number, glossary_name)
            for row_number, row in enumerate(reader, start=2)
        ]

    fqn_set = {term["fullyQualifiedName"] for term in terms}
    if len(fqn_set) != len(terms):
        raise ValueError("O CSV possui termos com FQN duplicado.")

    for term in terms:
        if term["parent"] and term["parent"] not in fqn_set:
            raise ValueError(
                f"Linha {term['row_number']}: parent inexistente: {term['parent']}"
            )

    terms.sort(key=lambda term: str(term["fullyQualifiedName"]).count("."))
    return definition, terms


def _term_payload(glossary_name: str, term: dict) -> dict:
    payload = {
        "name": term["name"],
        "displayName": term["displayName"],
        "description": term["description"],
        "glossary": glossary_name,
    }
    if term["parent"]:
        payload["parent"] = term["parent"]
    if term["synonyms"]:
        payload["synonyms"] = term["synonyms"]
    if term["references"]:
        payload["references"] = term["references"]
    if term["tags"]:
        payload["tags"] = [{"tagFQN": tag} for tag in term["tags"]]
    return payload


def _exists(api_url: str, resource: str, fqn: str, jwt_token: str) -> bool:
    request = Request(
        url=f"{api_url}/{resource}/name/{quote(fqn, safe='')}",
        headers={"Authorization": f"Bearer {jwt_token}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=30):
            return True
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise RuntimeError(
            f"OpenMetadata recusou GET {resource} {fqn} ({exc.code})"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Nao foi possivel acessar o OpenMetadata em {api_url}: {exc.reason}"
        ) from exc


def _put_json(api_url: str, resource: str, payload: dict, jwt_token: str) -> dict:
    request = Request(
        url=f"{api_url}/{resource}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        },
        method="PUT",
    )

    try:
        with urlopen(request, timeout=30) as response:
            response_body = response.read().decode("utf-8")
    except HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"OpenMetadata recusou PUT {resource} ({exc.code}): {response_body}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Nao foi possivel acessar o OpenMetadata em {api_url}: {exc.reason}"
        ) from exc

    return json.loads(response_body) if response_body else {}


def sync_glossary(
    glossary_definition_path: str,
    host_port: str,
    jwt_token: str,
    dry_run: bool = False,
) -> dict[str, int | str]:
    """Cria o glossario e os termos que faltam, sem tocar nos que existem."""
    definition, terms = load_glossary(glossary_definition_path)
    glossary_name = str(definition["name"]).strip()

    summary: dict[str, int | str] = {
        "glossary": glossary_name,
        "terms": len(terms),
        "created": 0,
    }
    if dry_run:
        logging.info("Glossario validado em dry-run: %s", summary)
        return summary

    jwt_token = jwt_token.strip()
    if not jwt_token:
        raise ValueError("INGESTION_TOKEN nao pode ser vazio.")

    api_url = _normalize_api_url(host_port)
    if not _exists(api_url, "glossaries", glossary_name, jwt_token):
        _put_json(
            api_url,
            "glossaries",
            {
                "name": glossary_name,
                "displayName": str(definition["displayName"]).strip(),
                "description": str(definition["description"]).strip(),
                "mutuallyExclusive": bool(definition["mutuallyExclusive"]),
            },
            jwt_token,
        )
        summary["created"] = int(summary["created"]) + 1

    # `terms` vem ordenado por profundidade: o pai sempre existe antes do filho.
    for term in terms:
        if _exists(api_url, "glossaryTerms", term["fullyQualifiedName"], jwt_token):
            continue
        _put_json(api_url, "glossaryTerms", _term_payload(glossary_name, term), jwt_token)
        summary["created"] = int(summary["created"]) + 1

    logging.info("Glossario sincronizado: %s", summary)
    return summary
