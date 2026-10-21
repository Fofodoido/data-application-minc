#!/usr/bin/env python3
"""Verifica o projeto dbt contra as regras do ADR 0010 (camadas e nomenclatura).

Só apura — não interpreta, não sugere refactor além do destino mecânico de um
arquivo fora do padrão. É de propósito: os números da verificação saem daqui,
e não da leitura do modelo (ADR 0004).

Roda offline: sem banco, sem `dbt run`, sem manifest. Lê os `.sql` com regex,
o `dbt_project.yml` com PyYAML.

Uso:
  verificar.py                 # só os arquivos mudados em relação à main
  verificar.py --tudo          # o projeto inteiro (inventário da migração)
  verificar.py <caminhos...>   # arquivos ou pastas específicos
  verificar.py --json          # saída em JSON, para outro script ler

Sai com 1 se houver violação no escopo; 0 se não houver.
"""

from __future__ import annotations

import argparse
import json
import re
import signal
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[4]
PROJETO = RAIZ / "dbt" / "minc"
MODELS = PROJETO / "models"

# Produtos de dados (Gold) moram em `data_products/<produto>/`: o produto é a
# subpasta, sem lista a manter. Qualquer outra pasta de primeiro nível é
# fonte, exceto `intermediate`.
DATA_PRODUCTS = "data_products"
INTERMEDIATE = "intermediate"

RE_REF = re.compile(r"""\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}""")
RE_SOURCE = re.compile(r"""source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)""")
RE_COMENTARIO = re.compile(r"--[^\n]*|/\*.*?\*/|\{#.*?#\}", re.S)
RE_CONFIG = re.compile(r"\{\{\s*config\(.*?\)\s*\}\}", re.S)
RE_BRONZE_PROIBIDO = re.compile(
    r"\b(join|where|case|cast|group\s+by|distinct|union)\b|::", re.I
)
RE_FALSE_POR_AUSENCIA = re.compile(r"coalesce\s*\([^)]*\bfalse\b[^)]*\)", re.I)


@dataclass
class Modelo:
    caminho: Path
    pasta: str  # a fonte, `intermediate` ou, em data_products/, o produto
    produto: bool
    nome: str
    camada: str  # bronze | silver | intermediate | gold | fora
    refs: list[str]
    sources: list[tuple[str, str]]
    corpo: str


@dataclass
class Relatorio:
    violacoes: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    avisos: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    sugestoes: list[str] = field(default_factory=list)

    def viola(self, regra: str, msg: str) -> None:
        self.violacoes[regra].append(msg)

    def avisa(self, regra: str, msg: str) -> None:
        self.avisos[regra].append(msg)


def _camada(pasta: str, produto: bool, nome: str) -> str:
    if pasta == INTERMEDIATE:
        return "intermediate" if nome.startswith("int_") else "fora"
    if pasta == DATA_PRODUCTS:
        return "fora"  # arquivo solto em data_products/, fora de um produto
    if produto:
        if nome.startswith(("bronze_", "silver_", "int_")):
            return "fora"
        return "gold"
    if nome.startswith("bronze_"):
        return "bronze"
    if nome.startswith("silver_"):
        return "silver"
    return "fora"


def carregar(caminho: Path) -> Modelo:
    texto = caminho.read_text(encoding="utf-8")
    corpo = RE_COMENTARIO.sub("", texto)
    corpo = RE_CONFIG.sub("", corpo)
    rel = caminho.relative_to(MODELS)
    produto = rel.parts[0] == DATA_PRODUCTS and len(rel.parts) > 2
    if produto:
        pasta = rel.parts[1]
    else:
        pasta = rel.parts[0] if len(rel.parts) > 1 else ""
    nome = caminho.stem
    return Modelo(
        caminho=caminho,
        pasta=pasta,
        produto=produto,
        nome=nome,
        camada=_camada(pasta, produto, nome),
        refs=RE_REF.findall(corpo),
        sources=RE_SOURCE.findall(corpo),
        corpo=corpo,
    )


def indexar() -> dict[str, Modelo]:
    """Todos os modelos do projeto, por nome. Necessário mesmo em escopo
    parcial: para saber a camada do que um modelo referencia."""
    return {m.nome: m for m in (carregar(p) for p in sorted(MODELS.rglob("*.sql")))}


def escopo_diff() -> list[Path]:
    cmd = ["git", "-C", str(RAIZ), "diff", "--name-only", "main...HEAD", "--"]
    mudados = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
    cmd = ["git", "-C", str(RAIZ), "ls-files", "--others", "--exclude-standard"]
    novos = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
    cmd = ["git", "-C", str(RAIZ), "diff", "--name-only", "--"]
    nao_staged = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
    linhas = set(mudados.split() + novos.split() + nao_staged.split())
    return sorted(
        RAIZ / linha
        for linha in linhas
        if linha.startswith("dbt/minc/models/")
        and linha.endswith(".sql")
        and (RAIZ / linha).exists()
    )


def escopo_caminhos(args: list[str]) -> list[Path]:
    saida: list[Path] = []
    for a in args:
        p = Path(a)
        p = p if p.is_absolute() else RAIZ / p
        if p.is_dir():
            saida.extend(sorted(p.rglob("*.sql")))
        elif p.suffix == ".sql":
            saida.append(p)
    return saida


def schemas_do_projeto() -> dict[str, str]:
    """pasta -> +schema declarado no dbt_project.yml: o primeiro nível e, dentro
    de `data_products`, cada produto."""
    try:
        import yaml
    except ImportError:
        return {}
    cfg = yaml.safe_load((PROJETO / "dbt_project.yml").read_text(encoding="utf-8"))
    modelos = (cfg.get("models") or {}).get(cfg.get("name", "minc")) or {}
    saida = {}
    for pasta, conf in modelos.items():
        if isinstance(conf, dict) and "+schema" in conf:
            saida[pasta] = str(conf["+schema"])
    for produto, conf in (modelos.get(DATA_PRODUCTS) or {}).items():
        if isinstance(conf, dict) and "+schema" in conf:
            saida[produto] = str(conf["+schema"])
    return saida


def sugerir_destino(m: Modelo, indice: dict[str, Modelo]) -> str:
    """Destino mecânico para um arquivo fora do padrão. É sugestão: a camada
    certa depende do que o SQL faz, e isso quem decide é quem migra."""
    pastas_ref = {indice[r].pasta for r in m.refs if r in indice}
    fontes_src = {s[0] for s in m.sources}
    if m.produto:
        return "já está em pasta de produto; só o nome está fora do padrão"
    if m.refs and len(pastas_ref | fontes_src) > 1:
        n = len(pastas_ref | fontes_src)
        return f"cruza {n} origens → intermediate/int_<f1>_<f2>.sql"
    if "gold" in m.caminho.parts:
        return f"é gold → {DATA_PRODUCTS}/<produto>/<nome>.sql, convenção do produto"
    if m.sources and not m.refs:
        return (
            "lê só source → silver_<entidade>.sql na pasta da fonte (tipa) "
            "ou bronze_ (se for cópia fiel)"
        )
    return "silver_<entidade>.sql na pasta da fonte, se não cruzar fonte"


def _regra_r1(m: Modelo, rel: str, caminho: Path, r: Relatorio) -> None:
    # fonte/arquivo.sql e intermediate/arquivo.sql; data_products/<produto>/arquivo.sql
    if len(caminho.relative_to(MODELS).parts) != (3 if m.produto else 2):
        r.viola(
            "R1 pasta e nome", f"{rel}: subpasta na fonte/produto; arquivos são planos"
        )


def _regra_r6(m: Modelo, rel: str, r: Relatorio) -> None:
    if RE_FALSE_POR_AUSENCIA.search(m.corpo):
        r.avisa(
            "R6 ausente fica nulo", f"{rel}: coalesce(..., false): ausência vira falso?"
        )


def _regra_bronze(m: Modelo, rel: str, r: Relatorio) -> None:
    """R2 — bronze lê exatamente uma source e nada mais."""
    if len(m.sources) != 1:
        n = len(m.sources)
        r.viola("R2 bronze", f"{rel}: bronze lê {n} source(s); deve ler exatamente 1")
    if m.refs:
        r.viola("R2 bronze", f"{rel}: bronze com ref(); bronze não lê modelo")
    achado = RE_BRONZE_PROIBIDO.search(m.corpo)
    if achado:
        tok = achado.group(0).strip()
        r.viola(
            "R2 bronze", f"{rel}: bronze com `{tok}`; bronze não tipa, filtra nem cruza"
        )


def _regra_silver(
    m: Modelo, rel: str, refs: dict[str, Modelo | None], r: Relatorio
) -> None:
    """R3 — silver só referencia bronze/silver da mesma fonte; não lê source."""
    if m.sources:
        r.viola("R3 silver", f"{rel}: silver com source(); silver lê a bronze por ref()")
    for ref, alvo in refs.items():
        if alvo is None:
            continue
        if alvo.camada not in ("bronze", "silver"):
            r.viola(
                "R3 silver",
                f"{rel}: ref('{ref}') é {alvo.camada}; silver só lê bronze/silver",
            )
        if alvo.pasta != m.pasta:
            r.viola(
                "R3 silver",
                f"{rel}: ref('{ref}') é da fonte `{alvo.pasta}`; silver não cruza",
            )


def _regra_consome_silver(
    camada: str,
    regra: str,
    m: Modelo,
    rel: str,
    refs: dict[str, Modelo | None],
    r: Relatorio,
) -> None:
    """R4 (intermediate) e R5 (gold): só referenciam silver ou intermediate.
    A gold também lê gold do MESMO produto: é onde a regra de negócio que as
    golds de consumo compartilham fica escrita uma vez só."""
    if m.sources:
        r.viola(regra, f"{rel}: {camada} com source()")
    for ref, alvo in refs.items():
        if alvo is None or alvo.camada in ("silver", "intermediate"):
            continue
        if camada == "gold" and alvo.camada == "gold" and alvo.pasta == m.pasta:
            continue
        r.viola(
            regra,
            f"{rel}: ref('{ref}') é {alvo.camada}"
            + (f" do produto `{alvo.pasta}`" if alvo.camada == "gold" else "")
            + f"; {camada} lê só silver/int"
            + (" ou gold do próprio produto" if camada == "gold" else ""),
        )


def _regra_schema(pastas: set[str], r: Relatorio) -> None:
    """R7 — o +schema da pasta é o nome da pasta."""
    schemas = schemas_do_projeto()
    for pasta in sorted(p for p in pastas if p and p != DATA_PRODUCTS):
        declarado = schemas.get(pasta)
        if declarado is None:
            r.viola("R7 schema", f"models/{pasta}: sem `+schema` no dbt_project.yml")
        elif declarado != pasta:
            r.viola(
                "R7 schema",
                f"models/{pasta}: `+schema: {declarado}` ≠ nome da pasta; "
                f"renomeie a pasta para `{declarado}` (o schema costuma ser o alvo)",
            )


def verificar(escopo: list[Path], indice: dict[str, Modelo], tudo: bool) -> Relatorio:
    r = Relatorio()
    pastas_vistas: set[str] = set()

    for caminho in escopo:
        m = indice.get(caminho.stem) or carregar(caminho)
        rel = str(caminho.relative_to(RAIZ))
        pastas_vistas.add(m.pasta)

        if m.camada == "fora":
            r.viola("R1 pasta e nome", f"{rel}: nome não segue a camada da pasta")
            if tudo:
                r.sugestoes.append(f"{rel} → {sugerir_destino(m, indice)}")
            continue

        _regra_r1(m, rel, caminho, r)
        _regra_r6(m, rel, r)

        refs = {ref: indice.get(ref) for ref in m.refs}
        for ref, alvo in refs.items():
            if alvo is None:
                r.avisa("ref desconhecida", f"{rel}: ref('{ref}') não existe no projeto")

        if m.camada == "bronze":
            _regra_bronze(m, rel, r)
        elif m.camada == "silver":
            _regra_silver(m, rel, refs, r)
        elif m.camada == "intermediate":
            _regra_consome_silver("intermediate", "R4 intermediate", m, rel, refs, r)
        elif m.camada == "gold":
            _regra_consome_silver("gold", "R5 gold", m, rel, refs, r)

    _regra_schema(pastas_vistas, r)
    return r


def _bloco(titulo: str, itens: list[str], limite: int) -> None:
    print(f"\n── {titulo} · {len(itens)} ──")
    for i in itens[:limite]:
        print(f"  {i}")
    if len(itens) > limite:
        print(f"  … e mais {len(itens) - limite}")


def imprimir(r: Relatorio, escopo: list[Path], modo: str) -> None:
    total_v = sum(len(v) for v in r.violacoes.values())
    total_a = sum(len(v) for v in r.avisos.values())
    print(f"═══ Escopo: {len(escopo)} modelo(s) · {modo} ═══")
    print(f"violações: {total_v} · avisos: {total_a}")
    for regra in sorted(r.violacoes):
        _bloco(regra, r.violacoes[regra], 40)
    for regra in sorted(r.avisos):
        _bloco(f"aviso · {regra}", r.avisos[regra], 20)
    if r.sugestoes:
        _bloco("destino sugerido (mecânico; a camada é de quem migra)", r.sugestoes, 60)
    if total_v == 0:
        print("\nNenhuma violação no escopo.")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("caminhos", nargs="*", help="arquivos .sql ou pastas; vazio = diff")
    ap.add_argument("--tudo", action="store_true", help="verifica o projeto inteiro")
    ap.add_argument("--json", action="store_true", help="saída em JSON")
    args = ap.parse_args(argv)

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    indice = indexar()
    if args.tudo:
        escopo, modo = sorted(m.caminho for m in indice.values()), "projeto inteiro"
    elif args.caminhos:
        escopo, modo = escopo_caminhos(args.caminhos), "caminhos informados"
    else:
        escopo, modo = escopo_diff(), "mudados em relação à main local"

    r = verificar(escopo, indice, args.tudo)
    if args.json:
        print(
            json.dumps(
                {
                    "escopo": [str(p.relative_to(RAIZ)) for p in escopo],
                    "violacoes": dict(r.violacoes),
                    "avisos": dict(r.avisos),
                    "sugestoes": r.sugestoes,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        imprimir(r, escopo, modo)
    return 1 if r.violacoes else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
