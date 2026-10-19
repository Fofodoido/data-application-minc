"""Cliente da API do OpenMetadata ciente dos três modos de execução.

Quatro cuidados, cada um por um problema concreto:

- **paginação**: o `dbanalytics` tem 1.774 tabelas. Uma listagem que para na
  primeira página faria o sync declarar "conforme" o que nem leu;
- **retry** em 5xx e timeout: o proxy na frente da instância corta conexão
  longa, e um PATCH perdido no meio de 1.700 não pode derrubar a execução;
- **nome de exibição em patch separado, sempre**: o bot de ingestão é
  proibido de alterá-lo (`DisplayName-Deny`) e JSON Patch é tudo ou nada — indo
  junto, o 403 levava descrição, dono e domínio com ele;
- **sem `.env` lido aqui**: quem chama passa URL e token. A DAG os tira das
  Variables do Airflow; a linha de comando, de `config.acesso()`.

Três modos:

``offline``    sem URL/token. Não fala com a instância; relata o declarado.
``simulacao``  com credencial, sem ``confirmar``. Lê a instância, compara com
               o declarado e relata a diferença. Não escreve nada.
``confirmar``  escreve.
"""

from __future__ import annotations

import html
import logging
import time
from typing import Any, Iterator
from urllib.parse import quote

import requests

from openmetadata.governanca.config import api_base

log = logging.getLogger(__name__)

TENTATIVAS = 3
PAGINA = 100
#: Escrita espera mais que leitura: o PATCH de uma tabela com dezenas de
#: colunas etiquetadas reindexa a busca antes de responder, e já passou de 90 s
#: com a alteração aplicada.
TEMPO_DE_ESCRITA = 300


class ClienteOM:
    def __init__(
        self,
        url: str | None,
        token: str | None,
        confirmar: bool = False,
        sessao: requests.Session | None = None,
    ) -> None:
        self.offline = not (url and token)
        self.confirmar = confirmar and not self.offline
        self.base = api_base(url) if url else ""
        self.sessao = sessao or requests.Session()
        if token:
            self.sessao.headers.update(
                {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            )
        self.criados = 0
        self.atualizados = 0
        self.conformes = 0
        self.falhas: list[str] = []
        self.planejado: list[str] = []
        self._cache: dict[tuple[str, str, str], dict | None] = {}

    @property
    def modo(self) -> str:
        if self.offline:
            return "offline"
        return "confirmar" if self.confirmar else "simulacao"

    def cabecalho(self) -> None:
        if self.offline:
            log.info(
                "MODO OFFLINE — sem URL/token. A instância não é consultada nem "
                "escrita; o que segue é o que está declarado."
            )
        elif not self.confirmar:
            log.info("MODO SIMULAÇÃO — nada é escrito. Use --confirmar para aplicar.")

    # ── transporte ───────────────────────────────────────────────────────
    def _requisitar(self, metodo: str, rota: str, **kwargs: Any) -> requests.Response:
        """Uma chamada com retry em erro de servidor e de rede.

        4xx não é repetido: é recusa, e repetir só multiplica a recusa.
        """
        kwargs.setdefault("timeout", 90)
        url = f"{self.base}/{rota.lstrip('/')}"
        ultimo: Exception | None = None
        for tentativa in range(1, TENTATIVAS + 1):
            try:
                resposta = self.sessao.request(metodo, url, **kwargs)
            except (requests.ConnectionError, requests.Timeout) as exc:
                ultimo = exc
            else:
                if resposta.status_code < 500:
                    return resposta
                ultimo = RuntimeError(f"HTTP {resposta.status_code}")
            time.sleep(2**tentativa)
        raise RuntimeError(f"{metodo} {rota} falhou {TENTATIVAS} vezes: {ultimo}")

    # ── leitura ──────────────────────────────────────────────────────────
    def existe(self, rota: str, fqn: str, campos: str | None = None) -> dict | None:
        """Busca uma entidade pelo FQN. Em offline, ninguém existe ainda."""
        if self.offline:
            return None
        chave = (rota, fqn, campos or "")
        if chave in self._cache:
            return self._cache[chave]
        resposta = self._requisitar(
            "GET",
            f"{rota}/name/{quote(fqn, safe='')}",
            params={"fields": campos} if campos else None,
        )
        entidade: dict | None = resposta.json() if resposta.ok else None
        self._cache[chave] = entidade
        return entidade

    def esquecer(self, rota: str, fqn: str) -> None:
        """Descarta o cache de uma entidade depois de escrever nela."""
        for chave in [c for c in self._cache if c[0] == rota and c[1] == fqn]:
            del self._cache[chave]

    def listar(self, rota: str, parametros: dict | None = None) -> Iterator[dict]:
        """Todas as páginas, seguindo o cursor `after` da API."""
        if self.offline:
            return
        parametros = dict(parametros or {})
        parametros.setdefault("limit", PAGINA)
        while True:
            resposta = self._requisitar("GET", rota, params=parametros)
            if not resposta.ok:
                self.falha(f"listar {rota}", resposta)
                return
            corpo = resposta.json()
            yield from corpo.get("data", [])
            depois = (corpo.get("paging") or {}).get("after")
            if not depois:
                return
            parametros["after"] = depois

    def obter(self, rota: str, parametros: dict | None = None) -> dict | None:
        if self.offline:
            return None
        resposta = self._requisitar("GET", rota, params=parametros)
        return resposta.json() if resposta.ok else None

    # ── escrita ──────────────────────────────────────────────────────────
    def _registrar(self, mensagem: str) -> None:
        self.planejado.append(mensagem)
        log.info("  [%s] %s", self.modo, mensagem)

    def criar(
        self, rota: str, corpo: dict, rotulo: str, metodo: str = "PUT"
    ) -> dict | None:
        if not self.confirmar:
            self._registrar(f"criaria {rota}: {rotulo}")
            self.criados += 1
            return None
        resposta = self._escrever(metodo, rota, f"criar {rota} {rotulo}", json=corpo)
        if resposta is None:
            return None
        if not resposta.ok:
            self.falha(f"criar {rota} {rotulo}", resposta)
            return None
        self.criados += 1
        criada: dict = resposta.json() if resposta.content else {}
        return criada

    def patch(self, rota: str, ident: str, operacoes: list[dict], rotulo: str) -> bool:
        """Aplica um JSON Patch. Lista vazia significa que já está conforme.

        `displayName` sai sempre num patch próprio, depois do resto: é o único
        campo que o bot pode ter recusado, e a recusa não pode levar junto o
        que ele tem permissão de escrever.
        """
        if not operacoes:
            self.conformes += 1
            return True
        exibicao = [o for o in operacoes if o["path"] == "/displayName"]
        demais = [o for o in operacoes if o["path"] != "/displayName"]
        ok = True
        for lote, sufixo in ((demais, ""), (exibicao, " (nome de exibição)")):
            if lote:
                ok = self._patch(rota, ident, lote, rotulo + sufixo) and ok
        return ok

    def _patch(self, rota: str, ident: str, operacoes: list[dict], rotulo: str) -> bool:
        if not self.confirmar:
            campos = ", ".join(sorted({o["path"].strip("/") for o in operacoes}))
            self._registrar(f"ajustaria {rotulo}: {campos}")
            self.atualizados += 1
            return True
        resposta = self._escrever(
            "PATCH",
            f"{rota}/{ident}",
            f"ajustar {rotulo}",
            headers={"Content-Type": "application/json-patch+json"},
            json=operacoes,
        )
        if resposta is None:
            return False
        if not resposta.ok:
            self.falha(f"ajustar {rotulo}", resposta)
            return False
        self.atualizados += 1
        return True

    def enviar(self, metodo: str, rota: str, corpo: Any, rotulo: str) -> dict | None:
        """Escrita que não é criação nem patch (linhagem, suíte lógica...)."""
        if not self.confirmar:
            self._registrar(rotulo)
            self.atualizados += 1
            return None
        resposta = self._escrever(metodo, rota, rotulo, json=corpo)
        if resposta is None:
            return None
        if not resposta.ok:
            self.falha(rotulo, resposta)
            return None
        self.atualizados += 1
        return resposta.json() if resposta.content else {}

    def _escrever(
        self, metodo: str, rota: str, contexto: str, **kwargs: Any
    ) -> requests.Response | None:
        """Escrita que, esgotadas as tentativas, vira falha no relatório.

        Uma tabela que não responde não pode interromper as outras mil: o sync
        segue, sai com erro no fim, e a próxima execução refaz o que faltou.
        """
        kwargs.setdefault("timeout", TEMPO_DE_ESCRITA)
        try:
            return self._requisitar(metodo, rota, **kwargs)
        except RuntimeError as exc:
            self.falhas.append(f"{contexto}: {exc}")
            log.warning("  FALHA %s: %s", contexto, exc)
            return None

    def falha(self, contexto: str, resposta: requests.Response) -> None:
        mensagem = f"{contexto}: {explicar(resposta)}"
        self.falhas.append(mensagem)
        log.warning("  FALHA %s", mensagem)

    def resumo(self) -> dict[str, int]:
        return {
            "criados": self.criados,
            "atualizados": self.atualizados,
            "conformes": self.conformes,
            "falhas": len(self.falhas),
        }

    def relatar(self) -> int:
        """Imprime o resumo e devolve o código de saída do comando."""
        r = self.resumo()
        print(
            f"\ncriados={r['criados']}  atualizados={r['atualizados']}  "
            f"já conformes={r['conformes']}  falhas={r['falhas']}"
        )
        for mensagem in self.falhas:
            print(f"  - {mensagem}")
        return 1 if self.falhas else 0


def explicar(resposta: requests.Response) -> str:
    """Traduz as recusas que já custaram diagnóstico errado uma vez."""
    corpo = resposta.text
    if resposta.status_code == 403 and "DisplayName-Deny" in corpo:
        return (
            "HTTP 403 — o bot de ingestão é proibido de alterar nome de exibição "
            "(IngestionBotRole / DisplayName-Deny). Só um administrador troca, "
            "pela interface."
        )
    if resposta.status_code == 403:
        return f"HTTP 403 — permissão negada ao bot. {corpo[:160]}"
    if resposta.status_code == 400 and "Data Product Domain Validation" in corpo:
        return (
            "HTTP 400 — o ativo não está no domínio do produto. O domínio é "
            "aplicado antes do produto; confira `dominios.yml`."
        )
    return f"HTTP {resposta.status_code} {corpo[:240]}"


# ── comparação ───────────────────────────────────────────────────────────
def operacoes_de_diferenca(atual: dict, desejado: dict[str, Any]) -> list[dict]:
    """JSON Patch só dos campos que realmente diferem.

    É o que torna o sync idempotente: duas execuções seguidas terminam com
    `atualizados=0`.
    """
    operacoes = []
    for campo, valor in desejado.items():
        if valor is None:
            continue
        if equivalente(atual.get(campo), valor):
            continue
        operacoes.append({"op": "add", "path": f"/{campo}", "value": valor})
    return operacoes


def equivalente(atual: Any, desejado: Any) -> bool:
    """Compara ignorando o que a API acrescenta por conta própria.

    A resposta traz referências completas (id, href, displayName, deleted…);
    comparar o dicionário inteiro faria todo campo parecer diferente e o sync
    nunca convergiria.
    """
    if isinstance(desejado, list) and isinstance(atual, list):
        return chaves(atual) == chaves(desejado)
    if isinstance(desejado, list) and atual is None:
        return not desejado
    if isinstance(desejado, dict) and isinstance(atual, dict):
        return all(equivalente(atual.get(k), v) for k, v in desejado.items())
    if isinstance(desejado, str) and isinstance(atual, str):
        return mesmo_texto(atual, desejado)
    return bool(atual == desejado)


def mesmo_texto(atual: str | None, desejado: str | None) -> bool:
    """Compara texto tratando a escapada HTML que a instância aplica ao gravar.

    `=`, `'` e `"` voltam como `&#61;`, `&#39;` e `&#34;`. Comparando cru, a
    descrição pareceria divergente para sempre e o sync a reescreveria a cada
    execução.
    """
    return html.unescape(atual or "").strip() == html.unescape(desejado or "").strip()


def chaves(itens: list) -> set:
    """Reduz uma lista de referências ao que a identifica."""
    reduzidas = set()
    for item in itens:
        if not isinstance(item, dict):
            reduzidas.add(item)
            continue
        # `relatedTerms` é `TermRelation`: a referência vem embrulhada em `term`,
        # com o tipo ao lado. Sem este caso os dois lados reduziriam a vazio.
        if isinstance(item.get("term"), dict):
            alvo = chaves([item["term"]])
            reduzidas.add(
                f"{item.get('relationType', 'relatedTo')}:{'|'.join(sorted(alvo))}"
            )
            continue
        if "constraintType" in item:
            reduzidas.add(
                (
                    item["constraintType"],
                    tuple(item.get("columns") or ()),
                    tuple(item.get("referredColumns") or ()),
                )
            )
            continue
        for chave in ("tagFQN", "fullyQualifiedName", "id", "name"):
            if item.get(chave):
                reduzidas.add(item[chave])
                break
    return reduzidas


def texto(valor: str | None) -> str:
    """Normaliza descrição de YAML multilinha para uma linha só."""
    return " ".join((valor or "").split())


def referencia(entidade: dict, tipo: str) -> dict:
    """Referência de entidade no formato que a API espera num patch.

    `name` e `fullyQualifiedName` vão junto do `id`: a interface os usa como
    rótulo do link, e sem eles uma lista de referências aparece em branco.
    """
    return {
        "id": entidade["id"],
        "type": tipo,
        "name": entidade["name"],
        "fullyQualifiedName": entidade["fullyQualifiedName"],
    }


def etiqueta(fqn: str, origem: str = "Classification") -> dict:
    """Etiqueta aplicada por automação, não por decisão manual na tela."""
    return {
        "tagFQN": fqn,
        "source": origem,
        "labelType": "Automated",
        "state": "Confirmed",
    }


class Proprietarios:
    """Resolve a chave declarada no YAML para a entidade do OpenMetadata.

    A chave (`minc_data_engineering`) aparece em todos os YAMLs de governança;
    o mapa `proprietarios:` de `dominios.yml` diz se ela é time ou pessoa.
    Nenhum sincronizador decide dono por conta própria.
    """

    def __init__(self, om: ClienteOM, dominios: dict) -> None:
        self.om = om
        self.mapa = dominios.get("proprietarios") or {}
        self.cache: dict[str, list[dict] | None] = {}

    def resolver(self, chave: str | None) -> list[dict] | None:
        if not chave:
            return None
        if chave in self.cache:
            return self.cache[chave]
        declarado = self.mapa.get(chave)
        if not declarado:
            self.om.falhas.append(f"proprietário '{chave}' não está em dominios.yml")
            self.cache[chave] = None
            return None
        tipo = declarado.get("type", "team")
        entidade = self.om.existe(
            "teams" if tipo == "team" else "users", declarado["name"]
        )
        if not entidade:
            if not self.om.offline:
                self.om.falhas.append(
                    f"proprietário '{declarado['name']}' não existe como {tipo} "
                    "na instância"
                )
            self.cache[chave] = None
            return None
        self.cache[chave] = [referencia(entidade, tipo)]
        return self.cache[chave]
