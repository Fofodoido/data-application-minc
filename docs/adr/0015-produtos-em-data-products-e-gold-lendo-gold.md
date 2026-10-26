# ADR 0015 — Produtos em `data_products/`, Intermediate nomeada pelas fontes, Gold lendo Gold do mesmo produto e o que é regra de negócio

- **Data:** 2026-10-26
- **Status:** proposto
- **Escopo:** pastas, nomes e regras transversais do projeto dbt
- **Substitui em parte:** [ADR 0010](0010-camadas-e-schemas-do-data-lakehouse.md),
  nas seções "Schemas e nomenclatura" (linhas da Intermediate e da Gold),
  "Pastas no projeto dbt" e "Regras transversais" (regras 3 e 5). O resto do
  0010 continua valendo.
- **Origem:** PR #87 (migração da Meta 5) e a revisão dele

## Contexto

O PR #87 migrou a Meta 5 para a estrutura do ADR 0010 e, no caminho, mudou
três regras. Ele editou o texto do 0010 já aceito, no mesmo commit que mudou o
`verificar.py` e os modelos que o `verificar.py` confere. O `GUIA.md` proíbe
isso. Este ADR registra as três mudanças como decisão própria, e o 0010 volta
ao texto de 07/10/2026.

Havia também uma lacuna: o 0010 manda a regra de negócio para a Gold
(regra 3), mas não diz o que é regra de negócio. A definição foi escrita só na
skill `arquitetura-lakehouse-minc`, e lá o estorno aparecia como limpeza da
Silver, enquanto o código do mesmo PR o pareava na Gold.

## Decisão

### 1. Produtos de dados ficam em `data_products/`

```text
dbt/minc/models/
├── <fonte>/                  bronze_* e silver_*
├── intermediate/             int_*
└── data_products/
    └── <produto>/            Gold do produto, na convenção do produto
```

`data_products/` é só organização no repositório e não vira schema. Cada
produto declara o próprio schema:

```yaml
# dbt_project.yml
data_products:
  cultura_em_numeros:
    +schema: cultura_em_numeros
```

Toda subpasta de `data_products/` é um produto. Não há lista de produtos para
manter.

### 2. A Intermediate leva no nome só as fontes que cruza, em ordem alfabética

`int_<fonte1>_..._<fonteN>`, por exemplo `int_ancine_bbagil_salic`. É uma
intermediate por combinação de fontes. Um conceito no nome
(`int_<fontes>_eventos`) é sinal de que o modelo aplica regra de produto e
pertence à Gold.

### 3. O que é regra de negócio, e o que é limpeza

**Regra de negócio** é tudo que decide **quem conta** para um indicador.
Fica na Gold:

- escopo de mecanismo (quais mecanismos da Ancine entram);
- classificação em política (LPG, PNAB, LAB);
- exclusão de beneficiário (ente público, FNC, Banco do Brasil);
- piso de valor (`valor > 0`);
- o que é pagamento efetivo (só débito, status `Pago`);
- **pareamento de estorno**;
- definição de novo entrante e de coorte.

**Limpeza** é o que qualquer produto faria igual. Fica na Silver:

- tipar e aparar texto;
- transformar o valor que não é data, número ou documento válido em `NULL`;
- deduplicar o registro repetido pela extração;
- descartar lixo de parsing.

**O critério:** se dois produtos poderiam querer respostas diferentes, é
regra de negócio. Na dúvida, é Gold.

Para a regra de negócio funcionar na Gold, a Silver e a Intermediate entregam
as linhas e as colunas de que ela precisa, sem decidir nada. O BB Ágil, por
exemplo, entrega o débito e o crédito com `creditdebitindicator`, a conta e o
documento bruto. Cada Gold decide se pareia o estorno e como.

### 4. A Gold lê Silver, Intermediate ou Gold do mesmo produto

Esta regra substitui a regra 5 do 0010:

> **5.** Gold referencia Intermediate, Silver ou Gold **do mesmo produto**.
> Nunca Bronze, nunca `source`, nunca Gold de outro produto.

Quando várias golds de consumo precisam da mesma regra, ela é escrita uma vez
numa gold-base do produto, e as outras leem dela. Na Meta 5,
`eixo2_meta5_evento_fomento` diz quem conta, e as demais golds leem dela.

## Por quê

**`data_products/`.** A pasta distingue produto de fonte pelo caminho. Antes
eram três cópias da lista `PRODUTOS` (no `verificar.py`, no coletor do site e
no anotador do OpenMetadata), e produto novo exigia lembrar das três.

**Fontes em ordem alfabética.** Sem uma ordem fixa, o mesmo cruzamento ganha
dois nomes (`int_salic_ancine` e `int_ancine_salic`) e duas implementações.

**Definição de regra de negócio.** "Só na Gold" sem definição vira "o que cada
um acha". A skill e o código discordavam sobre o estorno no mesmo PR.

**Gold lendo Gold.** A regra de negócio não pode ir para a Intermediate
(regra 3), e repeti-la em cada gold de consumo produz regras divergentes ao
longo do tempo. A gold-base é o único lugar onde ela fica escrita uma vez sem
sair da Gold.

## O que foi descartado

- **Produtos na raiz de `models/` com a lista `PRODUTOS`.** Era o desenho do
  0010. Três cópias da lista, e uma pasta de produto indistinguível de uma
  pasta de fonte.
- **Regra compartilhada na Intermediate.** A Intermediate passaria a carregar
  a escolha de um produto, e deixaria de servir aos outros.
- **Repetir a regra em cada gold de consumo.** O mesmo filtro escrito cinco
  vezes diverge na primeira correção.
- **Gold lendo Gold de outro produto.** Acopla produtos com donos e ritmos
  diferentes. Se dois produtos precisarem da mesma regra, fica em aberto
  (ver "Consequências").
- **Estorno como limpeza da Silver.** Como parear (por conta, por valor, em
  que janela) é escolha do indicador, não da fonte.

## Consequências

- O `verificar.py` já aplica as decisões 1, 2 e 4 desde o commit `78004a6`
  (R1, R5 e R7). A `references/regras.md` da skill passa a citar este ADR na
  R5 e na R8.
- O 0010 volta ao texto aceito em 07/10/2026, com o status "partes
  substituídas pelo ADR 0015".
- As silvers que hoje filtram "quem conta" estão fora do padrão, mesmo que o
  script não acuse: a R8 é manual.
- **Fica em aberto:** uma regra de negócio compartilhada entre dois produtos.
  Hoje só existe um produto (`cultura_em_numeros`).
