# Diagnóstico de cobertura — contemplados × extrato BB Ágil

Investigação de por que só 46% dos contemplados da LPG (38.084/82.307) e 89% do PNAB (10.736/12.085) aparecem no extrato bancário do BB Ágil.

SQL avulso, para rodar manualmente (psql/DBeaver) contra o banco `minc`. Não é modelo dbt e não altera nada do pipeline.

## Schema

Há dois conjuntos de nomes em circulação, e as queries desta pasta misturam os dois.

O que o **código escreve hoje** (`plugins/schemas_minc.py`):

```
bbagil.extrato_bbagil
bbagil.subtransacao_bbagil
bbagil.controle_extracao_bbagil_extrato
bbagil.controle_extracao_bbagil_subtransacoes
transferegov.plano_acao_minc
transferegov.plano_acao_dado_bancario_minc
```

O que o **dump antigo para o banco do MinC** tinha: `bsc.raw_bbagil_*` e
`transferegov_fundo_a_fundo.raw_planos_acao*`, este último com as colunas curtas
(`agencia`, `conta`, `situacao_conta`). A Q0 já está reescrita nos nomes do código; as
demais ainda usam os antigos. Confira com `\dt bbagil.*` antes de rodar e troque num
lugar só, no CTE de origem.

## Tabelas da Meta 5 depois do ADR 0010 (21/10/2026)

As queries citam o schema `agentes`, que a migração para o ADR 0010 esvaziou: ele
fica no banco até o DROP manual, **parado na última carga**, e depois disso
estas queries quebram. Elas registram a investigação de agosto/setembro como
foi feita e não foram reescritas: trocar a tabela muda o que elas medem. Para
rodar de novo, troque no CTE de origem:

| Antes | Agora | Atenção |
|---|---|---|
| `agentes.bbagil_extrato_filtrado` | `bbagil.silver_pagamento_extrato_bbagil` | a silver só limpa: repasse a ente público, FNC, Banco do Brasil e lançamento interno continuam lá |
| `agentes.bbagil_subtransacoes_filtrado` | `bbagil.silver_pagamento_subtransacao_bbagil` | idem, sem o filtro de ente público |
| `agentes.pagamentos_bbagil_beneficiarios` | `cultura_em_numeros.eixo2_meta5_evento_fomento` com `fonte = 'BB_AGIL'` | é a versão com todos os filtros e a política classificada; tem `data_evento`, não `data_pagamento`, e já aplica o piso de valor |
| `agentes.identificadores_contemplados` | `transferegov.silver_planilha_contemplados` | desabilitada até a cadeia de anexos rodar |
| `agentes.primeiro_pagamento_bancario` | `cultura_em_numeros.eixo2_meta5_primeiro_acesso_agente` | já não existia antes da migração; a substituta compara todos os mecanismos, não só LPG × PNAB |

## Ordem de execução

**A ordem importa.** Cada bloco muda a população sobre a qual os seguintes calculam. Rodar Q4 antes de Q0 produz uma tabela por ente que você vai jogar fora — e, pior, uma acusação falsa contra prefeituras que nunca foram consultadas.

| # | Arquivo | Pergunta | Por que nesta posição |
|---|---|---|---|
| 0 | `00_cobertura_extracao.sql` | Quantos planos de ação foram de fato consultados no BB Ágil? | Separa "nunca olhamos" de "olhamos e não achamos". Pode fechar sozinha boa parte dos 54%. |
| 1 | `01_integridade_manchete.sql` | O "99,63% de novos entrantes" sobrevive? | Se não sobreviver, a prioridade muda de explicar o gap para corrigir o número publicado. |
| 2 | `02_espaco_de_chaves.sql` | O denominador 82.307 está certo? | Define a população. Tudo depois disso conta sobre ela. |
| 3 | `03_escada_de_match.sql` ⭐ | Em que causas exclusivas o gap se parte? | A query central. Só é válida com 0–2 resolvidos. |
| 4 | `04_ponte_plano_acao.sql` | Quais entes listaram contemplados que nunca receberam? | Precisa do desconto da Q0 para ser interpretável. |
| 5 | `05_direcao_reversa.sql` | Quem recebeu sem estar na lista? | Precisa da Q2 (docs de 10 dígitos inflam este número). |
| 6 | `06_perfil_do_gap.sql` | O gap é de pessoas ou de reais? De qual modalidade? | Perguntas substantivas, só sobre população limpa. |

## Os três achados que motivaram esta investigação

1. **"Novo entrante" é quase tautológico.** `primeiro_pagamento_bancario.sql:39-44` roda `MIN(...) OVER (PARTITION BY beneficiario_documento)` sobre um universo com **exatamente dois** programas. Logo `'Não'` significa literalmente "recebeu o outro programa antes", e quem aparece em um só programa é **sempre** `'Sim'`. Os 141 "veteranos" da LPG são as 141 pessoas cujo pagamento PNAB antecede o LPG. O contraste 99,63% × 76,43% reflete a cronologia dos programas, não novidade do agente. → **Q1**

2. **O denominador pode estar incompleto.** `identificadores_contemplados.sql:29` usa `ILIKE '%cpf%cnpj%'`, padrão que **não casa uma coluna chamada só `cnpj`** — mas `cotas_dbt/bronze/stg_contemplados_lpg.sql:7` lê exatamente essa coluna. Somado a `LENGTH >= 11` sem `LPAD` contra um lado bancário que sempre faz `LPAD`. → **Q2**

3. **Contas de ação nunca extraídas.** `_carregar_entes_transferegov` só consulta planos com agência **e** conta não-nulas; o resto nunca entra na tabela de controle. Um contemplado pode não aparecer só porque o município dele nunca foi consultado. → **Q0**

## O achado que saiu da Q0 (03/09/2026)

**A DAG consultava uma conta por plano de ação, e planos da LPG têm duas.**
`_SQL_CONTAS_POR_PLANO` era `SELECT DISTINCT ON (id_plano_acao)`, priorizando a conta
ativa: a segunda conta nunca era consultada e não deixava rastro nenhum na tabela de
controle — nem como erro. O plano aparecia aqui como `5_extraido`, e a Q0, que contava
planos, não tinha como ver o buraco.

Corrigido em `extracao_bbagil_dag.py` (grão passou a ser plano × conta × período, com
a conta nas quatro chaves) e nesta pasta (a Q0 passou a classificar por conta, com
`ordem_conta` separando a conta secundária). **Os números que esta Q0 deu antes de
03/09/2026 estão inflados para a LPG**: o `left join banco` no grão de plano duplicava
todo plano de duas contas.

## Invariantes de verificação

Cada arquivo termina com o seu. O mais importante é o da **Q3**: a soma dos motivos de ausência tem que dar exatamente

- LPG: `82.307 − 38.084 = 44.223`
- PNAB: `12.085 − 10.736 = 1.349`

Se não fechar, o `CASE` não é uma partição — algum motivo está se sobrepondo, e nenhum percentual dali é interpretável. Pare e conserte antes de seguir.

## Convenções

- Resultados agregados vão para `data/transferegov/`, seguindo os CSVs que já estão lá.
- Cada arquivo tem um `SELECT` ativo e os demais comentados. Descomente um por vez.
- `count(distinct ...)` sempre que houver ponte por anexo — o fan-out (um documento em vários anexos) supercontaria pessoas com `count(*)`.
