# ADR 0013 — A governança do OpenMetadata é declarada no repositório e aplicada depois dos conectores

- Status: proposto
- Data: 2026-10-18
- Escopo: `dbt/minc/governance/`, `helpers/openmetadata/governanca/`,
  `scripts/governance/`, `dags/openmetadata_ingestion_dag.py`
- Origem: frente F6 de [`docs/openmetadata/MEMORY.md`](../openmetadata/MEMORY.md)

## Contexto

Os conectores do OpenMetadata (Postgres e dbt) catalogam estrutura: schema,
tabela, coluna, descrição e linhagem. O que eles não expressam ficou sem dono.
Medido em 2026-10-18 no `MinC.dbanalytics`, com 1.774 tabelas:

- nenhuma tabela com produto de dados, e nenhum produto de dados do MinC na
  instância;
- 10 tabelas com certificação, e nenhuma com permissão de uso;
- as 589 views da bronze do SALIC sem domínio, dono nem tier;
- os casos de teste do dbt existiam, todos **sem resultado**: a DAG rodava só
  `dbt docs generate`, cujo `run_results.json` não tem teste nenhum. Sem
  resultado não há incidente, e o painel de qualidade fica vazio;
- o glossário corrigido no CSV nunca chegou à instância, que seguia com a
  versão anterior de seis termos;
- nenhuma métrica, contrato ou suíte de teste do MinC.

E o conector dbt **apaga** a certificação das tabelas a cada execução: o
`createOrUpdate` substitui o campo que a requisição não traz.

## Decisão

1. **Duas camadas, não dois writers da mesma coisa.** Os conectores continuam
   a única fonte da estrutura. A governança é uma camada por cima, aplicada
   por PATCH idempotente, campo a campo, e só com o que o conector não
   escreve: produto, permissão de uso, certificação, formato de coluna,
   chaves, propriedades, relações tipadas do glossário, métricas, testes
   nativos, contratos e severidade de incidente.
2. **O repositório é a fonte.** A declaração mora em `dbt/minc/governance/`,
   versionada com o projeto dbt que ela descreve. O que se edita na tela se
   perde no próximo sync — exceto o que não é nosso (PII do classifier, termo
   pendurado à mão, propriedade de outro órgão), que é preservado.
3. **Fail-closed.** O produto de cada ativo sai da pasta dbt e do nome da
   source. Ativo sem produto ou sem camada é erro de teste, não default.
4. **Sempre por último, com `all_done`.** A task de governança fecha a
   corrente da DAG mesmo que uma recipe antes dela falhe, porque é ela que
   devolve a certificação.
5. **Resultado de teste chega ao catálogo.** A DAG roda `dbt test` depois do
   `docs generate`, sem a bronze do SALIC (4.883 testes sobre 157 M de
   linhas). Testes nativos do OpenMetadata conferem formato, tabela vazia e
   coluna que mudou, numa suíte que só contém nativos.
6. **Alerta só dentro do OpenMetadata**: incidente e feed de atividade, sem
   e-mail nem webhook (decisão de 2026-10-18).
7. **Três modos**: sem credencial, offline; com credencial, simulação que só
   lê; escrita só com `--confirmar`.

## Consequências

- Modelo ou source novo exige uma linha em `dominios.yml` (o teste avisa).
- A execução da DAG ganha o `dbt test` (495 testes no recorte padrão) e a
  suíte nativa. Os dois leem dado do banco, o que nunca acontecia antes.
- Formato de coluna só é afirmado onde o código ou a documentação sustentam;
  documento cru do SALIC é "como na origem", e o teste é que mede.
- A instância é compartilhada: classificação, propriedade e nome de produto
  são globais, por isso os prefixos `minc`/`FormatoMinC`/`minc_` e a recusa de
  tomar um produto que já exista em outro domínio.
- Linhagem de coluna continua só do conector dbt; a auditoria mede a cobertura
  em vez de um segundo writer tentar completá-la.
