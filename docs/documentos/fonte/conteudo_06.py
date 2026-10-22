"""Documento 06 — Exemplos de Uso (Meta 02 · Produto 5 e Meta 03 · Produto 3)."""

DOC = {
    "slug": "06-exemplos-de-uso",
    "titulo": "Exemplos de Uso",
    "subtitulo": (
        "Como consultar a Plataforma de Dados MinC: onde os dados ficam, que "
        "perguntas cada tabela responde, consultas prontas e as ressalvas que "
        "precisam acompanhar cada número."
    ),
    "rodape": "Exemplos de Uso",
    "meta": [
        ("Meta 02 · Produto 5", "Documentação dos pipelines e exemplos de uso."),
        ("Meta 03 · Produto 3", "Scripts e procedimentos de implantação."),
    ],
    "capitulos": [
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "01",
            "eyebrow": "Sobre este documento",
            "titulo": "Onde os dados ficam",
            "icone": "database",
            "paginas": [
                [
                    (
                        "lead",
                        "Este documento é para quem vai consultar a plataforma. Ele mostra em "
                        "que schema cada conjunto de dados vive, que pergunta cada tabela "
                        "responde e como escrever a consulta certa, incluindo as ressalvas "
                        "que precisam acompanhar o resultado.",
                    ),
                    ("h3", "Os schemas"),
                    (
                        "table",
                        ["Schema", "O que contém", "Consultar?"],
                        [
                            [
                                "<code>minc_cotas</code>",
                                "Tabelas de gold da Meta 3: cotas, cobertura e pagamentos "
                                "elegíveis.",
                                "Sim",
                            ],
                            [
                                "<code>cultura_em_numeros</code>",
                                "Tabelas de gold da Meta 5: eventos de fomento e primeiro "
                                "acesso dos agentes.",
                                "Sim",
                            ],
                            [
                                "<code>metadata</code>",
                                "Catálogo de metadados dos modelos, com carimbo de atualização.",
                                "Sim",
                            ],
                            [
                                "<code>transferegov</code>, <code>bbagil</code>, "
                                "<code>relatorio_gestao</code>, <code>bronze</code>",
                                "Camadas de pouso do dado bruto.",
                                "Só para depuração",
                            ],
                        ],
                        "As camadas de pouso mudam de estrutura conforme a origem muda. "
                        "Consultas de negócio devem ler sempre as tabelas de gold.",
                        ["26%", "52%", "22%"],
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Primeira consulta a fazer, sempre",
                        [
                            "Antes de citar qualquer número, confira quando ele foi "
                            "calculado. O catálogo de metadados responde isso para qualquer "
                            "tabela da plataforma.",
                        ],
                    ),
                    (
                        "code",
                        "select table_name, materialization, dt_transform\n"
                        "from metadata.models_metadata\n"
                        "where schema_name = 'minc_cotas'\n"
                        "order by dt_transform desc;",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "02",
            "eyebrow": "Meta 3",
            "titulo": "Consultas de cotas",
            "icone": "chart-pie",
            "paginas": [
                [
                    ("h3", "1. Ler a cobertura antes das cotas"),
                    (
                        "p",
                        "<code>cobertura_pagamentos</code> informa que fração dos pagamentos "
                        "tem perfil demográfico identificado. Esse percentual é o teto de "
                        "confiabilidade de qualquer veredito de cumprimento de cota: quanto "
                        "menor, menos representativo o resultado.",
                    ),
                    (
                        "code",
                        "select ano_final,\n"
                        "       qtd_pagamentos,\n"
                        "       qtd_pessoas,\n"
                        "       cobertura_pessoas_pct,\n"
                        "       cobertura_valor_pct,\n"
                        "       cobertura_temporal_pct\n"
                        "from minc_cotas.cobertura_pagamentos\n"
                        "order by ano_final;",
                        "<code>cobertura_temporal_pct</code> mede outra coisa: a fração do "
                        "valor cujo ano de edital a plataforma conseguiu derivar.",
                    ),
                    ("h3", "2. Cumprimento das cotas por ano"),
                    (
                        "p",
                        "As duas tabelas de distribuição têm a mesma estrutura, produzida pela "
                        "mesma macro. Cada linha é um par de ano e grupo de cota, com o "
                        "percentual medido de duas formas e o veredito de cumprimento.",
                    ),
                    (
                        "code",
                        "select ano_final,\n"
                        "       grupo,\n"
                        "       meta_minima_pct,\n"
                        "       pct_sobre_total,\n"
                        "       pct_sobre_com_perfil,\n"
                        "       status_sobre_com_perfil\n"
                        "from minc_cotas.distribuicao_cotas_lpg\n"
                        "where ano_final = '2024'\n"
                        "order by grupo;",
                    ),
                ],
                [
                    (
                        "callout",
                        "shield-check",
                        "Qual dos dois percentuais usar",
                        [
                            "<code>pct_sobre_total</code> divide pelo valor pago inteiro, "
                            "incluindo pagamentos sem perfil demográfico casado. Como um "
                            "agente sem perfil nunca entra no numerador de nenhuma cota, esse "
                            "percentual <strong>subestima sistematicamente</strong> o "
                            "cumprimento.",
                            "<code>pct_sobre_com_perfil</code> divide pelo valor pago a "
                            "agentes com perfil identificado. É o denominador honesto, e é "
                            "sobre ele que <code>status_sobre_com_perfil</code> é calculado.",
                        ],
                    ),
                    ("h3", "3. Comparar os dois programas"),
                    (
                        "code",
                        "select programa, ano_final, grupo,\n"
                        "       pct_sobre_com_perfil, meta_minima_pct\n"
                        "from minc_cotas.distribuicao_cotas_lpg\n"
                        "union all\n"
                        "select programa, ano_final, grupo,\n"
                        "       pct_sobre_com_perfil, meta_minima_pct\n"
                        "from minc_cotas.distribuicao_cotas_pnab\n"
                        "order by ano_final, grupo, programa;",
                        "O grupo <code>territorio_vulneravel</code> aparece só na LPG. O "
                        "lado-valor do PNAB não traz localização do agente, então a cota "
                        "territorial não é calculável para esse programa.",
                    ),
                    ("h3", "4. Descer ao pagamento individual"),
                    (
                        "code",
                        "select ano_final, programa_fomento, origem_registro,\n"
                        "       tem_perfil, flag_negra, flag_indigena, flag_pcd,\n"
                        "       valor_pago_num\n"
                        "from minc_cotas.fct_pagamentos_elegiveis\n"
                        "where tem_perfil is false\n"
                        "limit 100;",
                        "Consulta útil para investigar a cobertura: mostra os pagamentos que "
                        "não casaram com nenhum perfil demográfico.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "03",
            "eyebrow": "Meta 5",
            "titulo": "Consultas de agentes",
            "icone": "check-badge",
            "paginas": [
                [
                    ("h3", "1. Indicador de primeiro acesso"),
                    (
                        "p",
                        "<code>eixo2_meta5_primeiro_acesso_anual</code> responde à pergunta "
                        "central da Meta 5: que fração dos agentes atendidos por cada mecanismo, "
                        "em cada ano, nunca havia recebido recurso de fomento federal antes. A "
                        "linha <code>'TODOS'</code> conta cada agente uma vez só e não é a soma "
                        "dos mecanismos.",
                    ),
                    (
                        "code",
                        "select ano,\n"
                        "       programa_fomento,\n"
                        "       agentes_atendidos,\n"
                        "       novos_entrantes,\n"
                        "       percentual_primeiro_acesso,\n"
                        "       ano_censurado,\n"
                        "       ano_parcial\n"
                        "from cultura_em_numeros.eixo2_meta5_primeiro_acesso_anual\n"
                        "order by programa_fomento, ano;",
                    ),
                    ("h3", "2. A porta de entrada de cada agente"),
                    (
                        "p",
                        "<code>eixo2_meta5_primeiro_acesso_agente</code> tem uma linha por agente "
                        "e mecanismo. Exatamente uma linha por agente é a porta de entrada, o "
                        "mecanismo pelo qual ele chegou primeiro ao fomento federal.",
                    ),
                    (
                        "code",
                        "select programa_fomento,\n"
                        "       count(*) filter (where eh_porta_de_entrada) as novos_entrantes,\n"
                        "       count(*)                                    as agentes\n"
                        "from cultura_em_numeros.eixo2_meta5_primeiro_acesso_agente\n"
                        "group by programa_fomento\n"
                        "order by novos_entrantes desc;",
                    ),
                ],
                [
                    (
                        "callout",
                        "shield-check",
                        "Os percentuais são teto",
                        [
                            "\"Novo entrante\" quer dizer sem evento anterior nas fontes "
                            "disponíveis, não que o agente nunca acessou. Fonte que falta só "
                            "transforma veterano em falso novo entrante. Não cite um ano marcado "
                            "em <code>ano_censurado</code> ou <code>ano_parcial</code> sem dizer "
                            "que ele está marcado.",
                        ],
                    ),
                    ("h3", "3. Do fomento direto à Rouanet"),
                    (
                        "p",
                        "<code>eixo2_meta5_trajetoria_fomento_direto_rouanet</code> mostra quem "
                        "entrou pela LAB, LPG ou PNAB e depois captou pela Lei Rouanet, e quem "
                        "fez o caminho inverso.",
                    ),
                    (
                        "code",
                        "select trajetoria,\n"
                        "       tipo_pessoa,\n"
                        "       agentes\n"
                        "from cultura_em_numeros.eixo2_meta5_trajetoria_fomento_direto_rouanet\n"
                        "order by agentes desc;",
                    ),
                    ("h3", "4. Descer ao evento"),
                    (
                        "p",
                        "<code>eixo2_meta5_evento_fomento</code> é a espinha das demais: uma "
                        "linha por vez que um agente recebeu recurso, já com o mecanismo "
                        "classificado. É onde se confere de onde veio um número.",
                    ),
                    (
                        "code",
                        "select programa_fomento,\n"
                        "       ano_evento,\n"
                        "       count(distinct beneficiario_documento) as agentes,\n"
                        "       sum(valor)                             as valor\n"
                        "from cultura_em_numeros.eixo2_meta5_evento_fomento\n"
                        "group by 1, 2\n"
                        "order by 1, 2;",
                        "O valor do BB Ágil é o pago ao agente; o da Rouanet e o da Ancine são "
                        "captação. Não some mecanismos de naturezas diferentes sem dizer.",
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "04",
            "eyebrow": "Camada semântica",
            "titulo": "Métricas definidas uma vez",
            "icone": "governance",
            "paginas": [
                [
                    (
                        "p",
                        "Além das tabelas, o projeto declara uma camada semântica: métricas "
                        "com nome, rótulo e definição versionados. Elas existem para que o "
                        "mesmo indicador tenha uma única definição, em vez de ser reescrito "
                        "em cada painel.",
                    ),
                    (
                        "table",
                        ["Métrica", "O que mede"],
                        [
                            ["<code>valor_total_pago</code>", "Soma de todo o valor pago. Denominador bruto das cotas."],
                            ["<code>valor_pago_com_perfil</code>", "Valor pago apenas a agentes com perfil identificado. Denominador honesto."],
                            ["<code>agentes_contemplados</code>", "Agentes distintos que receberam ao menos um pagamento."],
                            ["<code>valor_pago_agentes_negros</code>", "Valor pago a agentes de raça ou cor negra, preta ou parda."],
                            ["<code>valor_pago_agentes_indigenas</code>", "Valor pago a agentes indígenas."],
                            ["<code>valor_pago_agentes_pcd</code>", "Valor pago a agentes com deficiência."],
                            ["<code>valor_pago_territorio_vulneravel</code>", "Valor pago a agentes de território vulnerabilizado. Só LPG."],
                            ["<code>participacao_agentes_negros</code>", "Percentual do valor com perfil destinado a agentes negros. Meta 25%."],
                            ["<code>participacao_agentes_indigenas</code>", "Percentual destinado a agentes indígenas. Meta 10%."],
                            ["<code>participacao_agentes_pcd</code>", "Percentual destinado a agentes com deficiência. Meta 5%."],
                            ["<code>participacao_territorio_vulneravel</code>", "Percentual destinado a territórios vulnerabilizados. Meta 20%, só LPG."],
                            ["<code>cobertura_perfil_valor</code>", "Fração do valor pago cujos agentes têm perfil identificado."],
                        ],
                        None,
                        ["38%", "62%"],
                    ),
                    (
                        "callout",
                        "check-badge",
                        "A métrica que se lê primeiro",
                        [
                            "<code>cobertura_perfil_valor</code> é descrita no próprio projeto "
                            "como o teto de confiabilidade das cotas: quanto menor, menos "
                            "representativo é qualquer veredito de cumprimento. A definição "
                            "registra que ela deve ser lida <strong>antes</strong> das quatro "
                            "métricas de participação.",
                        ],
                    ),
                ],
            ],
        },
        # ─────────────────────────────────────────────────────────────────
        {
            "num": "05",
            "eyebrow": "Fechamento",
            "titulo": "Ressalvas de leitura",
            "icone": "shield-check",
            "paginas": [
                [
                    (
                        "p",
                        "Três ressalvas precisam acompanhar qualquer número citado a partir "
                        "desta plataforma. Todas estão registradas no código dos modelos "
                        "correspondentes, e são reproduzidas aqui para que quem consulta não "
                        "dependa de ler SQL para conhecê-las.",
                    ),
                    ("h3", "1. O denominador do PNAB mede repasse, não pagamento"),
                    (
                        "p",
                        "Enquanto a extração do BB Ágil estiver bloqueada, o lado-valor do "
                        "PNAB vem das listas de contemplados. Isso significa que o denominador "
                        "mede o valor <strong>recebido</strong> pelos entes federados, na "
                        "ordem de R$ 2,7 bilhões, e não o valor <strong>pago</strong> às "
                        "pessoas, na ordem de R$ 447 milhões. Quando "
                        "<code>fct_pagamentos_bbagil</code> for reativado, a tabela de "
                        "distribuição passa a ler o valor pago real.",
                    ),
                    ("h3", "2. Parte das classificações de acesso é inferida"),
                    (
                        "p",
                        "Nem todo proponente respondeu se já havia acessado fomento antes. "
                        "As omissões recebem classificação inferida por ordem cronológica "
                        "entre programas, sempre marcada em <code>status_origem</code>. "
                        "Agregados que não separam <code>total_campo_preenchido</code> de "
                        "<code>total_inferido</code> misturam declaração e dedução.",
                    ),
                    ("h3", "3. Nem todo pagamento tem ano de edital"),
                    (
                        "p",
                        "O ano é derivado numa cascata de três fontes. Quando nenhuma "
                        "resolve, a linha recebe <code>sem_ano</code>. "
                        "<code>cobertura_temporal_pct</code>, em "
                        "<code>cobertura_pagamentos</code>, informa que fração do valor "
                        "conseguiu ser datada. Séries temporais que ignoram esse percentual "
                        "descrevem apenas a parte datável do conjunto.",
                    ),
                    (
                        "callout",
                        "clipboard-document-check",
                        "Regra prática",
                        [
                            "Toda citação de percentual de cota deve vir acompanhada da "
                            "cobertura de perfil do mesmo recorte. Sem ela, o número informa "
                            "o que foi medido, mas não sobre quanto do universo a medição "
                            "aconteceu.",
                        ],
                    ),
                ],
            ],
        },
    ],
}
