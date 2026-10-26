"""Planejamento SICONFI a partir de linhas reais do extrato de entregas.

As linhas abaixo vieram de ``/extrato_entregas`` (São Paulo, 2025). O extrato
usa o nome por extenso do demonstrativo; procurar só a sigla fazia RREO e RGF
nunca entrarem na fila.
"""

import pytest

from cliente_siconfi import (
    PlanScope,
    classify_delivery,
    planned_units,
    poder_da_instituicao,
    work_units,
)

_BASE = {"exercicio": 2025, "cod_ibge": 3550308, "periodo": 1}


@pytest.mark.parametrize(
    ("entregavel", "expected"),
    [
        ("Relatório Resumido de Execução Orçamentária", "rreo"),
        ("Relatório de Gestão Fiscal", "rgf"),
        ("Balanço Anual (DCA)", "dca"),
        ("MSC Agregada", "msc"),
        ("MSC Encerramento", "msc"),
        ("RREO", "rreo"),
        ("Algo novo", None),
    ],
)
def test_classifica_nome_por_extenso(entregavel: str, expected: str | None) -> None:
    assert classify_delivery(entregavel) == expected


def test_rreo_gera_uma_unidade() -> None:
    item = {
        **_BASE,
        "entregavel": "Relatório Resumido de Execução Orçamentária",
        "periodicidade": "B",
        "tipo_relatorio": "P",
    }
    assert work_units(item) == [
        (
            "rreo",
            {
                "id_ente": 3550308,
                "an_exercicio": 2025,
                "nr_periodo": 1,
                "co_tipo_demonstrativo": "RREO",
            },
            "||",
        )
    ]


_RGF = {
    **_BASE,
    "entregavel": "Relatório de Gestão Fiscal",
    "periodicidade": "Q",
    "tipo_relatorio": "P",
}


@pytest.mark.parametrize(
    ("instituicao", "poder"),
    [
        ("Prefeitura Municipal de São Paulo - SP", "E"),
        ("Governo do Estado do Espírito Santo", "E"),
        ("Governo do Distrito Federal", "E"),
        ("Câmara de Vereadores de São Paulo - SP", "L"),
        ("Câmara Legislativa do Distrito Federal", "L"),
        ("Assembleia Legislativa do Estado do Espírito Santo", "L"),
        # Conferido na API: o RGF do TC sai em co_poder=L, não em J.
        ("Tribunal de Contas do Município de São Paulo", "L"),
        ("Tribunal de Justiça do Estado do Espírito Santo", "J"),
        ("Ministério Público do Estado do Espírito Santo", "M"),
        ("Defensoria Pública do Estado do Espírito Santo", "D"),
        ("Consórcio Intermunicipal", None),
    ],
)
def test_poder_da_instituicao(instituicao: str, poder: str | None) -> None:
    assert poder_da_instituicao(instituicao) == poder


def test_rgf_consulta_so_o_poder_de_quem_entregou() -> None:
    item = {**_RGF, "instituicao": "Câmara de Vereadores de São Paulo - SP"}
    units = work_units(item)
    assert [u[1]["co_poder"] for u in units] == ["L"]
    assert units[0][0] == "rgf" and units[0][1]["in_periodicidade"] == "Q"


def test_rgf_de_instituicao_desconhecida_consulta_os_cinco_poderes() -> None:
    units = work_units({**_RGF, "instituicao": "Consórcio Intermunicipal"})
    assert [u[1]["co_poder"] for u in units] == ["E", "L", "J", "M", "D"]


def test_dca_nao_sai_do_extrato() -> None:
    # A DCA é planejada direto por ente e exercício (PlanScope.dca_units).
    item = {**_BASE, "entregavel": "Balanço Anual (DCA)", "periodicidade": "A"}
    assert work_units(item) == []


def test_msc_agregada_usa_mes_do_extrato() -> None:
    item = {**_BASE, "entregavel": "MSC Agregada", "periodicidade": "M", "periodo": 4}
    units = work_units(item)
    assert len(units) == 24
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCC"}
    assert {u[1]["me_referencia"] for u in units} == {4}


def test_msc_encerramento_consulta_dezembro() -> None:
    # O extrato traz periodo=1, mas a API só devolve a MSCE com me_referencia=12.
    item = {**_BASE, "entregavel": "MSC Encerramento", "periodicidade": "A"}
    units = work_units(item)
    assert {u[1]["co_tipo_matriz"] for u in units} == {"MSCE"}
    assert {u[1]["me_referencia"] for u in units} == {12}


def test_linha_sem_ente_e_ignorada() -> None:
    assert work_units({"entregavel": "Relatório de Gestão Fiscal"}) == []


# --- Recorte configurável ---------------------------------------------------

_ANOS = {"start_year": 2019, "end_year": 2025}
_MSC_DEZ = {
    "id_ente": 32,
    "an_referencia": 2024,
    "me_referencia": 12,
    "co_tipo_matriz": "MSCC",
    "classe_conta": 6,
    "id_tv": "ending_balance",
}


def test_recorte_padrao_so_filtra_os_anos() -> None:
    scope = PlanScope.from_config(_ANOS)
    assert scope.allows("msc_patrimonial", {**_MSC_DEZ, "classe_conta": 1})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "an_referencia": 2026})
    # Sem intervalo próprio, a DCA fica desligada.
    assert not scope.enabled("dca")
    assert not scope.allows("dca", {"id_ente": 32, "an_exercicio": 2024})


def test_recorte_do_outro_repositorio() -> None:
    scope = PlanScope.from_config(
        {
            **_ANOS,
            "fact_endpoints": ["msc_orcamentaria", "dca"],
            "dca_start_year": 2014,
            "dca_end_year": 2025,
            "msc_months": [12],
            "msc_classes": [6],
            "msc_value_types": ["ending_balance"],
            "msc_matrix_types": ["mscc"],
        }
    )
    assert scope.allows("msc_orcamentaria", _MSC_DEZ)
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "me_referencia": 11})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "classe_conta": 5})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "id_tv": "period_change"})
    assert not scope.allows("msc_orcamentaria", {**_MSC_DEZ, "co_tipo_matriz": "MSCE"})
    assert not scope.allows("msc_patrimonial", {**_MSC_DEZ, "classe_conta": 1})
    assert scope.allows("dca", {"id_ente": 32, "an_exercicio": 2024})
    assert not scope.allows("dca", {"id_ente": 32, "an_exercicio": 2013})
    assert not scope.enabled("rreo")
    # O extrato não é demonstrativo de fatos: nunca fica de fora.
    assert scope.enabled("extrato_entregas")


def test_uma_linha_de_msc_do_extrato_vira_uma_unidade_no_recorte_estreito() -> None:
    scope = PlanScope.from_config(
        {
            **_ANOS,
            "fact_endpoints": ["msc_orcamentaria"],
            "msc_months": [12],
            "msc_classes": [6],
            "msc_value_types": ["ending_balance"],
            "msc_matrix_types": ["MSCC"],
        }
    )
    item = {
        "exercicio": 2024,
        "cod_ibge": 32,
        "entregavel": "MSC Agregada",
        "periodicidade": "M",
    }
    kept = [
        u
        for periodo in range(1, 13)
        for u in work_units({**item, "periodo": periodo})
        if scope.allows(u[0], u[1])
    ]
    assert [(u[0], u[1]) for u in kept] == [("msc_orcamentaria", _MSC_DEZ)]


def test_constraints_vao_para_o_claim() -> None:
    scope = PlanScope.from_config({**_ANOS, "rgf_poderes": ["e", "L"]})
    assert scope.constraints("rgf") == {
        "an_exercicio": frozenset(range(2019, 2026)),
        "co_poder": frozenset({"E", "L"}),
    }
    assert set(scope.constraints("extrato_entregas")) == {"an_referencia"}


def test_fingerprint_muda_com_o_recorte() -> None:
    base = PlanScope.from_config(_ANOS)
    assert base.fingerprint() == PlanScope.from_config(dict(_ANOS)).fingerprint()
    narrowed = PlanScope.from_config({**_ANOS, "msc_months": [12]})
    assert base.fingerprint() != narrowed.fingerprint()


_DCA = {"dca_start_year": 2014, "dca_end_year": 2025}


def test_dca_tem_intervalo_proprio_e_nao_depende_do_recorte_das_msc() -> None:
    scope = PlanScope.from_config({"start_year": 2026, "end_year": 2026, **_DCA})
    assert scope.enabled("dca")
    assert scope.constraints("dca") == {"an_exercicio": frozenset(range(2014, 2026))}
    # As demais continuam no recorte global.
    assert scope.constraints("msc_orcamentaria")["an_referencia"] == frozenset({2026})


def test_dca_planejada_direto_por_ente_e_exercicio() -> None:
    scope = PlanScope.from_config({**_ANOS, **_DCA})
    units = list(scope.dca_units([3550308, 3106200]))
    assert len(units) == 2 * 12
    assert ({"id_ente": 3550308, "an_exercicio": 2014}, None) in units
    assert ({"id_ente": 3106200, "an_exercicio": 2025}, None) in units
    # Sem marcador de revisão: a DCA entregue não volta para a fila.
    assert {revision for _, revision in units} == {None}
    # Todas as unidades planejadas passam pelo mesmo filtro do claim.
    assert all(scope.allows("dca", params) for params, _ in units)


def test_dca_desligada_nao_planeja_nada() -> None:
    assert list(PlanScope.from_config(_ANOS).dca_units([1, 2])) == []
    fora = PlanScope.from_config({**_ANOS, **_DCA, "fact_endpoints": ["rreo"]})
    assert not fora.enabled("dca")
    assert list(fora.dca_units([1, 2])) == []


def test_anos_da_dca_nao_forcam_reler_o_extrato() -> None:
    sem = PlanScope.from_config(_ANOS)
    com = PlanScope.from_config({**_ANOS, **_DCA})
    assert sem.fingerprint() == com.fingerprint()


@pytest.mark.parametrize(
    "config",
    [
        {"dca_start_year": 2014},
        {"dca_end_year": 2025},
        {"dca_start_year": 2025, "dca_end_year": 2014},
    ],
)
def test_intervalo_da_dca_invalido_falha_cedo(config: dict) -> None:
    with pytest.raises(ValueError, match="siconfi_config"):
        PlanScope.from_config({**_ANOS, **config})


# --- RREO: bimestre e anexo (RCL = Anexo 03 do bimestre 6) -----------------

_RCL = {"rreo_periods": [6], "rreo_anexos": ["RREO-Anexo 03"]}


def _linha_rreo(periodo: int, tipo: str = "P") -> dict:
    return {
        "exercicio": 2024,
        "cod_ibge": 3550308,
        "entregavel": "Relatório Resumido de Execução Orçamentária",
        "periodicidade": "B",
        "periodo": periodo,
        "tipo_relatorio": tipo,
    }


def test_rreo_sem_filtro_continua_pedindo_o_relatorio_inteiro() -> None:
    scope = PlanScope.from_config(_ANOS)
    units = list(planned_units({**_linha_rreo(3), "exercicio": 2024}, scope))
    assert len(units) == 1
    assert "no_anexo" not in units[0][1]


def test_rcl_so_o_bimestre_6_e_so_o_anexo_03() -> None:
    scope = PlanScope.from_config({**_ANOS, **_RCL})
    kept = [u for p in range(1, 7) for u in planned_units(_linha_rreo(p), scope)]
    assert [(u[0], u[1]) for u in kept] == [
        (
            "rreo",
            {
                "id_ente": 3550308,
                "an_exercicio": 2024,
                "nr_periodo": 6,
                "co_tipo_demonstrativo": "RREO",
                "no_anexo": "RREO-Anexo 03",
            },
        )
    ]


def test_rcl_do_municipio_pequeno_usa_o_rreo_simplificado() -> None:
    scope = PlanScope.from_config({**_ANOS, **_RCL})
    (unit,) = planned_units(_linha_rreo(6, tipo="S"), scope)
    assert unit[1]["co_tipo_demonstrativo"] == "RREO Simplificado"
    assert unit[1]["no_anexo"] == "RREO-Anexo 03"


def test_varios_anexos_viram_uma_unidade_cada() -> None:
    scope = PlanScope.from_config(
        {**_ANOS, "rreo_anexos": ["RREO-Anexo 03", "RREO-Anexo 02"]}
    )
    units = list(planned_units(_linha_rreo(6), scope))
    assert [u[1]["no_anexo"] for u in units] == ["RREO-Anexo 02", "RREO-Anexo 03"]


def test_filtro_do_rreo_nao_afeta_outros_endpoints() -> None:
    scope = PlanScope.from_config({**_ANOS, **_RCL})
    rgf = {
        **_BASE,
        "entregavel": "Relatório de Gestão Fiscal",
        "periodicidade": "Q",
        "instituicao": "Prefeitura Municipal de São Paulo - SP",
        "periodo": 1,
        "exercicio": 2024,
    }
    assert len(list(planned_units(rgf, scope))) == 1
    assert scope.expand("rgf", {"id_ente": 1}) == [{"id_ente": 1}]


def test_constraints_do_rreo_vao_para_o_claim() -> None:
    scope = PlanScope.from_config({**_ANOS, **_RCL})
    assert scope.constraints("rreo") == {
        "an_exercicio": frozenset(range(2019, 2026)),
        "nr_periodo": frozenset({6}),
        "no_anexo": frozenset({"RREO-Anexo 03"}),
    }
    # Unidades antigas, pedidas sem no_anexo, deixam de ser reservadas.
    assert not scope.allows(
        "rreo",
        {"id_ente": 1, "an_exercicio": 2024, "nr_periodo": 6},
    )


def test_filtro_do_rreo_muda_o_fingerprint_para_reler_o_extrato() -> None:
    base = PlanScope.from_config(_ANOS)
    assert base.fingerprint() != PlanScope.from_config({**_ANOS, **_RCL}).fingerprint()


@pytest.mark.parametrize(
    "config",
    [
        {"rreo_periods": [7]},
        {"rreo_periods": [0]},
        {"rreo_anexos": ["RREO-Anexo 99"]},
        {"rreo_anexos": ["rreo-anexo 03"]},
    ],
)
def test_filtro_invalido_do_rreo_falha_cedo(config: dict) -> None:
    with pytest.raises(ValueError, match="siconfi_config"):
        PlanScope.from_config({**_ANOS, **config})


@pytest.mark.parametrize(
    "config",
    [
        {"fact_endpoints": ["msc_orcamentario"]},
        {"rgf_poderes": ["X"]},
        {"msc_value_types": ["saldo_final"]},
        {"msc_matrix_types": ["MSCX"]},
    ],
)
def test_recorte_invalido_falha_cedo(config: dict) -> None:
    with pytest.raises(ValueError, match="siconfi_config"):
        PlanScope.from_config({**_ANOS, **config})


# --- Limitador em memória --------------------------------------------------


def test_limitador_espaca_as_requisicoes_sem_banco(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """1 req/s na API inteira: a DAG roda uma task por vez, então basta o
    processo espaçar as próprias chamadas -- sem Postgres (ADR 0005)."""
    import cliente_siconfi

    relogio = iter([100.0, 100.2, 101.05, 103.0])
    esperas: list[float] = []
    monkeypatch.setattr(cliente_siconfi.time, "monotonic", lambda: next(relogio))
    monkeypatch.setattr(cliente_siconfi.time, "sleep", esperas.append)

    limitador = cliente_siconfi.SiconfiRateLimiter(interval_s=1.0)
    for _ in range(4):
        limitador.reserve()

    # 1ª imediata; 2ª chega 0,2 s depois e espera 0,8 s; 3ª chega 1,05 s depois
    # da 1ª, mas a 2ª saiu às 101,0, então espera 0,95 s; 4ª já passou do prazo.
    assert esperas == pytest.approx([0.8, 0.95])
