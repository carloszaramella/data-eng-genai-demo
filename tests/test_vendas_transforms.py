"""Testes unitários para as transformações de vendas."""

import pandas as pd
import pytest

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture
def df_pedidos():
    """Cria um DataFrame de pedidos sintéticos."""
    data = {
        "ID_PEDIDO": [
            "ped-001",
            "ped-002",
            "ped-003",
            "ped-004",
            "ped-005",
            "ped-006",
            "ped-007",
            "ped-008",
            "ped-009",
            "ped-010",
            "ped-011",
            "ped-012",
            "ped-013",
            "ped-014",
        ],
        "PRODUTO": [
            "NOTEBOOK",
            "CELULAR",
            "GELADEIRA",
            "MONITOR",
            "LIQUIDIFICADOR",
            "NOTEBOOK",
            "CELULAR",
            "GELADEIRA",
            "MONITOR",
            "NOTEBOOK",
            "CELULAR",
            "LIQUIDIFICADOR",
            "GELADEIRA",
            "MONITOR",
        ],
        "VALOR_UNITARIO": [
            1500.0,
            1000.0,
            2000.0,
            600.0,
            300.0,
            1500.0,
            1000.0,
            2000.0,
            600.0,
            1500.0,
            1000.0,
            300.0,
            2000.0,
            600.0,
        ],
        "QUANTIDADE": [2, 3, 1, 2, 5, 1, 1, 2, 3, 3, 2, 1, 3, 1],
        "DATA_CRIACAO": [
            "2026-01-01T10:00:00",
            "2026-01-02T11:00:00",
            "2026-01-03T12:00:00",
            "2026-01-04T13:00:00",
            "2026-01-05T14:00:00",
            "2026-01-06T15:00:00",
            "2026-01-07T16:00:00",
            "2026-01-08T17:00:00",
            "2026-01-09T18:00:00",
            "2026-01-10T19:00:00",
            "2026-01-11T20:00:00",
            "2026-01-12T21:00:00",
            "2026-01-13T22:00:00",
            "2026-01-14T23:00:00",
        ],
        "UF": [
            "SP",
            "RJ",
            "MG",
            "SP",
            "RJ",
            "MG",
            "SP",
            "RJ",
            "MG",
            "SP",
            "RJ",
            "MG",
            "SP",
            "RJ",
        ],
        "ID_CLIENTE": [1, 2, 1, 3, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    }
    return pd.DataFrame(data)


@pytest.fixture
def df_clientes():
    """Cria um DataFrame de clientes sintéticos."""
    data = {
        "id": list(range(1, 13)),
        "nome": [
            "Alice Silva",
            "Bruno Costa",
            "Carla Dias",
            "Daniel Souza",
            "Elena Rocha",
            "Felipe Lima",
            "Gabi Santos",
            "Hugo Pereira",
            "Iris Alves",
            "João Martins",
            "Karen Ribeiro",
            "Lucas Ferreira",
        ],
        "email": [
            "alice@email.com",
            "bruno@email.com",
            "carla@email.com",
            "daniel@email.com",
            "elena@email.com",
            "felipe@email.com",
            "gabi@email.com",
            "hugo@email.com",
            "iris@email.com",
            "joao@email.com",
            "karen@email.com",
            "lucas@email.com",
        ],
    }
    return pd.DataFrame(data)


class TestCalcularValorTotalPedido:
    """Testes para o cálculo do valor total do pedido."""

    def test_valor_total_correto(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        assert "VALOR_TOTAL" in resultado.columns

        row = resultado[resultado["ID_PEDIDO"] == "ped-001"].iloc[0]
        assert row["VALOR_TOTAL"] == 3000.0  # 1500 * 2

    def test_valor_total_quantidade_multipla(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        row = resultado[resultado["ID_PEDIDO"] == "ped-002"].iloc[0]
        assert row["VALOR_TOTAL"] == 3000.0  # 1000 * 3

    def test_preserva_colunas_originais(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        for col in df_pedidos.columns:
            assert col in resultado.columns

    def test_nao_modifica_original(self, df_pedidos):
        original_cols = list(df_pedidos.columns)
        VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        assert list(df_pedidos.columns) == original_cols


class TestAgregarPorCliente:
    """Testes para a agregação de compras por cliente."""

    def test_agregacao_soma_correta(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        # Cliente 1: (1500*2) + (2000*1) = 5000
        row_c1 = resultado[resultado["ID_CLIENTE"] == 1].iloc[0]
        assert row_c1["TOTAL_COMPRAS"] == 5000.0

        # Cliente 2: (1000*3) + (300*5) = 4500
        row_c2 = resultado[resultado["ID_CLIENTE"] == 2].iloc[0]
        assert row_c2["TOTAL_COMPRAS"] == 4500.0

    def test_contagem_pedidos(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        # Cliente 1: 2 pedidos
        row_c1 = resultado[resultado["ID_CLIENTE"] == 1].iloc[0]
        assert row_c1["QTD_PEDIDOS"] == 2

    def test_todos_clientes_presentes(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        clientes_unicos = df_pedidos["ID_CLIENTE"].nunique()
        assert len(resultado) == clientes_unicos


class TestRankearTopN:
    """Testes para o ranking dos top N clientes."""

    def test_top_10_retorna_no_maximo_10(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        assert len(resultado) <= 10

    def test_top_3(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=3)

        assert len(resultado) == 3

    def test_ranking_ordenado(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        valores = resultado["TOTAL_COMPRAS"].tolist()
        assert valores == sorted(valores, reverse=True)

    def test_primeiro_lugar_correto(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        primeiro = resultado[resultado["RANKING"] == 1].iloc[0]
        # Cliente 11: 2000*3 = 6000 (maior valor)
        assert primeiro["ID_CLIENTE"] == 11
        assert primeiro["TOTAL_COMPRAS"] == 6000.0


class TestEnriquecerComClientes:
    """Testes para o enriquecimento com dados de clientes."""

    def test_colunas_presentes(self, df_pedidos, df_clientes):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        df_ranking = VendasTransforms.rankear_top_n(df_agregado, n=10)
        resultado = VendasTransforms.enriquecer_com_clientes(df_ranking, df_clientes)

        colunas_esperadas = {
            "RANKING",
            "ID_CLIENTE",
            "NOME_CLIENTE",
            "EMAIL_CLIENTE",
            "TOTAL_COMPRAS",
            "QTD_PEDIDOS",
        }
        assert set(resultado.columns) == colunas_esperadas

    def test_nome_cliente_correto(self, df_pedidos, df_clientes):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        df_ranking = VendasTransforms.rankear_top_n(df_agregado, n=10)
        resultado = VendasTransforms.enriquecer_com_clientes(df_ranking, df_clientes)

        row_c1 = resultado[resultado["ID_CLIENTE"] == 1].iloc[0]
        assert row_c1["NOME_CLIENTE"] == "Alice Silva"


class TestPipelineCompleto:
    """Testes para o pipeline completo de transformação."""

    def test_pipeline_executa_sem_erro(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)
        assert len(resultado) > 0

    def test_pipeline_retorna_top_10(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)
        assert len(resultado) <= 10

    def test_pipeline_ranking_correto(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)

        valores = resultado["TOTAL_COMPRAS"].tolist()
        assert valores == sorted(valores, reverse=True)
