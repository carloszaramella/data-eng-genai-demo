"""Testes unitários para as transformações de vendas."""

import pytest
from pyspark.sql import SparkSession

from src.transforms.vendas_transforms import VendasTransforms


@pytest.fixture(scope="session")
def spark():
    """Cria uma SparkSession para testes."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-vendas-transforms")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def df_pedidos(spark):
    """Cria um DataFrame de pedidos sintéticos."""
    data = [
        ("ped-001", "NOTEBOOK", 1500.0, 2, "2026-01-01T10:00:00", "SP", 1),
        ("ped-002", "CELULAR", 1000.0, 3, "2026-01-02T11:00:00", "RJ", 2),
        ("ped-003", "GELADEIRA", 2000.0, 1, "2026-01-03T12:00:00", "MG", 1),
        ("ped-004", "MONITOR", 600.0, 2, "2026-01-04T13:00:00", "SP", 3),
        ("ped-005", "LIQUIDIFICADOR", 300.0, 5, "2026-01-05T14:00:00", "RJ", 2),
        ("ped-006", "NOTEBOOK", 1500.0, 1, "2026-01-06T15:00:00", "MG", 4),
        ("ped-007", "CELULAR", 1000.0, 1, "2026-01-07T16:00:00", "SP", 5),
        ("ped-008", "GELADEIRA", 2000.0, 2, "2026-01-08T17:00:00", "RJ", 6),
        ("ped-009", "MONITOR", 600.0, 3, "2026-01-09T18:00:00", "MG", 7),
        ("ped-010", "NOTEBOOK", 1500.0, 3, "2026-01-10T19:00:00", "SP", 8),
        ("ped-011", "CELULAR", 1000.0, 2, "2026-01-11T20:00:00", "RJ", 9),
        ("ped-012", "LIQUIDIFICADOR", 300.0, 1, "2026-01-12T21:00:00", "MG", 10),
        ("ped-013", "GELADEIRA", 2000.0, 3, "2026-01-13T22:00:00", "SP", 11),
        ("ped-014", "MONITOR", 600.0, 1, "2026-01-14T23:00:00", "RJ", 12),
    ]
    columns = [
        "ID_PEDIDO",
        "PRODUTO",
        "VALOR_UNITARIO",
        "QUANTIDADE",
        "DATA_CRIACAO",
        "UF",
        "ID_CLIENTE",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def df_clientes(spark):
    """Cria um DataFrame de clientes sintéticos."""
    data = [
        (1, "Alice Silva", "alice@email.com"),
        (2, "Bruno Costa", "bruno@email.com"),
        (3, "Carla Dias", "carla@email.com"),
        (4, "Daniel Souza", "daniel@email.com"),
        (5, "Elena Rocha", "elena@email.com"),
        (6, "Felipe Lima", "felipe@email.com"),
        (7, "Gabi Santos", "gabi@email.com"),
        (8, "Hugo Pereira", "hugo@email.com"),
        (9, "Iris Alves", "iris@email.com"),
        (10, "João Martins", "joao@email.com"),
        (11, "Karen Ribeiro", "karen@email.com"),
        (12, "Lucas Ferreira", "lucas@email.com"),
    ]
    columns = ["id", "nome", "email"]
    return spark.createDataFrame(data, columns)


class TestCalcularValorTotalPedido:
    """Testes para o cálculo do valor total do pedido."""

    def test_valor_total_correto(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        assert "VALOR_TOTAL" in resultado.columns

        row = resultado.filter(resultado.ID_PEDIDO == "ped-001").collect()[0]
        assert row["VALOR_TOTAL"] == 3000.0  # 1500 * 2

    def test_valor_total_quantidade_multipla(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        row = resultado.filter(resultado.ID_PEDIDO == "ped-002").collect()[0]
        assert row["VALOR_TOTAL"] == 3000.0  # 1000 * 3

    def test_preserva_colunas_originais(self, df_pedidos):
        resultado = VendasTransforms.calcular_valor_total_pedido(df_pedidos)

        colunas_originais = set(df_pedidos.columns)
        colunas_resultado = set(resultado.columns)
        assert colunas_originais.issubset(colunas_resultado)


class TestAgregarPorCliente:
    """Testes para a agregação de compras por cliente."""

    def test_agregacao_soma_correta(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        # Cliente 1: (1500*2) + (2000*1) = 5000
        row_c1 = resultado.filter(resultado.ID_CLIENTE == 1).collect()[0]
        assert row_c1["TOTAL_COMPRAS"] == 5000.0

        # Cliente 2: (1000*3) + (300*5) = 4500
        row_c2 = resultado.filter(resultado.ID_CLIENTE == 2).collect()[0]
        assert row_c2["TOTAL_COMPRAS"] == 4500.0

    def test_contagem_pedidos(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        # Cliente 1: 2 pedidos
        row_c1 = resultado.filter(resultado.ID_CLIENTE == 1).collect()[0]
        assert row_c1["QTD_PEDIDOS"] == 2

    def test_todos_clientes_presentes(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        resultado = VendasTransforms.agregar_por_cliente(df_com_total)

        clientes_unicos = df_pedidos.select("ID_CLIENTE").distinct().count()
        assert resultado.count() == clientes_unicos


class TestRankearTopN:
    """Testes para o ranking dos top N clientes."""

    def test_top_10_retorna_no_maximo_10(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        assert resultado.count() <= 10

    def test_top_3(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=3)

        assert resultado.count() == 3

    def test_ranking_ordenado(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        rows = resultado.collect()
        valores = [row["TOTAL_COMPRAS"] for row in rows]
        assert valores == sorted(valores, reverse=True)

    def test_primeiro_lugar_correto(self, df_pedidos):
        df_com_total = VendasTransforms.calcular_valor_total_pedido(df_pedidos)
        df_agregado = VendasTransforms.agregar_por_cliente(df_com_total)
        resultado = VendasTransforms.rankear_top_n(df_agregado, n=10)

        primeiro = resultado.filter(resultado.RANKING == 1).collect()[0]
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

        row_c1 = resultado.filter(resultado.ID_CLIENTE == 1).collect()[0]
        assert row_c1["NOME_CLIENTE"] == "Alice Silva"


class TestPipelineCompleto:
    """Testes para o pipeline completo de transformação."""

    def test_pipeline_executa_sem_erro(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)
        assert resultado.count() > 0

    def test_pipeline_retorna_top_10(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)
        assert resultado.count() <= 10

    def test_pipeline_ranking_correto(self, df_pedidos, df_clientes):
        resultado = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)

        rows = resultado.collect()
        valores = [row["TOTAL_COMPRAS"] for row in rows]
        assert valores == sorted(valores, reverse=True)
