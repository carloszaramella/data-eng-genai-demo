"""Transformações puras de dados para o pipeline de vendas.

Todas as funções recebem DataFrames e retornam DataFrames,
garantindo testabilidade total sem dependência de I/O.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


class VendasTransforms:
    """Contém as transformações de negócio para o ranking de clientes."""

    @staticmethod
    def calcular_valor_total_pedido(df_pedidos: DataFrame) -> DataFrame:
        """Calcula o valor total de cada pedido (VALOR_UNITARIO * QUANTIDADE)."""
        return df_pedidos.withColumn("VALOR_TOTAL", F.col("VALOR_UNITARIO") * F.col("QUANTIDADE"))

    @staticmethod
    def agregar_por_cliente(df_pedidos_com_total: DataFrame) -> DataFrame:
        """Agrega o valor total de compras por cliente."""
        return df_pedidos_com_total.groupBy("ID_CLIENTE").agg(
            F.sum("VALOR_TOTAL").alias("TOTAL_COMPRAS"),
            F.count("ID_PEDIDO").alias("QTD_PEDIDOS"),
        )

    @staticmethod
    def rankear_top_n(df_agregado: DataFrame, n: int = 10) -> DataFrame:
        """Retorna os top N clientes por volume total de compras."""
        window = Window.orderBy(F.col("TOTAL_COMPRAS").desc())
        return (
            df_agregado.withColumn("RANKING", F.row_number().over(window))
            .filter(F.col("RANKING") <= n)
            .orderBy("RANKING")
        )

    @staticmethod
    def enriquecer_com_clientes(df_ranking: DataFrame, df_clientes: DataFrame) -> DataFrame:
        """Faz o join do ranking com os dados de clientes para enriquecer o resultado."""
        return df_ranking.join(
            df_clientes.select(
                F.col("id").alias("ID_CLIENTE"),
                F.col("nome").alias("NOME_CLIENTE"),
                F.col("email").alias("EMAIL_CLIENTE"),
            ),
            on="ID_CLIENTE",
            how="left",
        ).select(
            "RANKING",
            "ID_CLIENTE",
            "NOME_CLIENTE",
            "EMAIL_CLIENTE",
            "TOTAL_COMPRAS",
            "QTD_PEDIDOS",
        )

    @classmethod
    def executar_pipeline(
        cls, df_pedidos: DataFrame, df_clientes: DataFrame, top_n: int = 10
    ) -> DataFrame:
        """Executa o pipeline completo de transformação."""
        df_com_total = cls.calcular_valor_total_pedido(df_pedidos)
        df_agregado = cls.agregar_por_cliente(df_com_total)
        df_ranking = cls.rankear_top_n(df_agregado, top_n)
        df_enriquecido = cls.enriquecer_com_clientes(df_ranking, df_clientes)
        return df_enriquecido
