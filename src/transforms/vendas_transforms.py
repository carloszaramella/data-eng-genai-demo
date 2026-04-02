"""Transformações puras de dados para o pipeline de vendas.

Todas as funções recebem DataFrames e retornam DataFrames,
garantindo testabilidade total sem dependência de I/O.
"""

import pandas as pd


class VendasTransforms:
    """Contém as transformações de negócio para o ranking de clientes."""

    @staticmethod
    def calcular_valor_total_pedido(df_pedidos: pd.DataFrame) -> pd.DataFrame:
        """Calcula o valor total de cada pedido (VALOR_UNITARIO * QUANTIDADE)."""
        df = df_pedidos.copy()
        df["VALOR_TOTAL"] = df["VALOR_UNITARIO"] * df["QUANTIDADE"]
        return df

    @staticmethod
    def agregar_por_cliente(df_pedidos_com_total: pd.DataFrame) -> pd.DataFrame:
        """Agrega o valor total de compras por cliente."""
        return df_pedidos_com_total.groupby("ID_CLIENTE", as_index=False).agg(
            TOTAL_COMPRAS=("VALOR_TOTAL", "sum"),
            QTD_PEDIDOS=("ID_PEDIDO", "count"),
        )

    @staticmethod
    def rankear_top_n(df_agregado: pd.DataFrame, n: int = 10) -> pd.DataFrame:
        """Retorna os top N clientes por volume total de compras."""
        df = (
            df_agregado.sort_values("TOTAL_COMPRAS", ascending=False).head(n).reset_index(drop=True)
        )
        df["RANKING"] = range(1, len(df) + 1)
        return df

    @staticmethod
    def enriquecer_com_clientes(
        df_ranking: pd.DataFrame, df_clientes: pd.DataFrame
    ) -> pd.DataFrame:
        """Faz o join do ranking com os dados de clientes."""
        df_clientes_sel = df_clientes[["id", "nome", "email"]].rename(
            columns={
                "id": "ID_CLIENTE",
                "nome": "NOME_CLIENTE",
                "email": "EMAIL_CLIENTE",
            }
        )
        df = pd.merge(df_ranking, df_clientes_sel, on="ID_CLIENTE", how="left")
        return df[
            [
                "RANKING",
                "ID_CLIENTE",
                "NOME_CLIENTE",
                "EMAIL_CLIENTE",
                "TOTAL_COMPRAS",
                "QTD_PEDIDOS",
            ]
        ]

    @classmethod
    def executar_pipeline(
        cls,
        df_pedidos: pd.DataFrame,
        df_clientes: pd.DataFrame,
        top_n: int = 10,
    ) -> pd.DataFrame:
        """Executa o pipeline completo de transformação."""
        df_com_total = cls.calcular_valor_total_pedido(df_pedidos)
        df_agregado = cls.agregar_por_cliente(df_com_total)
        df_ranking = cls.rankear_top_n(df_agregado, top_n)
        df_enriquecido = cls.enriquecer_com_clientes(df_ranking, df_clientes)
        return df_enriquecido
