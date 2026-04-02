"""Transformações puras de negócio para o pipeline de vendas.

Todas as funções são *side-effect free*: recebem DataFrames e retornam
DataFrames, sem interação com I/O ou estado global.
"""

import pandas as pd


class VendasTransforms:
    """Encapsula as regras de transformação do ranking de clientes."""

    @staticmethod
    def calcular_valor_total_pedido(
        df_pedidos: pd.DataFrame,
    ) -> pd.DataFrame:
        """Adiciona a coluna ``VALOR_TOTAL`` (preço unitário × quantidade)."""
        df = df_pedidos.copy()
        df["VALOR_TOTAL"] = df["VALOR_UNITARIO"] * df["QUANTIDADE"]
        return df

    @staticmethod
    def agregar_por_cliente(
        df_pedidos_com_total: pd.DataFrame,
    ) -> pd.DataFrame:
        """Agrega valor total e quantidade de pedidos por cliente."""
        return df_pedidos_com_total.groupby("ID_CLIENTE", as_index=False).agg(
            TOTAL_COMPRAS=("VALOR_TOTAL", "sum"),
            QTD_PEDIDOS=("ID_PEDIDO", "count"),
        )

    @staticmethod
    def rankear_top_n(
        df_agregado: pd.DataFrame,
        n: int = 10,
    ) -> pd.DataFrame:
        """Retorna os *top N* clientes ordenados por ``TOTAL_COMPRAS``."""
        df = (
            df_agregado.sort_values("TOTAL_COMPRAS", ascending=False).head(n).reset_index(drop=True)
        )
        df["RANKING"] = range(1, len(df) + 1)
        return df

    @staticmethod
    def enriquecer_com_clientes(
        df_ranking: pd.DataFrame,
        df_clientes: pd.DataFrame,
    ) -> pd.DataFrame:
        """Junta o ranking com nome e e-mail dos clientes."""
        df_sel = df_clientes[["id", "nome", "email"]].rename(
            columns={
                "id": "ID_CLIENTE",
                "nome": "NOME_CLIENTE",
                "email": "EMAIL_CLIENTE",
            }
        )
        df = pd.merge(df_ranking, df_sel, on="ID_CLIENTE", how="left")
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
        """Executa o pipeline completo de transformação em sequência."""
        df_com_total = cls.calcular_valor_total_pedido(df_pedidos)
        df_agregado = cls.agregar_por_cliente(df_com_total)
        df_ranking = cls.rankear_top_n(df_agregado, top_n)
        return cls.enriquecer_com_clientes(df_ranking, df_clientes)
