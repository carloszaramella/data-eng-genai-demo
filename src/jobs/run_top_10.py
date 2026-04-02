"""Job de orquestração do pipeline Top 10 Clientes."""

import logging
from typing import Optional

from src.data_io.data_io_manager import DataIOManager
from src.transforms.vendas_transforms import VendasTransforms


class RunTop10Job:
    """Orquestra a execução do pipeline Top 10 Clientes."""

    def __init__(self, data_io: DataIOManager, logger: Optional[logging.Logger] = None):
        self._data_io = data_io
        self._logger = logger or logging.getLogger(__name__)

    def run(self) -> None:
        """Executa o pipeline completo."""
        self._logger.info("Iniciando pipeline Top 10 Clientes...")

        self._logger.info("Lendo dados de pedidos (pedidos_bronze)...")
        df_pedidos = self._data_io.read("pedidos_bronze")
        self._logger.info("Pedidos carregados: %d registros", len(df_pedidos))

        self._logger.info("Lendo dados de clientes (clientes_bronze)...")
        df_clientes = self._data_io.read("clientes_bronze")
        self._logger.info("Clientes carregados: %d registros", len(df_clientes))

        self._logger.info("Executando transformações...")
        df_top_10 = VendasTransforms.executar_pipeline(df_pedidos, df_clientes)

        self._logger.info("Resultado - Top 10 Clientes:")
        self._logger.info("\n%s", df_top_10.to_string(index=False))

        self._logger.info("Salvando resultado em top_10_clientes...")
        self._data_io.write(df_top_10, "top_10_clientes")

        self._logger.info("Pipeline Top 10 Clientes finalizado com sucesso!")
