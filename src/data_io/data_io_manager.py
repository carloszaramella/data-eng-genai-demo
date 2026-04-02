"""Módulo de I/O abstraído com Strategy Pattern para leitura e escrita de dados."""

import os
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from src.core.exceptions import DataNotFoundError, OutputNotFoundError


class DataIOManager:
    """Gerencia leitura e escrita de DataFrames usando IDs lógicos do catálogo."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: Dict[str, Any],
        output_config: Dict[str, Any],
        base_path: str,
    ):
        self._spark = spark
        self._catalog = catalog
        self._output_config = output_config
        self._base_path = base_path

    def read(self, dataset_id: str) -> DataFrame:
        """Lê um DataFrame a partir de um ID lógico do catálogo."""
        if dataset_id not in self._catalog:
            raise DataNotFoundError(dataset_id)

        entry = self._catalog[dataset_id]
        data_format = entry["format"]
        path = os.path.join(self._base_path, entry["path"])
        options = entry.get("options", {})

        reader = self._spark.read.format(data_format)
        for key, value in options.items():
            reader = reader.option(key, str(value))

        return reader.load(path)

    def write(self, df: DataFrame, output_id: str) -> None:
        """Escreve um DataFrame usando a configuração de output do catálogo."""
        if output_id not in self._output_config:
            raise OutputNotFoundError(output_id)

        entry = self._output_config[output_id]
        data_format = entry.get("format", "parquet")
        path = os.path.join(self._base_path, entry["path"])
        mode = entry.get("mode", "overwrite")

        df.write.format(data_format).mode(mode).save(path)
