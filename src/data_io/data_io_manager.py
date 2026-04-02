"""Módulo de I/O abstraído com Strategy Pattern para leitura e escrita de dados."""

import glob
import os
from typing import Any, Dict

import pandas as pd

from src.core.exceptions import DataNotFoundError, OutputNotFoundError


class DataIOManager:
    """Gerencia leitura e escrita de DataFrames usando IDs lógicos do catálogo."""

    def __init__(
        self,
        catalog: Dict[str, Any],
        output_config: Dict[str, Any],
        base_path: str,
    ):
        self._catalog = catalog
        self._output_config = output_config
        self._base_path = base_path

    def read(self, dataset_id: str) -> pd.DataFrame:
        """Lê um DataFrame a partir de um ID lógico do catálogo."""
        if dataset_id not in self._catalog:
            raise DataNotFoundError(dataset_id)

        entry = self._catalog[dataset_id]
        data_format = entry["format"]
        path = os.path.join(self._base_path, entry["path"])
        options = entry.get("options", {})

        if data_format == "json":
            return self._read_json(path, options)
        elif data_format == "csv":
            return self._read_csv(path, options)
        else:
            raise ValueError(f"Formato não suportado: {data_format}")

    def _read_json(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê arquivos JSON/JSONL (suporta .gz)."""
        return pd.read_json(path, lines=True, **options)

    def _read_csv(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê arquivos CSV de um diretório (suporta .gz)."""
        if os.path.isdir(path):
            pattern = os.path.join(path, "*.csv*")
            files = sorted(glob.glob(pattern))
            if not files:
                raise DataNotFoundError(f"Nenhum arquivo CSV encontrado em: {path}")
            frames = [pd.read_csv(f, **options) for f in files]
            return pd.concat(frames, ignore_index=True)
        else:
            return pd.read_csv(path, **options)

    def write(self, df: pd.DataFrame, output_id: str) -> None:
        """Escreve um DataFrame usando a configuração de output do catálogo."""
        if output_id not in self._output_config:
            raise OutputNotFoundError(output_id)

        entry = self._output_config[output_id]
        data_format = entry.get("format", "parquet")
        path = os.path.join(self._base_path, entry["path"])

        os.makedirs(path, exist_ok=True)

        if data_format == "parquet":
            output_file = os.path.join(path, f"{output_id}.parquet")
            df.to_parquet(output_file, index=False)
        elif data_format == "csv":
            output_file = os.path.join(path, f"{output_id}.csv")
            df.to_csv(output_file, index=False)
        else:
            raise ValueError(f"Formato de output não suportado: {data_format}")
