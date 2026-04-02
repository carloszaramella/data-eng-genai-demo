"""Módulo de I/O abstraído via Strategy Pattern.

O DataIOManager resolve IDs lógicos (ex.: ``pedidos_bronze``) em caminhos
físicos e delega a leitura/escrita ao *reader* adequado (CSV, JSON, Parquet).
"""

import glob
import os
from typing import Any, Dict

import pandas as pd

from src.core.exceptions import DataNotFoundError, OutputNotFoundError


class DataIOManager:
    """Gerencia leitura e escrita de DataFrames usando IDs lógicos."""

    def __init__(
        self,
        catalog: Dict[str, Any],
        output_config: Dict[str, Any],
        base_path: str,
    ) -> None:
        self._catalog = catalog
        self._output_config = output_config
        self._base_path = base_path

    # ------------------------------------------------------------------
    # Leitura
    # ------------------------------------------------------------------

    def read(self, dataset_id: str) -> pd.DataFrame:
        """Lê um DataFrame a partir de um ID lógico do catálogo.

        Raises
        ------
        DataNotFoundError
            Se o ``dataset_id`` não existir no catálogo.
        ValueError
            Se o formato declarado não for suportado.
        """
        if dataset_id not in self._catalog:
            raise DataNotFoundError(dataset_id)

        entry = self._catalog[dataset_id]
        fmt = entry["format"]
        path = os.path.join(self._base_path, entry["path"])
        options = entry.get("options", {})

        if fmt == "json":
            return self._read_json(path, options)
        if fmt == "csv":
            return self._read_csv(path, options)
        raise ValueError(f"Formato de leitura não suportado: {fmt}")

    def _read_json(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê um arquivo JSON-lines (suporta ``.gz``)."""
        return pd.read_json(path, lines=True, **options)

    def _read_csv(self, path: str, options: Dict[str, Any]) -> pd.DataFrame:
        """Lê arquivo(s) CSV de um caminho ou diretório (suporta ``.gz``)."""
        if os.path.isdir(path):
            pattern = os.path.join(path, "*.csv*")
            files = sorted(glob.glob(pattern))
            if not files:
                raise DataNotFoundError(f"Nenhum arquivo CSV encontrado em: {path}")
            frames = [pd.read_csv(f, **options) for f in files]
            return pd.concat(frames, ignore_index=True)
        return pd.read_csv(path, **options)

    # ------------------------------------------------------------------
    # Escrita
    # ------------------------------------------------------------------

    def write(self, df: pd.DataFrame, output_id: str) -> None:
        """Persiste um DataFrame usando a configuração de saída.

        Raises
        ------
        OutputNotFoundError
            Se o ``output_id`` não existir na configuração de outputs.
        ValueError
            Se o formato declarado não for suportado.
        """
        if output_id not in self._output_config:
            raise OutputNotFoundError(output_id)

        entry = self._output_config[output_id]
        fmt = entry.get("format", "parquet")
        filename = entry.get("filename", output_id)
        path = os.path.join(self._base_path, entry["path"])
        os.makedirs(path, exist_ok=True)

        if fmt == "parquet":
            dest = os.path.join(path, f"{filename}.parquet")
            df.to_parquet(dest, index=False)
        elif fmt == "csv":
            dest = os.path.join(path, f"{filename}.csv")
            df.to_csv(dest, index=False)
        else:
            raise ValueError(f"Formato de escrita não suportado: {fmt}")
