"""Módulo responsável pelo carregamento da configuração YAML.

O ConfigLoader é a interface única para acessar parâmetros do pipeline,
garantindo que nenhum caminho ou threshold esteja hardcoded no código.
"""

import os
from typing import Any, Dict, Optional

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    """Carrega e disponibiliza a configuração do pipeline a partir de YAML."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        if config_path is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            config_path = os.path.join(project_root, "config", "config.yaml")

        if not os.path.isfile(config_path):
            raise ConfigNotFoundError(config_path)

        with open(config_path, "r", encoding="utf-8") as fh:
            self._data: Dict[str, Any] = yaml.safe_load(fh)

    @property
    def config(self) -> Dict[str, Any]:
        """Dicionário completo de configuração."""
        return self._data

    def get_app_config(self) -> Dict[str, Any]:
        """Retorna a seção ``app`` (nome, log_level, …)."""
        return self._data.get("app", {})

    def get_catalog(self) -> Dict[str, Any]:
        """Retorna o catálogo de datasets de entrada."""
        return self._data.get("catalog", {})

    def get_output_config(self) -> Dict[str, Any]:
        """Retorna a configuração dos outputs."""
        return self._data.get("output", {})
