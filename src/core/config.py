"""Módulo responsável pelo carregamento da configuração."""

import os
from typing import Any, Dict, Optional

import yaml

from src.core.exceptions import ConfigNotFoundError


class ConfigLoader:
    """Carrega e disponibiliza a configuração do pipeline a partir de um arquivo YAML."""

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            config_path = os.path.join(project_root, "config", "config.yaml")

        if not os.path.isfile(config_path):
            raise ConfigNotFoundError(config_path)

        with open(config_path, "r", encoding="utf-8") as f:
            self._config: Dict[str, Any] = yaml.safe_load(f)

    @property
    def config(self) -> Dict[str, Any]:
        """Retorna o dicionário completo de configuração."""
        return self._config

    def get_catalog(self) -> Dict[str, Any]:
        """Retorna o catálogo de datasets de entrada."""
        return self._config.get("catalog", {})

    def get_output_config(self) -> Dict[str, Any]:
        """Retorna a configuração de outputs."""
        return self._config.get("output", {})

    def get_app_config(self) -> Dict[str, Any]:
        """Retorna a configuração da aplicação."""
        return self._config.get("app", {})
