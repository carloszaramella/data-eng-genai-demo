"""Configuração de logging padronizado para o pipeline."""

import logging
import sys


class LoggingSetup:
    """Configura o logging da aplicação."""

    @staticmethod
    def configure(log_level: str = "INFO", app_name: str = "pipeline") -> logging.Logger:
        """Configura e retorna um logger com o nível especificado."""
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)

        logger = logging.getLogger(app_name)
        logger.setLevel(numeric_level)

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(numeric_level)
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger
