"""Configuração padronizada de logging para o pipeline."""

import logging
import sys


class LoggingSetup:
    """Cria e configura loggers de forma centralizada."""

    @staticmethod
    def configure(
        log_level: str = "INFO",
        app_name: str = "pipeline",
    ) -> logging.Logger:
        """Retorna um ``Logger`` configurado com handler para *stdout*."""
        numeric_level = getattr(logging, log_level.upper(), logging.INFO)

        logger = logging.getLogger(app_name)
        logger.setLevel(numeric_level)

        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(numeric_level)
            fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            handler.setFormatter(fmt)
            logger.addHandler(handler)

        return logger
