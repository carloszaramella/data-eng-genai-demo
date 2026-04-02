"""Ponto de entrada da aplicação — Composition Root.

Instancia e injeta todas as dependências nos jobs,
seguindo o princípio de Injeção de Dependência.
"""

import os
import sys

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup


def main() -> None:
    """Configura dependências e executa o pipeline Top 10 Clientes."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    config_path = os.path.join(project_root, "config", "config.yaml")
    config_loader = ConfigLoader(config_path=config_path)

    app_cfg = config_loader.get_app_config()
    logger = LoggingSetup.configure(
        log_level=app_cfg.get("log_level", "INFO"),
        app_name=app_cfg.get("name", "pipeline"),
    )

    logger.info("Configuração carregada com sucesso.")

    try:
        data_io = DataIOManager(
            catalog=config_loader.get_catalog(),
            output_config=config_loader.get_output_config(),
            base_path=project_root,
        )

        job = RunTop10Job(data_io=data_io, logger=logger)
        job.run()
    except Exception as exc:
        logger.error("Erro na execução do pipeline: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
