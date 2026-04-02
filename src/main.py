"""Ponto de entrada da aplicação — Composition Root.

Instancia e injeta todas as dependências nos jobs.
"""

import os
import sys

from src.core.config import ConfigLoader
from src.data_io.data_io_manager import DataIOManager
from src.jobs.run_top_10 import RunTop10Job
from src.utils.logging_setup import LoggingSetup
from src.utils.spark_manager import SparkManager


def main() -> None:
    """Composition Root: configura dependências e executa o pipeline."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    config_path = os.path.join(project_root, "config", "config.yaml")
    config_loader = ConfigLoader(config_path=config_path)

    app_config = config_loader.get_app_config()
    logger = LoggingSetup.configure(
        log_level=app_config.get("log_level", "INFO"),
        app_name=app_config.get("name", "pipeline"),
    )

    logger.info("Configuração carregada com sucesso.")

    spark_manager = SparkManager(config_loader.get_spark_config())
    spark = spark_manager.get_or_create()
    logger.info("SparkSession criada: %s", spark.sparkContext.appName)

    try:
        data_io = DataIOManager(
            spark=spark,
            catalog=config_loader.get_catalog(),
            output_config=config_loader.get_output_config(),
            base_path=project_root,
        )

        job = RunTop10Job(data_io=data_io, logger=logger)
        job.run()
    except Exception as e:
        logger.error("Erro na execução do pipeline: %s", e)
        sys.exit(1)
    finally:
        spark_manager.stop()
        logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
