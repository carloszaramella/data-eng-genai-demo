"""Factory para criação e gerenciamento da SparkSession."""

from typing import Any, Dict

from pyspark.sql import SparkSession


class SparkManager:
    """Gerencia o ciclo de vida da SparkSession."""

    def __init__(self, spark_config: Dict[str, Any]):
        self._spark_config = spark_config
        self._spark: SparkSession | None = None

    def get_or_create(self) -> SparkSession:
        """Cria ou retorna a SparkSession existente."""
        if self._spark is not None:
            return self._spark

        app_name = self._spark_config.get("app_name", "SparkApp")
        master = self._spark_config.get("master", "local[*]")

        builder = SparkSession.builder.appName(app_name).master(master)

        extra_config = self._spark_config.get("config", {})
        for key, value in extra_config.items():
            builder = builder.config(key, value)

        self._spark = builder.getOrCreate()
        return self._spark

    def stop(self) -> None:
        """Encerra a SparkSession."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
