"""Exceções customizadas para o pipeline de dados."""


class ConfigNotFoundError(Exception):
    """Exceção lançada quando o arquivo de configuração não é encontrado."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        super().__init__(f"Arquivo de configuração não encontrado: {config_path}")


class DataNotFoundError(Exception):
    """Exceção lançada quando um dataset não é encontrado no catálogo."""

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        super().__init__(f"Dataset não encontrado no catálogo: '{dataset_id}'")


class OutputNotFoundError(Exception):
    """Exceção lançada quando uma configuração de output não é encontrada."""

    def __init__(self, output_id: str):
        self.output_id = output_id
        super().__init__(f"Configuração de output não encontrada: '{output_id}'")
