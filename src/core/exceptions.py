"""Exceções customizadas para o pipeline de dados."""


class ConfigNotFoundError(FileNotFoundError):
    """Lançada quando o arquivo de configuração YAML não é encontrado."""

    def __init__(self, path: str) -> None:
        self.path = path
        super().__init__(f"Arquivo de configuração não encontrado: {path}")


class DataNotFoundError(KeyError):
    """Lançada quando um ID lógico não existe no catálogo de datasets."""

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id
        super().__init__(f"Dataset não encontrado no catálogo: '{dataset_id}'")


class OutputNotFoundError(KeyError):
    """Lançada quando um ID lógico não existe na configuração de saída."""

    def __init__(self, output_id: str) -> None:
        self.output_id = output_id
        super().__init__(f"Configuração de output não encontrada: '{output_id}'")
