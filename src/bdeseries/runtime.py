from pathlib import Path

from bdeseries.download import download
from bdeseries.utils import create_data_dir

_INITIALIZED: bool = False

DATA_PATH: Path | None = None
FINANCIAL_ACCOUNTS_PATH: Path | None = None


def initialize(*, download_catalog: bool = False) -> None:
    """
    Prepara el entorno de bdeseries.

    - Crea el directorio de datos si no existe.
    - Opcionalmente, descarga el catálogo de datos y/o lo regenera.
    """

    global _INITIALIZED, DATA_PATH, FINANCIAL_ACCOUNTS_PATH

    # si el entorno ya está inicializado, omitimos
    if _INITIALIZED:
        return

    DATA_PATH = create_data_dir()
    FINANCIAL_ACCOUNTS_PATH = DATA_PATH / "cf"
    FINANCIAL_ACCOUNTS_PATH.mkdir(parents=True, exist_ok=True)

    if download_catalog:
        download(force_download=True)

    _INITIALIZED = True
