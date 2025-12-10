import logging

from bdeseries.download import download
from bdeseries.utils import create_data_dir

logger: logging.Logger = logging.getLogger(__name__)

_INITIALIZED: bool = False


def initialize(*, download_catalog: bool = False) -> None:
    """
    Prepara el entorno de bdeseries.

    - Crea el directorio de datos si no existe.
    - Opcionalmente, descarga el catálogo de datos y/o lo regenera.
    """

    global _INITIALIZED

    # si el entorno ya está inicializado, omitimos
    if _INITIALIZED:
        logger.info("bdeseries is already initialized")
        return

    logger.info("initializing bdeseries...")
    create_data_dir()

    if download_catalog:
        download(force_download=True)

    _INITIALIZED = True
