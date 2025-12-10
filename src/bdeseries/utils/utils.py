import os
from pathlib import Path
from typing import Final

from platformdirs import user_data_dir

APPNAME: Final[str] = "bdeseries"
ENV: Final[str] = "BDESERIES_DATA_PATH"
FINANTIAL_ACCOUNTS_ALIAS: Final[str] = "cf"

# cache to avoid recomputing over and over again
DATA_PATH: Path | None = None


def _compute_data_path() -> Path:
    """Calcula la ruta donde se almacenan los datos"""

    # obtiene la ruta, da prioridad a la variable de entorno sobre la ruta por defecto
    environment_value: str | None = os.getenv(ENV)
    if environment_value is not None:
        data_path: Path = Path(environment_value)
    else:
        data_path: Path = Path(user_data_dir(appname=APPNAME))

    # normaliza la ruta
    data_path = data_path.expanduser()
    data_path = data_path.resolve(strict=False)
    return data_path


def get_data_path() -> Path:
    """Devuelve la ruta donde se almacenan los datos"""
    global DATA_PATH
    if DATA_PATH is None:  # cache miss
        DATA_PATH = _compute_data_path()  # cache update
    return DATA_PATH


def get_finantial_accounts_path() -> Path:
    """Devuelve la ruta donde se almacenan los datos de cuenta financiera"""
    return get_data_path() / FINANTIAL_ACCOUNTS_ALIAS


def set_data_path(path: Path | str) -> Path:
    """Fija manualmente la ruta donde se almacenarán los datos"""
    global DATA_PATH

    if isinstance(path, str):
        data_path: Path = Path(path)
    else:
        data_path: Path = path

    # normaliza la ruta
    data_path = data_path.expanduser()
    data_path = data_path.resolve(strict=False)
    DATA_PATH = data_path  # cache update
    return DATA_PATH


def create_data_dir() -> None:
    """Crea el directorio donde se almacenarán los datos"""
    data_path: Path = get_data_path()
    financial_accounts_path: Path = get_finantial_accounts_path()
    data_path.mkdir(parents=True, exist_ok=True)
    financial_accounts_path.mkdir(parents=True, exist_ok=True)


def is_writable(path: Path | None = None) -> bool:
    """Comprueba si se tienen permisos de escritura en una determinada ruta"""
    import tempfile

    path: Path = path if path is not None else get_data_path()
    try:
        # si la ruta no existe, o no es un directorio, fallará
        with tempfile.TemporaryFile(dir=path):
            pass
        return True
    except Exception:
        return False
