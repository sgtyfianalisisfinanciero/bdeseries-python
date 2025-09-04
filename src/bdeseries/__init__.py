"""When the package is imported, the package specific local data directory is created if it does not exists and the full data catalog is downloaded"""

from pathlib import Path

from bdeseries.utils import get_data_path

DATA_PATH: Path = get_data_path()
FINANCIAL_ACCOUNTS_PATH: Path = DATA_PATH / "cf"

DATA_PATH.mkdir(parents=True, exist_ok=True)
FINANCIAL_ACCOUNTS_PATH.mkdir(parents=True, exist_ok=True)

# TODO: Download the full data catalog
