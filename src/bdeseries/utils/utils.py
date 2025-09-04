import os
from pathlib import Path

from platformdirs import user_data_dir

APPNAME: str = "bdeseries"
ENV: str = "BDESERIES_DATA_PATH"


def get_data_path() -> Path:
    """Get the local path in which package specific data will be permanently stored.

    Returns
    -------
    Path
    """
    data_path_str: str | None = os.getenv(ENV)
    if data_path_str is not None:
        return Path(data_path_str)
    return Path(user_data_dir(appname=APPNAME))
