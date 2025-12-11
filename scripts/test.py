import logging
from pathlib import Path

import bdeseries

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


ROOT: Path = Path(__file__).parent.parent
DATA: Path = ROOT / "data"

bdeseries.set_data_path(DATA)
bdeseries.initialize(download_catalog=False)
bdeseries.generate_catalog()
