from pathlib import Path

import bdeseries

ROOT: Path = Path(__file__).parent.parent
DATA: Path = ROOT / "data"

bdeseries.set_data_path(DATA)
bdeseries.initialize(download_catalog=True)
