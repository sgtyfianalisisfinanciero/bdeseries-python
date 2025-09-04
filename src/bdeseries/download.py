from datetime import datetime

from bdeseries import DATA_PATH


def download(force_download: bool = False):
    """Download all the files from BdE if they have not been already downloaded today, unless force_download is set to True, then the files will be downloaded no matter what.

    Parameters
    ----------
    force_download : bool, optional
        Flag to force the download without checking if the files are already up to date, by default False
    """

    if force_download:
        pass  # download

    # check if all the downloaded files have been modified today
    today_date = datetime.today().date()
    already_downloaded_today: bool = all(
        [
            datetime.fromtimestamp(file.stat().st_mtime).date() == today_date
            for file in DATA_PATH.iterdir()
            if file.suffix == ".csv"
        ]
    )
    if not already_downloaded_today:
        pass  # download
