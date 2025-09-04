import asyncio
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import aiohttp

from bdeseries import DATA_PATH

URL: str = "https://www.bde.es/webbe/es/estadisticas/compartido/datos/zip/"

ZIPS: dict[str, str] = {
    "be": "Boletín estadístico",
    "si": "Síntesis de indicadores",
    "ti": "Tipos de interés",
    "pb": "",
    "TE_CF": "",
}


async def download_file(url: str, filename: Path):
    """Asynchronously download a single file to the given destination path from the given url

    Parameters
    ----------
    url : str
        url to download the file from
    filename : Path
        destination path for the file
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            with open(filename, mode="wb") as file:
                while True:
                    chunk = await response.content.read()
                    if not chunk:
                        break
                    file.write(chunk)
                print(f"Downloaded file {filename}")


async def download_files():
    """Asynchronously download all the BdE zip files into a temporary directory, then extracts them into the user local package specific data directory"""

    with tempfile.TemporaryDirectory() as tmpdirname:
        parameters: list[tuple[str, Path, Path]] = []
        for file in ZIPS:
            url: str = f"{URL}{file}.zip"
            extract_dir: Path = (
                DATA_PATH if file != "TE_CF" else DATA_PATH / "cf"
            )
            filename = Path(tmpdirname) / f"{file}.zip"
            parameters.append((url, extract_dir, filename))

        tasks = [
            download_file(url, filename) for url, _, filename in parameters
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        for _, extract_dir, filename in parameters:
            with zipfile.ZipFile(filename, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
                print(f"Unzipped {filename.name}")


def download(force_download: bool = False):
    """Download all the files from BdE if they have not been already downloaded today, unless force_download is set to True, then the files will be downloaded no matter what.

    Parameters
    ----------
    force_download : bool, optional
        Flag to force the download without checking if the files are already up to date, by default False
    """

    if force_download:
        asyncio.run(download_files())

    # check if all the downloaded files have been modified today
    today_date = datetime.today().date()
    modified_today: list[bool] = [
        datetime.fromtimestamp(file.stat().st_mtime).date() == today_date
        for file in DATA_PATH.iterdir()
        if file.suffix == ".csv"
    ]
    already_downloaded_today: bool = all(modified_today)
    if (not already_downloaded_today) or len(modified_today) == 0:
        asyncio.run(download_files())
