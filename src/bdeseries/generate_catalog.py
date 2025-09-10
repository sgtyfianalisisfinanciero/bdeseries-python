"""
#' This function Generate a catalog of the series contained in the csv files located in a given directory passed as argument.
#' @param directory Character string. The name of the directory that contains the csv files to be processed.
#' @param db Character string. The name of the database that the series contained in the csv belong to.
#' @keywords download full banco de españa series
#' @export
#' @examples generate_catalog("TE_CF)
"""

# TODO: continue in line 70 of the original R code

import calendar
from datetime import datetime
from pathlib import Path

import pandas as pd

from bdeseries import DATA_PATH

MONTHS_TRANSLATE = {
    "ene": "Jan",
    "feb": "Feb",
    "mar": "Mar",
    "abr": "Apr",
    "may": "May",
    "jun": "Jun",
    "jul": "Jul",
    "ago": "Aug",
    "sep": "Sep",
    "oct": "Oct",
    "nov": "Nov",
    "dic": "Dec",
}


def __convert_date(date_str: str) -> datetime:
    date_str: str = date_str.strip()
    date_len: int = len(date_str)
    if date_len == 4:  # interpret as a year
        return datetime.strptime(f"01 01 {date_str}", "%d %m %Y")
    elif date_len == 8:  # interpret as abbreviated moth and year
        try:
            month_str: str = MONTHS_TRANSLATE[date_str[:3].lower()]
            year_str: str = date_str[4:]
            month_num: int = datetime.strptime(month_str, "%b").month
            last_day: int = calendar.monthrange(int(year_str), month_num)[1]
            return datetime.strptime(
                f"{last_day} {month_str} {year_str}", "%d %b %Y"
            )
        except Exception as _:
            return pd.NaT
    else:  # interpret as day, abbreviated moth and year
        try:
            date_str = f"{date_str[:2]} {MONTHS_TRANSLATE[date_str[3:6].lower()]} {date_str[6:]}"
            return datetime.strptime(date_str, "%d %b %Y")
        except Exception as _:
            return pd.NaT


def generate_catalog(path: Path | None = None, db: str = ""):

    full_path: Path = DATA_PATH / path if path is not None else DATA_PATH
    csv_files: list[Path] = [
        file
        for file in full_path.iterdir()
        if (file.suffix == ".csv") and ("catalogo" not in file.stem)
    ]

    for file in csv_files:
        print(file.name)
        data: pd.DataFrame = pd.read_csv(
            file, encoding="latin1", skipinitialspace=True
        )
        data = data.tail(len(data.index) - 5)
        data = data.loc[:, ~data.columns.duplicated()]
        data.columns.values[0] = "fecha"
        data = data.loc[~data["fecha"].isin(["FUENTE", "NOTAS"]), :]
        data["fecha"] = data["fecha"].astype(str).apply(__convert_date)
