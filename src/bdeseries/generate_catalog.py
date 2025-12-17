"""modulo fundamental, se transforman los csv en un archivo gigante sobre el que se hacen las consultas"""

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final

import pandas as pd
from pandas.tseries.offsets import MonthEnd

from bdeseries.utils.utils import get_data_path

logger: logging.Logger = logging.getLogger(__name__)


class DateFormatError(Exception):
    def __init__(self, date_masks: dict[str, pd.Series]):
        super().__init__("Dataframe with several date formats")
        self.date_masks = date_masks


CATALOG_FLAG: Final[str] = "catalogo"


@dataclass(frozen=True)
class Month:
    abbr: str  # abreviatura del mes en español
    num: int  # número del mes


class Months(Enum):
    JANUARY = Month(abbr="ENE", num=1)
    FEBRUARY = Month(abbr="FEB", num=2)
    MARCH = Month(abbr="MAR", num=3)
    APRIL = Month(abbr="ABR", num=4)
    MAY = Month(abbr="MAY", num=5)
    JUNE = Month(abbr="JUN", num=6)
    JULY = Month(abbr="JUL", num=7)
    AUGUST = Month(abbr="AGO", num=8)
    SEPTEMBER = Month(abbr="SEP", num=9)
    OCTOBER = Month(abbr="OCT", num=10)
    NOVEMBER = Month(abbr="NOV", num=11)
    DECEMBER = Month(abbr="DIC", num=12)


# regular expressions for dates
MONTH_ABBRS: Final[set[str]] = {m.value.abbr for m in Months}
REGEX_MONTH_ABBRS: Final[str] = "|".join(MONTH_ABBRS)
YEAR_LENGTH: Final[int] = 4
DAY_LENGTH: Final[int] = 2
REGEX_YEAR: str = rf"^(\d{{{YEAR_LENGTH}}})$"
REGEX_MONTH_YEAR: str = rf"^({REGEX_MONTH_ABBRS}) (\d{{{YEAR_LENGTH}}})$"
REGEX_DAY_MONTH_YEAR: str = (
    rf"^(\d{{{DAY_LENGTH}}}) ({REGEX_MONTH_ABBRS})\s*(\d{{{YEAR_LENGTH}}})$"
)


@dataclass(frozen=True)
class DateFormat:
    regex: str


class DateFormats(Enum):
    YEAR = DateFormat(regex=REGEX_YEAR)
    MONTH_YEAR = DateFormat(regex=REGEX_MONTH_YEAR)
    DAY_MONTH_YEAR = DateFormat(regex=REGEX_DAY_MONTH_YEAR)


def _format_dates(raw_dates: pd.Index, date_format: DateFormats) -> pd.Series:
    abbr_to_num: Final[dict[str, int]] = {m.value.abbr: m.value.num for m in Months}
    aux = raw_dates.str.extract(date_format.value.regex)

    match date_format:
        case DateFormats.YEAR:
            aux.columns = ["year"]
            aux["day"] = 1
            aux["month"] = 1
            aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
            return pd.to_datetime(aux[["year", "month", "day"]], errors="coerce")
        case DateFormats.MONTH_YEAR:
            aux.columns = ["abbr", "year"]
            aux["day"] = 1
            aux["month"] = aux["abbr"].map(abbr_to_num)
            aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
            return pd.to_datetime(
                aux[["year", "month", "day"]], errors="coerce"
            ) + MonthEnd(0)
        case DateFormats.DAY_MONTH_YEAR:
            aux.columns = ["day", "abbr", "year"]
            aux["day"] = pd.to_numeric(aux["day"], errors="coerce")
            aux["month"] = aux["abbr"].map(abbr_to_num)
            aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
            return pd.to_datetime(aux[["year", "month", "day"]], errors="coerce")


def _get_date_format(masks: dict[str, pd.Series]) -> DateFormats | None:
    non_empty_masks: list[int] = [name for name, mask in masks.items() if mask.any()]
    if len(non_empty_masks) != 1:
        logger.error("Dataframe con varios formatos de fecha")
        return None
    return DateFormats[non_empty_masks[0]]


def _split_data(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # máscaras para fechas
    date_masks: dict[str, pd.Series] = {
        date_format.name: raw.index.str.fullmatch(date_format.value.regex)
        for date_format in DateFormats
    }

    # determina el formato de fecha que usa el dataframe
    date_format: DateFormats | None = _get_date_format(date_masks)
    if date_format is None:
        raise DateFormatError(date_masks)

    # máscara de datos y metadados
    data_mask: pd.Series = date_masks[date_format.name]
    metadata_mask: pd.Series = ~(data_mask)

    # TODO: sanity check para comprobar que los metadatos son válidos

    data: pd.DataFrame = raw.loc[data_mask, :].copy()
    data.index = _format_dates(raw_dates=data.index, date_format=date_format)

    metadata: pd.DataFrame = raw.loc[metadata_mask, :].copy()
    return data, metadata


def generate_catalog(directory: str | None = None, db: str | None = None):
    """Genera un catalogo de las series contenidas en los archivos csv almacenados en el directorio indicado"""
    # db: nombre de la base de datos a la que la serie pertenece
    # ejemplo: generate_catalog("TE_CF")

    db: str = "" if db is None else db

    path: Path = (
        get_data_path() / directory if directory is not None else get_data_path()
    )
    logger.info(f"Generating catalog from {path}")

    csv_files: list[Path] = [file for file in path.iterdir() if file.suffix == ".csv"]
    csv_file_total: int = len(csv_files)

    for idx, file in enumerate(csv_files):
        logger.info(
            f"Procesando fichero {file.stem}: {idx + 1} de {csv_file_total} \t - \t {((idx + 1) / csv_file_total) * 100:.2f}%"
        )

        # omitir archivos de catálogo
        if CATALOG_FLAG in file.stem:
            logger.info(f"Omitiendo {file.name}")
            continue

        raw: pd.DataFrame = pd.read_csv(file, encoding="latin1", index_col=0)

        ##### preprocessing dataframe #####
        # is this really necessary?
        # trim whitespace
        # remove duplicated column names (i guess it is)

        try:
            data, metadata = _split_data(raw)
        except DateFormatError as e:
            logger.error(f"Date format error in {file.name}", e.date_masks)

        # min_date = raw[DATE].min()
        # max_date = raw[DATE].max()

        # now we care about metadata
        # if raw.iloc[3, 0] != UNIT_DESCRIPTION and False:
        #     logger.warning(f"{file.name}\n{raw.iloc[3, 0]}")


#   catalogo <- lapply(
#     X=csv_files,
#     function(.x) {


#     # some csvs have a malformed structure in which row 4 of first column does not contain the units, but the description
#     # while having at row 3 a (possibly) irrelevant description.
#     # this needs to be accounted for. If it's the case, variable offset_serie will be set to one
#     short_csv_format <- FALSE


#     # some csvs do not contain FUENTE
#     withoutfuente <- FALSE


#     if (csv_datos[4,1] != "DESCRIPCIÓN DE LAS UNIDADES") {
#       # some csvs contain only three headers, and then continue to having dates:
#       if (stringr::str_detect(csv_datos_procesado[4], "FUENTE") |  stringr::str_detect(csv_datos_procesado[5], "FUENTE")) {
#         offset_serie <- 1
#         # cuando la tercera fila contiene una fecha
#         } else if(stringr::str_detect(csv_datos_procesado[[1]][4], "\\b\\d{4}\\b")) {
#         short_csv_format <- TRUE
#       }
#     } else {
#       offset_serie <- 0
#     }


#     series_en_csv_df <- lapply(
#       X=(names(csv_datos)) |> _[-1],
#       FUN=function(columna) {

#       descripcion <- stringr::str_remove(csv_datos[[columna]][3], pattern="Descripción de la DSD:")
#       alias <- as.character(csv_datos[[columna]][2])

#       # some series' descriptions contain a description of the unit instead of the description itself
#       # in these cases, alias is used to fill up descripcion field.
#       if (!grepl("Miles de Euros", descripcion) &
#           (descripcion != "Euros") &
#           (descripcion != "Años") &
#           (descripcion != "Monedas") &
#           (descripcion != "Billetes") &
#           (descripcion != "Porcentaje") &
#           (!is.na(descripcion))) {
#         descripcion <- descripcion
#       } else {
#         descripcion <- alias
#       }

#       serie_cf <- dplyr::tibble(nombre=columna,
#                                 numero=as.character(csv_datos[[columna]][1]),
#                                 alias=alias,
#                                 fichero=.x,
#                                 descripcion=descripcion,
#                                 tipo="",
#                                 unidades=dplyr::if_else(short_csv_format,
#                                                  "",
#                                                  dplyr::if_else(is.na(csv_datos[[columna]][4+offset_serie]),
#                                                                 "",
#                                                                 as.character(csv_datos[[columna]][4+offset_serie]))),
#                                 exponente="",
#                                 decimales="",
#                                 descripcion_unidades_exponente="",
#                                 frecuencia=dplyr::if_else(short_csv_format,
#                                                    "",
#                                                    dplyr::if_else(is.na(csv_datos[[columna]][5+offset_serie]),
#                                                                   "",
#                                                                   as.character(csv_datos[[columna]][5+offset_serie]))),
#                                 fecha_primera_observacion=fecha_minima,
#                                 fecha_ultima_observacion=fecha_maxima,
#                                 numero_observaciones=nrow(csv_datos_procesado[columna]),
#                                 titulo="",
#                                 fuente=as.character(csv_datos[[columna]][length(csv_datos[[columna]])-1]),
#                                 notas="",
#                                 db=db)
#       return(serie_cf)

#       }
#     ) |> dplyr::bind_rows()

#     csv_file_counter <<- csv_file_counter + 1

#     return(series_en_csv_df)
#     }
#   ) |>
#     dplyr::bind_rows() |>
#     dplyr::ungroup() |>
#     dplyr::mutate( # extraer de la ruta al fichero todos los directorios
#       fichero = stringr::str_extract(fichero, "(?<=bdeseries/).*$")
#     )

#   # remove duplicated column names again from the full catalog
#   catalogo <- catalogo[ , !duplicated(colnames(catalogo))]

#   return(catalogo)

# }
