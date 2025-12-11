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

CATALOG_FLAG: Final[str] = "catalogo"


INITIAL_ROWS_TO_IGNORE: Final[int] = 6

DATE: Final[str] = "fecha"

FAKE_DATES: Final[list[str]] = ["FUENTE", "NOTAS"]
UNIT_DESCRIPTION: Final[str] = "DESCRIPCIÓN DE LAS UNIDADES"


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


# ver cómo estructurar esto mejor semánticamente
class DateFormat(Enum):
    YEAR = 1
    MONTH_YEAR = 1
    DAY_MONTH_YEAR = 1


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


def _format_dates(raw: pd.Index) -> pd.Index:
    # máscaras para fechas (este cálculo está duplicado, ver cómo solucionar esto)
    # entiendo que solo se puede tener un formato de fecha dentro de un mismo dataframe, no debería ser problema
    year_mask: pd.Series = raw.str.fullmatch(REGEX_YEAR)
    month_year_mask: pd.Series = raw.str.fullmatch(REGEX_MONTH_YEAR)
    day_month_year_mask: pd.Series = raw.str.fullmatch(REGEX_DAY_MONTH_YEAR)

    ##### creamos la plantilla de la serie de salida
    formatted: pd.Index = pd.Index(pd.NaT, dtype="datetime64[ns]")

    abbr_to_num: Final[dict[str, int]] = {m.value.abbr: m.value.num for m in Months}

    ##### transformamos las fechas
    # año
    aux = raw[year_mask].str.extract(REGEX_YEAR)
    aux.columns = ["year"]
    aux["day"] = 1
    aux["month"] = 1
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted.loc[year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    )

    # abreviatura de mes + año
    aux = raw[month_year_mask].str.extract(REGEX_MONTH_YEAR)
    aux.columns = ["abbr", "year"]
    aux["day"] = 1
    aux["month"] = aux["abbr"].map(abbr_to_num)
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted.loc[month_year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    ) + MonthEnd(0)

    # día + abreviatura de mes + año
    aux = raw[day_month_year_mask].str.extract(REGEX_DAY_MONTH_YEAR)
    aux.columns = ["day", "abbr", "year"]
    aux["day"] = pd.to_numeric(aux["day"], errors="coerce")
    aux["month"] = aux["abbr"].map(abbr_to_num)
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted.loc[day_month_year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    )


def _split_index(raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # máscaras para fechas
    year_mask: pd.Series = raw.index.str.fullmatch(REGEX_YEAR)
    month_year_mask: pd.Series = raw.index.str.fullmatch(REGEX_MONTH_YEAR)
    day_month_year_mask: pd.Series = raw.index.str.fullmatch(REGEX_DAY_MONTH_YEAR)

    # máscara de datos y metadados
    data_mask: pd.Series = year_mask | month_year_mask | day_month_year_mask
    metadata_mask: pd.Series = ~(data_mask)

    # TODO: sanity check para comprobar que los metadatos son válidos

    data: pd.DataFrame = raw.loc[data_mask, :]
    metadata: pd.DataFrame = raw.loc[metadata_mask, :]
    return data, metadata

    ##### creamos la plantilla de la serie de salida
    formatted_date_series: pd.Series = pd.Series(
        pd.NaT, index=raw.index, dtype="datetime64[ns]"
    )

    ##### transformamos las fechas
    # año
    aux = raw.loc[year_mask].str.extract(REGEX_YEAR)
    aux.columns = ["year"]
    aux["day"] = 1
    aux["month"] = 1
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted_date_series.loc[year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    )

    # abreviatura de mes + año
    aux = raw.loc[month_year_mask].str.extract(REGEX_MONTH_YEAR)
    aux.columns = ["abbr", "year"]
    aux["day"] = 1
    aux["month"] = aux["abbr"].map(abbr_to_num)
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted_date_series.loc[month_year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    ) + MonthEnd(0)

    # día + abreviatura de mes + año
    aux = raw.loc[day_month_year_mask].str.extract(REGEX_DAY_MONTH_YEAR)
    aux.columns = ["day", "abbr", "year"]
    aux["day"] = pd.to_numeric(aux["day"], errors="coerce")
    aux["month"] = aux["abbr"].map(abbr_to_num)
    aux["year"] = pd.to_numeric(aux["year"], errors="coerce")
    formatted_date_series.loc[day_month_year_mask] = pd.to_datetime(
        aux[["year", "month", "day"]], errors="coerce"
    )

    ##### sanity check final
    if len(formatted_date_series[formatted_date_series.isna()].index) > 0 and False:
        logger.warning(
            f"In {file.name} the following dates could not be properly transformed:\n{raw[formatted_date_series.isna()]}"
        )

    return formatted_date_series, header_size


def _extract_metadata(data: pd.DataFrame):
    pass


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

        # TODO: we should preprocess the metadata first

        ##### preprocessing dataframe #####
        # is this really necessary?
        # trim whitespace
        # remove duplicated column names (i guess it is)

        data, metadata = _split_index(raw)
        data.index = _format_dates(data.index)
        print(metadata)

        # only_data = raw.iloc[INITIAL_ROWS_TO_IGNORE:]
        # only_data = only_data.loc[~only_data[DATE].isin(FAKE_DATES), :]

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
