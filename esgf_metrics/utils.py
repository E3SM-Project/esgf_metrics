from typing import Literal, Union

import pandas as pd


def bytes_to(
    bytes: Union[str, int],
    to: Literal["kb", "mb", "gb", "tb"],
    bsize: Literal[1024, 1000] = 1024,
) -> float:
    """Convert bytes to another unit.

    :param bytes: Bytes value
    :type bytes: Union[str, int]
    :param to: Unit to convert to
    :type to: Literal["kb", "mb", "gb", "tb"]
    :param bsize: Bytes size, defaults to 1024
    :type bsize: int, optional
    :return: Converted data units
    :rtype: float
    """
    map_sizes = {"kb": 1, "mb": 2, "gb": 3, "t": 4}

    bytes_float = float(bytes)
    return bytes_float / (bsize ** map_sizes[to])


def _cast_period_cols_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Cast the pd.Period dtype to str.

    The method is useful when writing the DataFrame out to the database.

    Parameters
    ----------
    df: pd.DataFrame
        The DataFrame of successful HTTP requests.

    Returns
    -------
    pd.DataFrame
        The DataFrame of successful HTTP requests.
    """
    if "year_month" in df.columns:
        df["year_month"] = df["year_month"].astype(str)

    if "fiscal_year_quarter" in df.columns:
        df["fiscal_year_quarter"] = df["fiscal_year_quarter"].astype(str)

    return df
