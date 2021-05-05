import os
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, TypedDict

import pandas as pd
from tqdm import tqdm

from usage_plotter.utils import bytes_to

# Type annotations
ProjectTitle = Literal["E3SM", "E3SM in CMIP6"]
FiscalYear = Literal["2019", "2020", "2021"]
LogLine = TypedDict(
    "LogLine",
    {
        "log_line": str,
        "date": pd.Timestamp,
        "calendar_yr": Optional[int],
        "calendar_month": Optional[int],
        "requester_ip": str,
        "path": str,
        "dataset_id": str,
        "file_id": Optional[str],
        "access_type": str,
        "status_code": str,
        "bytes": str,
        "mb": float,
        "project": ProjectTitle,
        "realm": Optional[str],
        "data_type": Optional[str],
        "science_driver": Optional[str],
        "campaign": Optional[str],
    },
)

# E3SM Facets that are available in file/dataset id and directory format
AVAILABLE_FACETS = {
    "realm": ["ocean", "atmos", "land", "sea-ice"],
    "data_type": ["time-series", "climo", "model-output", "mapping", "restart"],
    # E3SM only
    "time_frequency": [
        "3hr",
        "3hr_snap",
        "5day_snap",
        "6hr",
        "6hr_ave",
        "6hr_snap",
        "day",
        "day_cosp",
        "fixed",
        "mon",
        "monClim",
    ],
    # E3SM in CMIP6
    "activity": ["C4MIP", "CMIP", "DAMIP", "ScenarioMIP"],
    # Unavailable in templates
    "science_driver": ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"],
    "campaign": ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"],
}


def parse_logs(path: str) -> pd.DataFrame:
    """Parses Apache logs to extract information into a DataFrame.

    :param path: Path to access logs
    :type path: str
    :return: DataFrame containing parsed logs
    :rtype: pd.DataFrame
    """
    logs_paths = fetch_logs(path)
    parsed_lines: List[LogLine] = []
    for log in tqdm(logs_paths):
        for line in filter_log_lines(log):
            parsed_line = parse_log_line(line)
            parsed_lines.append(parsed_line)

    if not parsed_lines:
        raise IndexError(
            "No log lines were parsed. Check that you set the correct logs path."
        )

    df = pd.DataFrame(parsed_lines)
    df["date"] = pd.to_datetime(df["date"])
    df["calendar_yr_month"] = df["date"].dt.to_period("M")

    return df


def fetch_logs(path: str) -> List[str]:
    """Fetches Apache logs from a path.

    :param path: Path to access logs
    :type path: str
    :yield: List of absolute path to access logs
    :rtype: List[str]
    """
    logs_paths: List[str] = []
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            logs_paths.append(str(Path(root, file).absolute()))
    return logs_paths


def filter_log_lines(path: str):
    """Filter log lines using a generator.

    :param path: Path to access logs
    :type path: str
    :yield: A line from a log file
    :rtype: str
    """
    with open(path, "r") as instream:
        while line := instream.readline():
            if (
                "E3SM" in line
                and "xml" not in line
                and "ico" not in line
                and "cmip6_variables" not in line
                and "html" not in line
                and "catalog" not in line
                and "aggregation" not in line
            ):
                yield line


def parse_log_line(line: str) -> LogLine:
    """Parse raw log line to extract HTTP request info.

    Refer to README.md for directory and dataset id templates.

    :param line: Raw log line from Apache log
    :type line: str
    :return: Parsed log row as a dictionary
    :rtype: LogRow
    """
    attrs = line.split()
    path = attrs[6].replace("%2F", "/")

    parsed_line: LogLine = {
        "log_line": line,
        "date": None,
        "calendar_yr": None,
        "calendar_month": None,
        "requester_ip": attrs[0],
        "path": path,
        "dataset_id": "",
        "file_id": "",
        "access_type": attrs[11],
        "status_code": attrs[8],
        "bytes": attrs[9],
        "mb": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
        "project": "E3SM" if "/E3SM-Project" not in path else "E3SM in CMIP6",
        "realm": None,
        "data_type": None,
        "science_driver": None,
        "campaign": None,
    }

    # None values are filled using helper functions below.
    parsed_line = parse_log_timestamp(parsed_line, raw_timestamp=attrs[3])
    parsed_line = parse_log_path(parsed_line, path)
    return parsed_line


def parse_log_timestamp(log_line: LogLine, raw_timestamp: str) -> LogLine:
    """Parses a string timestamp for datetime values.

    Example timestamp: "[15/Jul/2019:03:18:49 -0700]"

    :param log_line: Parsed log line
    :type log_line: Dict[str, Any]
    :param raw_timestamp: Raw timestamp from log line
    :type raw_timestamp: str
    :return: Parsed log line with datetime values
    :rtype: LogLine
    """
    timestamp = raw_timestamp[raw_timestamp.find("[") + 1 : raw_timestamp.find(":")]

    log_line["date"] = datetime.strptime(timestamp, "%d/%b/%Y").date()
    log_line["calendar_yr"] = log_line["date"].year
    log_line["calendar_month"] = log_line["date"].month

    return log_line


def parse_log_path(log_line: LogLine, path_in_log_line: str) -> LogLine:
    """Parses the path in the log line for the dataset id, file id, and facets.

    :param log_line: Parsed log line
    :type log_line: LogLine
    :param path_in_log_path: The path of the dataset/file transferred in the request
    :type path_in_log_path: str
    :return: Parsed log line with dataset id, file id, and facets
    :rtype: LogLine
    """
    try:
        idx = path_in_log_line.index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        # This usually means an HTTP 302/404 request (incorrect path)
        idx = None  # type: ignore

    log_line["dataset_id"] = ".".join(path_in_log_line[idx:].split("/")[:-1])
    log_line["file_id"] = path_in_log_line.split("/")[-1]
    dataset_facets = log_line["dataset_id"].split(".")

    for facet, options in AVAILABLE_FACETS.items():
        matching_facet = None
        for option in options:
            if option in dataset_facets:
                matching_facet = option
        log_line[facet] = matching_facet  # type: ignore

    return log_line


def gen_report(df: pd.DataFrame, facet: Optional[str] = None) -> pd.DataFrame:
    """Generates a report for total requests and data accessed on a monthly basis.

    It calculates the equivalent fiscal month based on the fiscal year using the
    calendar month.

    :param df: DataFrame containing parsed logs
    :type df: pd.DataFrame
    :param facet: Facet to aggregate and merge on, defaults to None
    :type facet: Optional[str], optional
    :return: DataFrame containing fiscal year monthly report
    :rtype: pd.DataFrame
    """
    agg_cols = ["calendar_yr_month", "calendar_yr", "calendar_month"]
    if facet:
        agg_cols.append(facet)

    # Total requests on a monthly basis
    df_req_by_mon = df.copy()
    df_req_by_mon = df_req_by_mon.value_counts(subset=agg_cols).reset_index(
        name="requests"
    )
    # Total data accessed on a monthly basis (only successful requests)
    df_data_by_mon = df.copy()
    df_data_by_mon = df_data_by_mon[df_data_by_mon.status_code.str.contains("200|206")]
    df_data_by_mon = (
        df_data_by_mon.groupby(by=agg_cols).agg({"mb": "sum"}).reset_index()
    )
    df_data_by_mon["gb"] = df_data_by_mon.mb.div(1024)

    # Calendar year report
    df_mon_report = pd.merge(df_req_by_mon, df_data_by_mon, on=agg_cols)
    df_mon_report = df_mon_report.sort_values(by=agg_cols)

    # Fiscal year report
    df_fy_report = resample_to_quarter(df_mon_report, facet)
    return df_fy_report


def resample_to_quarter(df: pd.DataFrame, facet: Optional[str]) -> pd.DataFrame:
    """
    Resamples a DataFrame to calculate the fiscal year, quarter, and month
    using the calendar year and month.

    :param df: DataFrame containing monthly report
    :type df: pd.DataFrame
    :param facet: Facet to aggregate on
    :type facet: Optional[str], optional
    :return: DataFrame containing monthly report for fiscal year
    :rtype: pd.DataFrame
    """
    df_resample = df.copy()

    # Get equivalent fiscal information from calendar dates
    df_resample["fy_quarter"] = df_resample.apply(
        lambda row: row.calendar_yr_month.asfreq("Q-JUN"), axis=1
    )
    df_resample["fiscal_yr"] = df_resample.fy_quarter.dt.strftime("%F")
    df_resample["fiscal_quarter"] = df_resample.fy_quarter.dt.strftime("%q")
    df_resample["fiscal_month"] = df_resample.apply(
        lambda row: convert_to_fiscal_month(row.calendar_month), axis=1
    )

    agg_cols = [
        "fiscal_yr",
        "fiscal_quarter",
        "fiscal_month",
        "calendar_yr",
        "calendar_month",
        facet,
    ]
    df_qt: pd.DataFrame = (
        df_resample.groupby(by=agg_cols)
        .agg({"requests": "sum", "gb": "sum"})
        .reset_index()
    )
    # Reorder columns for a cleaner dataframe output.
    df_qt = df_qt[
        [
            *agg_cols,
            "requests",
            "gb",
        ]
    ]

    return df_qt


def convert_to_fiscal_month(calendar_month: int) -> int:
    """Converts a calendar month to the E3SM fiscal month equivalent.

    NOTE: E3SM IG FY is from July-Jun

    :param month: Calendar month
    :type month: int
    :return: Fiscal month
    :rtype: int
    """
    map_calendar_to_fiscal = {
        7: 1,
        8: 2,
        9: 3,
        10: 4,
        11: 5,
        12: 6,
        1: 7,
        2: 8,
        3: 9,
        4: 10,
        5: 11,
        6: 12,
    }
    return map_calendar_to_fiscal[calendar_month]
