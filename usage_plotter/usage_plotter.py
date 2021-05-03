import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import List, Literal, Optional, TypedDict, Union

import matplotlib.pyplot as plt
import pandas as pd
from pandas.core import generic  # noqa
from tqdm import tqdm

# E3SM Facets that are available in file/dataset id and directory format
REALMS = ["ocean", "atmos", "land", "sea-ice"]
DATA_TYPES = ["time-series", "climo", "model-output", "mapping", "restart"]
TIME_FREQUENCY = [
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
]

# Unavailable
CAMPAIGNS = ["BGC-v1", "Cryosphere-v1", "DECK-v1", "HighResMIP-v1"]
SCIENCE_DRIVERS = ["Biogeochemical Cycle", "Cryosphere", "Water Cycle"]

# Type annotations
Project = Literal["E3SM", "E3SM in CMIP6"]
LogLine = TypedDict(
    "LogLine",
    {
        "log_line": str,
        "date": pd.Timestamp,
        "year": Optional[str],
        "month": Optional[str],
        "requester_ip": str,
        "path": str,
        "dataset_id": Optional[str],
        "file_id": Optional[str],
        "access_type": str,
        "status_code": str,
        "bytes": str,
        "mb": float,
        "project": Project,
        "realm": Optional[str],
        "data_type": Optional[str],
        "science_driver": Optional[str],
        "campaign": Optional[str],
    },
)


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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root", help="path to directory full of access logs for ESGF datasets"
    )
    return parser.parse_args()


def parse_logs(logs_dir: str) -> List[LogLine]:
    log_lines = []
    for log in tqdm(get_logs(logs_dir)):
        for line in filter_log_lines(log):
            parsed_line = parse_log_line(line)
            log_lines.append(parsed_line)
    return log_lines


def get_logs(path: str):
    """Fetch Apache logs from a path using a generator.

    :param path: [description]
    :type path: str
    :yield: [description]
    :rtype: [type]
    """
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            yield str(Path(root, file).absolute())


def filter_log_lines(path: str):
    """Filter log lines using a generator.

    Refer to README.md for the typical directory and dataset id structures.

    :param path: [description]
    :type path: str
    :yield: [description]
    :rtype: [type]
    """
    with open(path, "r") as instream:
        while (line := instream.readline()) :
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
        "year": None,
        "month": None,
        "requester_ip": attrs[0],
        "path": path,
        "dataset_id": None,
        "file_id": None,
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

    # None values are filled using helper functions.
    parsed_line = parse_log_timestamp(parsed_line, raw_timestamp=attrs[3])
    parsed_line = parse_log_path(parsed_line)
    return parsed_line


def parse_log_timestamp(log_line: LogLine, raw_timestamp: str) -> LogLine:
    """Parse a string timestamp for specific datetime values.


    :param log_row: [description]
    :type log_row: Dict[str, Any]
    :param raw_timestamp: Raw timestamp from Apache log
    Example: "[15/Jul/2019:03:18:49 -0700]"
    :type raw_timestamp: str
    :return: [description]
    :rtype: Dict[str, Any]
    """
    timestamp = raw_timestamp[raw_timestamp.find("[") + 1 : raw_timestamp.find(":")]

    log_line["date"] = datetime.strptime(timestamp, "%d/%b/%Y").date()
    log_line["year"] = log_line["date"].year
    log_line["month"] = log_line["date"].month
    return log_line


def parse_log_path(log_row):
    """Parses the full path for the dataset and file ids.

    :param log_row: [description]
    :type log_row: [type]
    :return: [description]
    :rtype: [type]
    """
    try:
        idx = log_row["path"].index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        # This usually means an HTTP 302/404 request (incorrect path)
        idx = None

    log_row["dataset_id"] = ".".join(log_row["path"][idx:].split("/")[:-1])
    log_row["file_id"] = log_row["path"].split("/")[-1]

    facets = log_row["dataset_id"].split(".")
    log_row.update(
        {
            "realm": parse_id_for_facets(facets, REALMS),
            "data_type": parse_id_for_facets(facets, DATA_TYPES),
            "time_frequency": parse_id_for_facets(facets, TIME_FREQUENCY),
            "science_driver": parse_id_for_facets(facets, SCIENCE_DRIVERS),
            "campaign": parse_id_for_facets(facets, CAMPAIGNS),
        }
    )

    return log_row


def parse_id_for_facets(
    file_facets: List[str],
    options: List[str]
    # TODO: Refactor this function
) -> Optional[str]:
    """Extracts facets from a dataset id.

    :param file_facets: [description]
    :type file_facets: List[str]
    :param options: [description]
    :type options: List[str]
    :return: [description]
    :rtype: Optional[str]
    """
    facet = None
    for option in options:
        if option in file_facets:
            facet = option

    return facet


def plot_qt_report(
    df: pd.DataFrame,
    project: Project,
    fiscal_year: Literal["19", "20", "21"] = "21",
):
    """Plot quarterly report for total data accessed and total requests.

    :param df: DataFrame containing quarterly report.
    :type df: pd.DataFrame
    :param project: The related project
    :type project: str
    :param fiscal_year: The fiscal year to plot, defaults to "21"
    :type fiscal_year: Literal["19", "20", "21"], optional
    """
    df_fiscal_year = df.loc[df["fiscal_year"] == fiscal_year]

    generic_plotter(
        df=df_fiscal_year,
        title=f"{project} FY {fiscal_year} Total Data Access ",
        x="quarter",
        xlabel="Quarter",
        y=["gb"],
        ylabel="Data (GB)",
        round_label=True,
    )

    generic_plotter(
        df=df_fiscal_year,
        title=f"{project} FY {fiscal_year} Total Requests ",
        x="quarter",
        xlabel="Quarter",
        y=["requests"],
        ylabel="Requests",
    )

    # fig = plot_data.get_figure()
    # fig.savefig(f"e3sm_requests_by_month_{year}", dpi=fig.dpi, facecolor="w")


def generic_plotter(
    df: pd.DataFrame,
    title: str,
    x: str,
    xlabel: str,
    y: List[str],
    ylabel: str,
    round_label: bool = False,
):
    plot = df.plot(
        title=title,
        kind="bar",
        x=x,
        y=y,
        legend=None,
        rot=0,
    )
    plot.set(xlabel=xlabel, ylabel=ylabel)

    for p in plot.patches:
        y_label = p.get_height()
        if round_label:
            y_label = "%.2f" % p.get_height()

        plot.annotate(
            y_label,
            (p.get_x() + p.get_width() / 2.0, p.get_height()),
            ha="center",
            va="center",
            xytext=(0, 3.5),
            textcoords="offset points",
        )

    plt.show()


def generate_qt_report(df: pd.DataFrame) -> pd.DataFrame:
    """Generates the quarterly report for total data accessed and requests.

    :param df: DataFrame containing monthly report.
    :type df: pd.DataFrame
    :return: DataFrame containing quarterly report.
    :rtype: pd.DataFrame
    """
    # Total data accessed on a monthly basis (only successful requests)
    df_data_by_mon = df.copy()
    df_data_by_mon = df_data_by_mon[df_data_by_mon.status_code.str.contains("200|206")]
    df_data_by_mon = (
        df_data_by_mon.groupby(by=["month_year", "status_code"])
        .agg({"mb": "sum"})
        .reset_index()
    )
    df_data_by_mon["gb"] = df_data_by_mon.mb.div(1024)

    # Total requests on a monthly basis
    df_req_by_mon = df.copy()
    df_req_by_mon = df_req_by_mon.value_counts(
        subset=["month_year", "status_code"]
    ).reset_index(name="requests")

    # Total data accessed and requests on a quarterly basis
    df_data_by_qt = group_by_quarter(df_data_by_mon)
    df_req_by_qt = group_by_quarter(df_req_by_mon)

    # Generate final quarterly report
    merge_cols = ["fy_quarter", "fiscal_year", "quarter", "start_date", "end_date"]
    df_qt_report = pd.merge(df_data_by_qt, df_req_by_qt, on=merge_cols, how="inner")

    # Reorder columns for printing output
    df_qt_report = df_qt_report[[*merge_cols, "gb", "requests"]]
    return df_qt_report


def group_by_quarter(df: pd.DataFrame) -> pd.DataFrame:
    """Groups a pandas DataFrame by E3SM quarters.

    :param df: DataFrame containing monthly report.
    :type df: pd.DataFrame
    :return: DataFrame containing quarterly report.
    :rtype: pd.DataFrame

    # TODO: Confirm resampling quarter start month, it is based on IG FY
    """
    # Set index to month_year in order to resample on quarters
    df.set_index("month_year", inplace=True)

    df_gb_qt: pd.DataFrame = df.resample("Q-JUN", convention="end").sum().reset_index()
    df_gb_qt.rename({"month_year": "fy_quarter"}, axis=1, inplace=True)  # noqa
    df_gb_qt["fiscal_year"] = df_gb_qt.fy_quarter.dt.strftime("%f")
    df_gb_qt["quarter"] = df_gb_qt.fy_quarter.dt.strftime("%q")
    df_gb_qt["start_date"] = df_gb_qt.apply(
        lambda row: row.fy_quarter.start_time.date(), axis=1
    )
    df_gb_qt["end_date"] = df_gb_qt.apply(
        lambda row: row.fy_quarter.end_time.date(), axis=1
    )
    return df_gb_qt


if __name__ == "__main__":
    # Directory that contains the access logs
    logs_dir = "../access_logs"

    # Parse Apache access logs
    log_lines: List[LogLine] = parse_logs(logs_dir)

    # Generate dataframe from parsed log lines
    df = pd.DataFrame(log_lines)
    df["date"] = pd.to_datetime(df["date"])
    df["month_year"] = df["date"].dt.to_period("M")

    # E3SM quarterly report
    df_e3sm = df[df.project == "E3SM"]
    df_e3sm_qt_report = generate_qt_report(df_e3sm)

    # E3SM in CMIP6 quarterly report
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]
    df_e3sm_cmip6_qt_report = generate_qt_report(df_e3sm_cmip6)

    # Plot results
    plot_qt_report(df_e3sm_qt_report, project="E3SM", fiscal_year="20")
    plot_qt_report(df_e3sm_cmip6_qt_report, project="E3SM in CMIP6", fiscal_year="20")
