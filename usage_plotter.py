import argparse
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ipwhois import IPWhois, exceptions
from tqdm import tqdm

REALMS = ["ocean", "atmos", "land", "sea-ice"]
DATA_TYPES = ["time-series", "climo", "model-output", "mapping", "restart"]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "root", help="path to directory full of access logs for ESGF datasets"
    )
    return parser.parse_args()


def get_logs(path: str):
    """Fetch the logs from the specified path
    :param path:
    :return:
    """
    for root, dirs, files in os.walk(path):
        if not files:
            continue
        if dirs:
            continue
        for file in files:
            yield str(Path(root, file).absolute())


def filter_lines(path: str):
    """Filter lines for parameters specific to a project.

    Currently, it only supports E3SM.
    :param path:
    :return:
    """
    with open(path, "r") as instream:
        while (line := instream.readline()) :
            if (
                "E3SM" in line
                and "CMIP6" not in line
                and "xml" not in line
                and "ico" not in line
                and "cmip6_variables" not in line
                and "html" not in line
                and "catalog" not in line
                and "aggregation" not in line
            ):
                yield line


def parse_dataset_id(dataset_id: str):
    """
    e.g., 'E3SM.1_0.historical.1deg_atm_60-30km_ocean.sea-ice.180x360.model-output.mon.ens1.v1'
    :param dataset_id:
    :return:
    """

    facets = dataset_id.split(".")

    try:
        realm = facets[4]
    except IndexError:
        realm = None

    try:
        data_type = facets[4]
    except IndexError:
        data_type = None

    return realm, data_type


def parse_timestamp(timestamp: str, log_row: Dict[str, Any]) -> Dict[str, Any]:
    """Extract string date ('30/Aug/2019') and convert to date object ('2019-08-30').

    :param timestamp:
    :param log_row:
    :return:
    """

    timestamp_str = timestamp[timestamp.find("[") + 1 : timestamp.find(":")]
    log_row["date"] = datetime.strptime(
        timestamp_str,
        "%d/%b/%Y",
    ).date()  # type: datetime.date
    log_row["year"] = log_row["date"].year
    log_row["month"] = log_row["date"].month

    return log_row


def identify_requester(ip: str) -> None:
    # try:
    #     log_row['requester_id'] = IPWhois.lookup_rdap(IPWhois(log_row.get('requester_ip')))
    # except exceptions.IPDefinedError:
    #     log_row['requester_id'] = None
    pass


def parse_log_line(log_line: str):
    """
    :param log_line:
    :return:
    """
    attrs = log_line.split()

    log_row = {}  # type: Dict[str, Any]
    log_row = parse_timestamp(attrs[3], log_row)
    log_row["requester_ip"] = attrs[0]
    log_row["requester_id"] = identify_requester(log_row.get("requester_ip")) or None

    log_row["request_method"] = attrs[5]
    log_row["full_path"] = attrs[6]

    try:
        idx = log_row.get("full_path").index("user_pub_work") + len("user_pub_work") + 1
    except ValueError:
        idx = None
        print("ERROR: " + log_row.get("full_path"))

    log_row["dataset_id"] = ".".join(log_row.get("full_path")[idx:].split("/")[:-1])
    log_row["realm"], log_row["data_type"] = parse_dataset_id(log_row["dataset_id"])

    return log_row


def plot_requests_by_month(df: pd.DataFrame, project: str) -> pd.DataFrame:
    df_agg = df.groupby(by=["year", "month"]).size().reset_index(name="count")
    years = df_agg["year"].unique()

    for year in years:
        df_agg_yr = df_agg.loc[df_agg["year"] == year]
        plot = df_agg_yr.plot(
            title=f"{project} Requests by Month ({year})",
            kind="bar",
            x="month",
            y=["count"],
            legend=None,
        )
        plot.set(xlabel="Month", ylabel="Requests")
        plt.show()
        fig = plot.get_figure()
        fig.savefig(f"e3sm_requests_by_month_{year}", dpi=fig.dpi, facecolor="w")

    return df_agg


def main():
    # TODO: Include command line args: root_dir, project
    # parsed_args = parse_args()
    # root_dir = parsed_args.root if parsed_args.root else 'access_logs'
    root_dir = "access_logs"
    columns = [
        "date",
        "year",
        "month",
        "requester_ip",
        "requester_id",
        "request_method",
        "full_path",
        "dataset_id",
        "realm",
        "data_type",
    ]

    requests = []
    for log in tqdm(get_logs(root_dir)):
        for line in filter_lines(log):
            row = parse_log_line(line)
            requests.append(row)

    df_requests = pd.DataFrame(requests, columns=columns)

    # Aggregation plots
    df_agg_by_month = plot_requests_by_month(df_requests, project="E3SM")  # type: plt


if __name__ == "__main__":
    main()
