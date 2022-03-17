"""Parse module for parsing ESGF Apache access logs"""
import os
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, List, Optional, TypedDict

import pandas as pd
from tqdm import tqdm

from esgf_metrics.facets import AVAILABLE_FACETS, Project
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.settings import DEBUG_MODE, LOGS_DIR, OUTPUT_DIR
from esgf_metrics.utils import bytes_to

logger = setup_custom_logger(__name__)

LogLine = TypedDict(
    "LogLine",
    {
        "log_line": str,
        "date": pd.Timestamp,
        "calendar_year": Optional[int],
        "calendar_month": Optional[int],
        "requester_ip": str,
        "dataset_path": str,
        "dataset_id": str,
        "file_id": str,
        "access_type": str,
        "status_code": str,
        "bytes": str,
        "mb": float,
        "project": Project,
        "realm": Optional[str],
        "data_type": Optional[str],
        # facets
        "variable_id": Optional[str],
        "time_frequency": Optional[str],
        "activity": Optional[str],
    },
)


# Maps calendar month to fiscal month based on E3SM fiscal year.
E3SM_CY_TO_FY_MAP = {
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


class LogParser:
    """
    A class representing LogParser, which is used for parsing ESGF Apache
    access logs for generating metrics.
    """

    def __init__(self) -> None:
        # Path to the root directory of the access logs from ESGF nodes
        # (esgf-data1, esgf-data3, and esgf-data4). Access logs must be
        # stored in sub-directories labeled with the name of the node.
        self.log_dir = LOGS_DIR

        # Directory to store outputs including plots and validation reports.
        self.output_dir = OUTPUT_DIR

        # Absolute paths of each log file located under self.log_dir.
        self.log_paths = self._get_log_abs_paths()

        # Set to True to perform parsing and plotting on a select number of
        # files. This is useful for code development or debugging.
        # TODO: Maybe this should be an argparse because it is tedious to
        # type it in during runtime.
        self.debug_mode = DEBUG_MODE
        if self.debug_mode:
            num_logs = int(input("How many access logs do you want to parse? "))
            self.log_paths = self.log_paths[0:num_logs]

        # Dictionary with the parent key being the fiscal year and the value
        # being a sub-dictionary of report types. The key of the sub-dictionary
        # is the report type and the value is a DataFrame storing the report.
        """
        Example:
        {
            "FY2021": {
                "logs": pd.DataFrame(),
                "log_count_by_group": pd.DataFrame()
            }
            "FY2022": {
                "logs": pd.DataFrame(),
                "log_count_by_group": pd.DataFrame()
            }
        }
        """
        self.validation_reports: DefaultDict[
            str, DefaultDict[str, pd.DataFrame]
        ] = defaultdict(lambda: defaultdict(pd.DataFrame))

        # DataFrame for storing parsed log lines of the available log files.
        self.logs: pd.DataFrame = pd.DataFrame()

        # DataFrame of cumulative sum metrics by project.
        self.metrics: pd.DataFrame = pd.DataFrame()

        # Dictionary with the key being the project and the value being
        # a sub-dictionary of facet reports. The key of the
        # sub-dictionary is the facet name and the value is a DataFrame storing
        # the report.
        """
        Example:
        {
            "E3SM and E3SM CMIP6": {"realm": pd.DataFrame()},
            "E3SM": {"time_frequency": pd.DataFrame()},
            "E3SM CMIP6": {"activity": pd.DataFrame(), "variable_id": pd.DataFrame()},
        }
        """
        self.metrics_by_facet: DefaultDict[
            Project, DefaultDict[str, pd.DataFrame]
        ] = defaultdict(lambda: defaultdict(pd.DataFrame))

    def validate_logs(self, to_csv: bool = True):
        """Performs validation checks on the access logs and generates reports.

        Checks include:
          1. Ensure that logs are stored by node.

        CSV reports include:
          1. Access log filenames with labels for node, calendar date info,
            and fiscal date info.
          2. Count of available access logs by fiscal year and node.
            - Helps determine if there are gaps in access logs.
            - Gaps may indicate httpd wasn’t running on a node(s) and/or some
              logs weren’t backed up properly at that time.

        Parameters
        ----------
        to_csv : bool, optional
           Save the output as CSV, by default True.

        Raises
        ------
        ValueError
            If the node substring was not found in one or more access logs.
        """
        logger.info("Performing validation on access logs.")
        df = pd.DataFrame({"path": self.log_paths})
        df["node"] = df.path.str.extract(r"(esgf-data\d{1})")

        if df.node.isna().any():
            raise ValueError(
                "The node substring was not found in one or more of the access log "
                "absolute paths. Make sure that access logs are stored in "
                "sub-directories by node (`/esgf-data1`, `/esgf-data3`, `/esgf-data4`)."
            )

        df["filename"] = df.path.str.extract(r"(access_log-\d{8}$)")
        df = df[df["filename"].notna()]

        df["date"] = df.filename.str.extract(r"(\d{8}$)")
        df["date"] = pd.to_datetime(df.date, format="%Y%m%d")
        df["calendar_year_month"] = df["date"].dt.to_period("M")
        df["calendar_year"] = df["date"].dt.year
        df["calendar_month"] = df["date"].dt.month
        df = self._add_fiscal_date_cols(df)

        # Drop unnecessary columns for a cleaner output.
        df = df.drop(columns=["path", "calendar_year", "calendar_month"])

        logger.info(
            "Generating reports by fiscal year and node. Reports are stored in "
            f"{self.output_dir}."
        )
        logger.info(
            "Check the reports for any gaps in access logs. Gaps might "
            "indicate that httpd wasn't running on a node(s) and/or some logs "
            "weren't backed up properly at that time."
        )
        fiscal_years = df.fiscal_year.unique()
        for year in fiscal_years:
            df_fy_logs = df.query(f"fiscal_year == {year}").reset_index(drop=True)
            df_fy_logs = df_fy_logs.sort_values(by="date")
            df_fy_log_count = self._log_count_by_group(df_fy_logs)

            self.validation_reports[f"FY{year}"]["logs"] = df_fy_logs
            self.validation_reports[f"FY{year}"]["log_counts"] = df_fy_log_count

            if to_csv:
                df_fy_logs.to_csv(f"{self.output_dir}/fy_{year}_logs.csv", index=False)
                df_fy_log_count.to_csv(
                    f"{self.output_dir}/fy_{year}_log_count_by_group.csv", index=False
                )

    def parse_logs(self, to_csv: bool = False) -> pd.DataFrame:
        """Parse the ESGF Apache access logs in the specific path.

        Parameters
        -------
        to_csv : bool, optional
            Save the parsed log lines as a CSV, by default False.

        Returns
        -------
        pd.DataFrame
            DataFrame containing parsed log lines.
        """
        logger.info(f"Parsing access logs stored in `{self.log_dir}.")
        log_lines: List[LogLine] = []
        for path in tqdm(self.log_paths):
            for raw_line in self._filter_log_lines(path):
                try:
                    parsed_line = self._parse_log_line(raw_line)
                    log_lines.append(parsed_line)
                except IndexError:
                    # Ignore 414 URI too long requets
                    continue

        df = pd.DataFrame(log_lines)
        df = self._extract_dates(df)

        # TODO: The CSV will be extremely large.
        # Maybe we should save this information in a postgres database?
        if to_csv:
            df.to_csv(f"{self.output_dir}/parsed_access_log_lines.csv")

        self.logs = df

    def generate_metrics(self):
        """Generates aggregated metrics by project and facets.

        """
        if self.logs is None:
            raise ValueError(
                "Logs have not been parsed yet, call `parse_logs()` first."
            )
        logger.info(
            "Generating cumulative sums of requests data downloaded by project and "
            "by facet."
        )
        self.metrics = self._generate_monthly_metrics()
        for project, facets in AVAILABLE_FACETS.items():
            for facet in facets.keys():
                df_metrics = self._generate_monthly_metrics(facet)
                df_metrics.loc[df_metrics.project == project]
                self.metrics_by_facet[project][facet] = df_metrics

    def _get_log_abs_paths(self) -> List[str]:
        """Gets the absolute paths for each log file.

        This method also walks through sub-directories to find access logs.

        The directory structure of the access logs should look like:

        `/access_logs`
          - `/esgf-data1`
          - `/esgf-data3`
          - `/esgf-data4`

        Returns
        -------
        List[str]
            List of absolute paths for the log files.

        Raises
        ------
        IndexError
            If logs were not found.
        """
        abs_log_paths: List[str] = []
        for root, dirs, files in os.walk(self.log_dir):
            if not files:
                continue
            if dirs:
                continue
            for file in files:
                abs_log_paths.append(str(Path(root, file).absolute()))

        if not abs_log_paths:
            raise IndexError(
                "No logs were found. Check that you set the correct root access logs "
                "directory in `.env`."
            )
        abs_log_paths.sort()
        return abs_log_paths

    def _log_count_by_group(self, df):
        df_gb = (
            df.groupby(["calendar_year_month", "node"])
            .agg(total_logs=("filename", "count"), filenames=("filename", ", ".join))
            .reset_index()
        )
        df_gb["complete_logs"] = df_gb["total_logs"] > 3
        return df_gb

    def _filter_log_lines(self, log_path: str):
        """Filters the log lines for requests specific to E3SM data.

        It uses a generator to loop through each file to avoid storing all of
        them in memory.

        Parameters
        ----------
        log_path : str
            Absolute path to a log file.

        Yields
        ------
        str
            The log line specific to E3SM data.
        """
        with open(log_path, "r") as instream:
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

    def _parse_log_line(self, raw_line: str) -> LogLine:
        """Parses a log line for HTTP request and facet information.

        Refer to README.md for directory and dataset id templates.

        Parameters
        ---------
        raw_line : str
            The raw line

        Returns
        -------
        LogLine
            A dictionary containing parsed information from the raw log line.
        """
        attrs = raw_line.split()
        dataset_path = attrs[6].replace("%2F", "/")

        log_line: LogLine = {
            "log_line": raw_line,
            "date": None,
            "calendar_year": None,
            "calendar_month": None,
            "requester_ip": attrs[0],
            "dataset_path": dataset_path,
            "dataset_id": "",
            "file_id": "",
            "access_type": attrs[11],
            "status_code": attrs[8],
            "bytes": attrs[9],
            "mb": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
            "project": "E3SM" if "/E3SM-Project" not in dataset_path else "E3SM CMIP6",
            "realm": None,
            "data_type": None,
            # Facets
            "variable_id": None,
            "time_frequency": None,
            "activity": None,
        }

        log_line = self._extract_ids_from_dataset_path(log_line)
        log_line = self._extract_facets_from_dataset_id(log_line)
        return log_line

    def _extract_ids_from_dataset_path(self, log_line: LogLine) -> LogLine:
        """Parses the dataset id found in the log line for facets.

        Example dataset path:
            "CMIP6/CMIP/E3SM-Project/E3SM-1-0/historical/r1i1p1f1/Amon/wap/gr/v20191220/wap_Amon_E3SM-1-0_historical_r1i1p1f1_gr_190001-192412.nc"
        Example dataset id from path:
            "CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Amon.wap.gr.v20191220"
        Example file id from path:
            "wap_Amon_E3SM-1-0_historical_r1i1p1f1_gr_190001-192412.nc"

        Parameters
        ----------
        log_line : LogLine
            The log line.
        path_in_log_line : str
            The path found in the log line.

        Returns
        -------
        LogLine
            A dictionary containing parsed information from the raw log line.
        """
        ds_path = log_line["dataset_path"]

        try:
            start_index = ds_path.index("user_pub_work") + len("user_pub_work") + 1
        except ValueError:
            # Ignore HTTP 302/404 requests
            start_index = None

        split_path = ds_path[start_index:].split("/")
        log_line["dataset_id"] = ".".join(split_path[:-1])
        log_line["file_id"] = split_path[-1]
        return log_line

    def _extract_facets_from_dataset_id(self, log_line: LogLine) -> LogLine:
        """Parses the dataset id found in the log line for facets.

        Parameters
        ----------
        log_line : LogLine
            The log line.


        Returns
        -------
        LogLine
            A dictionary containing parsed information from the raw log line.
        """
        dataset_id = log_line["dataset_id"].split(".")

        # FIXME: This is suboptimal, it should be done after all log lines
        # are already in the DataFrame.
        for facets in AVAILABLE_FACETS.values():
            for facet, option in facets.items():
                facet_value = None
                if option in dataset_id:
                    facet_value = option
            log_line[facet] = facet_value  # type: ignore

        return log_line

    def _extract_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract the request dates from the log lines.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame of log lines.

        Returns
        -------
        pd.DataFrame
        """
        df["date"] = df.log_line.str.extract(r"(?<=\[)(\d{2}\/\w{3}\/\d{4})(?=:)")
        df["date"] = pd.to_datetime(df.date, format="%d/%b/%Y")

        df["calendar_year_month"] = df["date"].dt.to_period("M")
        df["calendar_year"] = df["date"].dt.year
        df["calendar_month"] = df["date"].dt.month

        return df

    def _generate_monthly_metrics(self, facet: Optional[str] = None):
        """Generates monthly metrics by project and facet (optionally).

        Parameters
        ----------
        facet : Optional[str], optional
            The facet column used for grouping in addition to the main
            aggregation columns, by default None.

        Returns
        -------
        pd.DataFrame
            A DataFrame of monthly metrics.
        """
        df_logs = self.logs.copy()
        agg_cols = ["project", "calendar_year_month", "calendar_year", "calendar_month"]

        if facet:
            agg_cols.insert(1, facet)
            df_logs[facet] = df_logs[facet].fillna(value="N/A")

        # Get total requests on a monthly basis
        df_req_by_mon = df_logs.value_counts(subset=agg_cols).reset_index(
            name="total_requests"
        )
        # Get total data downloaded on a monthly basis (only successful requests)
        # TODO: This should be filtered out in the initial creation of the df_logs dataframe
        df_data_by_mon = df_logs[df_logs.status_code.str.contains("200|206")]
        df_data_by_mon = (
            df_data_by_mon.groupby(by=agg_cols).agg({"mb": "sum"}).reset_index()
        )
        df_data_by_mon["total_gb"] = df_data_by_mon.mb.div(1024)

        # Monthly metrics with each month assigned an E3SM fiscal year label
        df_monthly = pd.merge(df_req_by_mon, df_data_by_mon, on=agg_cols)
        df_monthly = df_monthly.sort_values(by=agg_cols)
        df_monthly = self._add_fiscal_date_cols(df_monthly)

        # Add cumulative sum columns for requests and data access
        df_monthly = self._add_cumsum_cols(df_monthly, facet)
        df_monthly = self._reorder_columns(df_monthly, facet)
        return df_monthly

    def _add_fiscal_date_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds fiscal date columns based on the calendar date columns.

        This method resamples the monthly metrics DataFrame to extract the
        fiscal year, quarter, and month using the calendar year and month.

        Parameters
        ----------
        df : pd.DataFrame
            The monthly metrics DataFrame.

        Returns
        -------
        pd.DataFrame
            The monthly metrics DataFrame with fiscal year information.
        """
        # Get equivalent fiscal information from calendar dates
        df["fiscal_year_quarter"] = df.apply(
            lambda row: row.calendar_year_month.asfreq("Q-JUN"), axis=1
        )
        df["fiscal_year"] = df.fiscal_year_quarter.dt.strftime("%F").astype("int")
        df["fiscal_month"] = df.apply(
            lambda row: E3SM_CY_TO_FY_MAP[row.calendar_month], axis=1
        )
        df["fiscal_quarter"] = df.fiscal_year_quarter.dt.strftime("%q").astype("int")

        return df

    def _add_cumsum_cols(self, df: pd.DataFrame, facet: Optional[str]) -> pd.DataFrame:
        """Adds cumulative sum columns for requests and GB of data downloaded.

        Parameters
        ----------
        df_monthly : pd.DataFrame
            The DataFrame of monthly metrics
        facet : Optional[str]
            The facet column used for grouping in addition to the main
            aggregation columns.

        Returns
        -------
        pd.DataFrame
            The DataFrame of monthly metrics with cumulative sums.
        """
        gb_cols = ["project", "fiscal_year"]
        if facet:
            gb_cols.append(facet)

        df["cumulative_requests"] = df.groupby(gb_cols)["total_requests"].cumsum()
        df["cumulative_gb"] = df.groupby(gb_cols)["total_gb"].cumsum()

        return df

    def _reorder_columns(self, df, facet: Optional[str]):
        """Reorders columns for a DataFrame of aggregated metrics.

        In several class methods, DataFrame columns are appended to the end of
        the DataFrame, which makes the order of columns not succinct.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame of aggregated metrics with the original column order.
        facet : Optional[str]
            The facet column used for grouping in addition to the main
            aggregation columns.

        Returns
        -------
        pd.DataFrame
            A DataFrame of aggregated metrics with reordered columns.
        """
        columns = [
            "project",
            "fiscal_year_quarter",
            "fiscal_year",
            "fiscal_month",
            "fiscal_quarter",
            "calendar_year_month",
            "calendar_year",
            "calendar_month",
            "total_requests",
            "cumulative_requests",
            "total_gb",
            "cumulative_gb",
        ]
        if facet:
            columns.insert(1, facet)

        return df[columns]
