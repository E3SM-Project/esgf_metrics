"""Parse module for parsing ESGF Apache access logs"""
import os
from pathlib import Path
from typing import DefaultDict, List, Optional, TypedDict

import pandas as pd
from tqdm import tqdm

from esgf_metrics.database.settings import engine
from esgf_metrics.facets import AVAILABLE_FACETS, Project
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.settings import LOGS_DIR, OUTPUT_DIR
from esgf_metrics.utils import _cast_period_cols_to_str, bytes_to

logger = setup_custom_logger(__name__)


LogLine = TypedDict(
    "LogLine",
    {
        "log_path": str,
        "log_line": str,
        # Request information
        "ip_address": str,
        "date": pd.Timestamp,
        "year_month": Optional[str],
        "year": Optional[int],
        "month": Optional[int],
        "access_type": str,
        "status_code": str,
        "bytes": str,
        "megabytes": float,
        # Dataset information.
        "dataset_path": str,
        "dataset_id": str,
        "project": Project,
        "file_id": str,
        "realm": Optional[str],
        "data_type": Optional[str],
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

    FiscalFacetMetrics = DefaultDict[
        Project,
        DefaultDict[str, pd.DataFrame],
    ]

    def __init__(self, debug_mode=False) -> None:
        # Path to the root directory of the access logs from ESGF nodes
        # (esgf-data1, esgf-data3, and esgf-data4). Access logs must be
        # stored in sub-directories labeled with the name of the node.
        self.log_dir = LOGS_DIR

        # Directory to store outputs including plots and validation reports.
        self.output_dir = OUTPUT_DIR

        # Set to True to perform parsing and plotting on a select number of
        # files. This is useful for code development or debugging.
        self.debug_mode = debug_mode

        # DataFrame for storing log file metadata.
        self.df_log_file: Optional[pd.DataFrame] = self._parse_log_files()

        # DataFrame for storing parsed log lines from the available log files.
        self.df_log_request: Optional[pd.DataFrame] = self._parse_log_requests()

    def to_sql(self):
        """
        Inserts `self.df_log_file` and `self.df_log_request` to the `log_file`
        and `log_request` SQL tables respectively.
        """
        logger.info("Casting Period columns to string for database support.")
        df_log_file = _cast_period_cols_to_str(self.df_log_file)
        df_log_request = _cast_period_cols_to_str(self.df_log_request)

        logger.info(f"Inserting {df_log_file.shape[0]} `log_file` objects.")
        df_log_file.to_sql("log_file", con=engine, if_exists="append", index=False)
        df_log_file_cur = pd.read_sql(
            "SELECT id AS log_id, path AS log_path FROM log_file", con=engine
        )

        logger.info("Updating 'log_id' foreign key for `log_request` objects")
        df_log_request = df_log_request.merge(
            df_log_file_cur,
            on=["log_path"],
            how="left",
        )
        df_log_request = df_log_request.drop(["log_path"], axis=1)

        logger.info(f"Inserting {df_log_request.shape[0]} `log_request` objects.")
        df_log_request.to_sql(
            "log_request", con=engine, index=False, if_exists="append", chunksize=10000
        )

        logger.info("Database insertion completed")

    def _parse_log_files(self) -> Optional[pd.DataFrame]:
        """Parses metadata from the access logs.

        The parsed metadata include absolute path, server node, filename, and
        date components.

        Raises
        ------
        ValueError
            If the node substring was not found in the path of >=1 logs.

        Raises
        ------
        Optional[pd.DataFrame]
            A DataFrame of parsed log files.
        """
        # Absolute paths of each log file located under self.log_dir.
        paths = self._get_abs_paths()

        if len(paths) == 0:
            logger.info(f"All logs in `{self.log_dir}` have already been parsed.")
            return None
        else:
            logger.info(f"New logs found: {paths}")

        df = pd.DataFrame({"path": paths})

        # Parse the "path" column for additional metadata.
        df["node"] = df.path.str.extract(r"(aims3|esgf-data\d{1})")
        if df.node.isna().any():
            raise ValueError(
                "The node substring was not found in one or more of the access log "
                "absolute paths. Make sure that access logs are stored in "
                "sub-directories by node (`/aims3`, `/esgf-data1`, `/esgf-data3`, "
                "`/esgf-data4`)."
            )

        # Extract the filename from the path.
        df["filename"] = df.path.str.extract(r"(access_log-\d{8}$)")
        df = df[df["filename"].notna()]

        # Get the date information.
        df["date"] = df.filename.str.extract(r"(\d{8}$)")
        df["date"] = pd.to_datetime(df.date, format="%Y%m%d")
        df["year_month"] = df["date"].dt.to_period("M")
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        # Get the fiscal date information.
        df = self._add_fiscal_date_cols(df)

        return df

    def _get_abs_paths(self) -> List[str]:
        """Gets the absolute paths for each log file.

        This method also walks through sub-directories to find access logs.

        Access logs should be stored by node:

          - `/aims3`
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
        paths: List[str] = []

        for root, dirs, files in os.walk(self.log_dir):
            if not files:
                continue
            if dirs:
                continue
            for filename in files:
                if "access_log-" in filename:
                    paths.append(str(Path(root, filename).absolute()))

        if not paths:
            raise IndexError(
                f"No logs were found at `{self.log_dir}`. Check that you set the "
                "correct root access logs directory in `.env`."
            )

        if self.debug_mode:
            paths = paths[0:2]

        paths = self._keep_unparsed_logs(paths)
        paths = sorted(paths)

        return paths

    def _keep_unparsed_logs(self, paths: List[str]) -> List[str]:
        """Keep unparsed log paths in the list of logs to parse.

        Parameters
        ----------
        paths : List[str]
            The list of absolute log paths.

        Returns
        -------
        List[str]
            The list of absolute log paths.
        """
        parsed_log_paths = pd.read_sql_query(
            "SELECT path from log_file", con=engine
        ).path
        unparsed_log_paths = list(set(paths).difference(set(parsed_log_paths)))

        return unparsed_log_paths

    def _parse_log_requests(self) -> Optional[pd.DataFrame]:
        """Parse the log files for HTTP request information.

        Parameters
        -------
        to_csv : bool, optional
            Save the parsed log lines as a CSV, by default False.

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing parsed log lines.
        """
        if self.df_log_file is None:
            return None

        log_paths = self.df_log_file.path
        log_lines: List[LogLine] = []

        logger.info(
            f"Parsing lines for {len(log_paths)} access logs in `{self.log_dir}`. "
            "into dictionary objects."
        )
        for path in tqdm(log_paths):
            for line in self._filter_log_lines(path):
                parsed_line = self._parse_log_line(path, line)
                log_lines.append(parsed_line)

        logger.info("Converting dictionary objects into a DataFrame")
        df = pd.DataFrame(log_lines)
        df = self._keep_reqs_with_data(df)
        df = self._extract_dates(df)

        return df

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
                    # Only get successful requests with data transferred.
                    and (" 200 " in line or " 206 " in line)
                ):
                    yield line

    def _parse_log_line(self, log_path: str, line: str) -> LogLine:
        """Parses a log line for HTTP request and facet information.

        Refer to README.md for directory and dataset id templates.

        Parameters
        ---------
        log_path : str
            The path to the log related to the log line.
        line : str
            The raw log line.

        Returns
        -------
        LogLine
            A dictionary containing parsed information from the raw log line.
        """
        attrs = line.split()
        dataset_path = attrs[6].replace("%2F", "/")

        log_line: LogLine = {
            "log_path": log_path,
            "log_line": line,
            # Request information
            "ip_address": attrs[0],
            "date": None,
            "year_month": None,
            "year": None,
            "month": None,
            "access_type": attrs[11],
            "status_code": attrs[8],
            "dataset_path": dataset_path,
            "bytes": attrs[9],
            "megabytes": bytes_to(attrs[9], "mb") if "-" not in attrs[9] else 0,
            # Dataset information
            "dataset_id": "",
            "file_id": "",
            "project": "E3SM Native"
            if "/E3SM-Project" not in dataset_path
            else "E3SM CMIP6",
            "realm": None,
            "data_type": None,
            "variable_id": None,
            "time_frequency": None,
            "activity": None,
        }

        log_line = self._extract_ids_from_dataset_path(log_line)
        log_line = self._extract_facets_from_dataset_id(log_line)
        return log_line

    def _extract_ids_from_dataset_path(self, log_line: LogLine) -> LogLine:
        """Parses the dataset id found in the log line for facets.

        Example `dataset_path`:
            "/thredds/fileServer/user_pub_work/CMIP6/CMIP/E3SM-Project/E3SM-1-0/historical/r1i1p1f1/Amon/wap/gr/v20191220/wap_Amon_E3SM-1-0_historical_r1i1p1f1_gr_190001-192412.nc"

        Example `dataset_id from `dataset_path`:
            "CMIP6.CMIP.E3SM-Project.E3SM-1-0.historical.r1i1p1f1.Amon.wap.gr.v20191220"

        Example `file_id` from `dataset_path`:
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
        # Get the path following "/user_pub_work" and split it by "/".
        ds_path = log_line["dataset_path"].split("/user_pub_work/")[-1].split("/")

        # Parse the split path into "dataset_id" and "file_id".
        log_line["dataset_id"] = ".".join(ds_path[:-1])
        log_line["file_id"] = ds_path[-1]
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
        project_facets = AVAILABLE_FACETS[log_line["project"]]
        for facet, options in project_facets.items():
            facet_value = None
            for option in options:
                if option in dataset_id:
                    facet_value = option
                    break

            log_line[facet] = facet_value  # type: ignore

        return log_line

    def _keep_reqs_with_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep requests with data transferred.

        Sometimes 206 HTTP requests might not have any data transferred, so the
        bytes are represented with "-".

        requests.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame of successful HTTP requests.

        Returns
        -------
        pd.DataFrame
            DataFrame of successful HTTP requests with data.
        """
        df = df[df["bytes"] != "-"]

        return df

    def _extract_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extracts the HTTP request date components.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame of successful HTTP requests.

        Returns
        -------
        pd.DataFrame
        """
        df["date"] = df.log_line.str.extract(r"(?<=\[)(\d{2}\/\w{3}\/\d{4})(?=:)")
        df["date"] = pd.to_datetime(df.date, format="%d/%b/%Y")

        df["year_month"] = df["date"].dt.to_period("M")
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        return df

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
            The monthly metrics DataFrame with fiscal date columns.
        """
        df["fiscal_year_quarter"] = df.apply(
            lambda row: row.year_month.asfreq("Q-JUN"), axis=1
        )
        df["fiscal_year"] = df.fiscal_year_quarter.dt.strftime("%F").astype("int")
        df["fiscal_month"] = df.apply(lambda row: E3SM_CY_TO_FY_MAP[row.month], axis=1)

        return df
