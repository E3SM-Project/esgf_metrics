"""Parse module for parsing ESGF Apache access logs"""
from collections import defaultdict
from typing import DefaultDict, Optional

import pandas as pd

from esgf_metrics.database.settings import engine
from esgf_metrics.facets import AVAILABLE_FACETS, ProjectTitle
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.parse import E3SM_CY_TO_FY_MAP

logger = setup_custom_logger(__name__)


class MetricsGenerator:
    """
    A class representing LogParser, which is used for parsing ESGF Apache
    access logs for generating metrics.
    """

    FiscalFacetMetrics = DefaultDict[
        ProjectTitle,
        DefaultDict[str, pd.DataFrame],
    ]

    def __init__(self) -> None:
        # DataFrame of fiscal monthly metrics by project.
        self.df_monthly_metrics: pd.DataFrame = pd.DataFrame()
        self.df_fiscal_metrics: pd.DataFrame = pd.DataFrame()

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
        self.fiscal_facet_metrics: MetricsGenerator.FiscalFacetMetrics = defaultdict(
            lambda: defaultdict(pd.DataFrame)
        )

    def to_sql(self):
        pass

    def get_metrics(self):
        """Generates metrics for successful requests by project and facets.
        Raises
        ------
        ValueError
            If logs have not been parsed yet.
        """
        logger.info("Generating monthly metrics.")
        self.df_monthly_metrics = self._get_monthly_metrics()
        self.df_fiscal_metrics = self._get_fiscal_metrics(
            self.df_monthly_metrics.copy()
        )

        for project, facets in AVAILABLE_FACETS.items():
            for facet in facets.keys():
                # TODO: We might want to store this in the dictionary for plotting.
                df_monthly_metrics = self._get_monthly_metrics(facet)
                df_fy_metrics = self._get_fiscal_metrics(
                    df_monthly_metrics.copy(), facet
                )
                df_fy_metrics.loc[df_fy_metrics.project == project]
                self.fiscal_facet_metrics[project][facet] = df_fy_metrics

    def _get_monthly_metrics(self, facet: Optional[str] = None) -> pd.DataFrame:
        df = pd.read_sql(
            """
        SELECT lf.year,
            lf.month,
            lr.project,
            COUNT(*) as               sum_requests,
            SUM(lr.megabytes / 1024) sum_gigabytes
        FROM log_request lr
                LEFT JOIN log_file lf ON lr.log_id = lf.id
        GROUP BY lr.project, lf.year, lf.month;""",
            con=engine,
        )

        df[["cumsum_requests", "cumsum_gigabytes"]] = df.groupby(
            ["year", "project"]
        ).agg({"sum_requests": "cumsum", "sum_gigabytes": "cumsum"})

        return df

    def _get_fiscal_metrics(
        self, df_monthly: pd.DataFrame, facet: Optional[str] = None
    ):
        """Generates monthly metrics by project and facet (optionally).
        Parameters
        ----------
        df_monthly : pd.DataFrame
            The monthly metrics DataFrame.
        facet : Optional[str], optional
            The facet column used for grouping in addition to the main
            aggregation columns, by default None.
        Returns
        -------
        pd.DataFrame
            A DataFrame of monthly metrics.
        """
        # Get fiscal monthly cumulative sums for requests and size of data
        # download.
        df = self._add_fiscal_date_cols(df_monthly)
        df = self._get_fiscal_monthly_cumsums(df, facet)
        df = self._reorder_columns(df, facet)

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

    def _get_fiscal_monthly_cumsums(
        self, df: pd.DataFrame, facet: Optional[str]
    ) -> pd.DataFrame:
        """Adds cumulative sum columns for requests and data downloads (GB).

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
        if facet is not None:
            gb_cols.append(facet)

        df["cumulative_requests"] = df.groupby(gb_cols)["total_requests"].cumsum()
        df["cumulative_get_requests"] = df.groupby(gb_cols)[
            "total_get_requests"
        ].cumsum()
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
            The facet column used for grouping, in addition to the main
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
            "year_month",
            "year",
            "month",
            "total_requests",
            "cumulative_requests",
            "total_get_requests",
            "cumulative_get_requests",
            "total_gb",
            "cumulative_gb",
        ]
        if facet is not None:
            columns.insert(1, facet)

        return df[columns]
