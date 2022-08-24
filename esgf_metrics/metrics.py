"""Parse module for parsing ESGF Apache access logs"""
from collections import defaultdict
from typing import DefaultDict, Optional

import pandas as pd

from esgf_metrics.database.settings import engine
from esgf_metrics.facets import Project
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.utils import _cast_period_cols_to_str

logger = setup_custom_logger(__name__)


class MetricsGenerator:
    """
    A class representing LogParser, which is used for parsing ESGF Apache
    access logs for generating metrics.
    """

    FiscalFacetMetrics = DefaultDict[
        Project,
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
        df_fiscal_metrics = _cast_period_cols_to_str(self.df_fiscal_metrics)
        df_monthly_metrics = _cast_period_cols_to_str(self.df_monthly_metrics)

        df_monthly_metrics.to_sql(
            "metrics_monthly", con=engine, if_exists="append", index=False
        )
        df_fiscal_metrics.to_sql(
            "metrics_fiscal", con=engine, if_exists="append", index=False
        )

    def get_metrics(self):
        """Generates metrics for successful requests by project and facets.
        Raises
        ------
        ValueError
            If logs have not been parsed yet.
        """
        self.df_monthly_metrics = self._get_monthly_metrics()
        self.df_fiscal_metrics = self._get_fiscal_metrics(
            self.df_monthly_metrics.copy()
        )

        # for project, facets in AVAILABLE_FACETS.items():
        #     for facet in facets.keys():
        #         # TODO: We might want to store this in the dictionary for plotting.
        #         df_monthly_metrics = self._get_monthly_metrics(facet)
        #         df_fy_metrics = self._get_fiscal_metrics(
        #             df_monthly_metrics.copy(), facet
        #         )
        #         df_fy_metrics.loc[df_fy_metrics.project == project]
        #         self.fiscal_facet_metrics[project][facet] = df_fy_metrics

    def _get_monthly_metrics(self, facet: Optional[str] = None) -> pd.DataFrame:
        logger.info("Generating monthly metrics by E3SM file output type.")
        df = pd.read_sql(
            """
            SELECT
                lf.year_month,
                lf.year,
                lf.month,
                lr.project,
                COUNT(*) as               sum_requests,
                SUM(lr.megabytes / 1024)  sum_gigabytes
            FROM log_request lr
                LEFT JOIN log_file lf ON lr.log_id = lf.id
            GROUP BY lr.project, lf.year_month, lf.year, lf.month;
            """,
            con=engine,
        )

        df[["cumsum_requests", "cumsum_gigabytes"]] = df.groupby("project").agg(
            {"sum_requests": "cumsum", "sum_gigabytes": "cumsum"}
        )

        df["year_month"] = pd.PeriodIndex(df["year_month"], freq="M")
        df = df.sort_values(by=["project", "year"])

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
        logger.info("Generating fiscal metrics by E3SM file output type.")

        df = pd.read_sql(
            """
            SELECT
                lf.fiscal_year,
                lf.fiscal_year_quarter,
                lr.project,
                COUNT(*) as               sum_requests,
                SUM(lr.megabytes / 1024)  sum_gigabytes
            FROM log_request lr
                LEFT JOIN log_file lf ON lr.log_id = lf.id
            GROUP BY lf.fiscal_year, lr.project, lf.fiscal_year_quarter;
            """,
            con=engine,
        )

        df[["cumsum_requests", "cumsum_gigabytes"]] = df.groupby(["project"]).agg(
            {"sum_requests": "cumsum", "sum_gigabytes": "cumsum"}
        )
        df = df.sort_values(by=["project", "fiscal_year"])

        return df
