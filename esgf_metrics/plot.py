"""Plot module for plotting ESGF metrics."""
import pathlib
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter

from esgf_metrics.database.settings import engine
from esgf_metrics.facets import PROJECTS
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.parse import E3SM_CY_TO_FY_MAP, LogParser
from esgf_metrics.settings import OUTPUT_DIR

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure


logger = setup_custom_logger(__name__)


def plot_cumsum_by_project():
    """
    Plots the cumulative sums of the number of requests and downloads by
    project.
    """
    df = pd.read_sql(
        """
        SELECT
            year_month,
            project,
            cumsum_requests AS             "Cumulative Requests",
            (cumsum_gigabytes / 1024) AS   "Cumulative Downloads"
        FROM metrics_monthly
        """,
        con=engine,
    )

    base_config: pd.DataFrame.plot.__init__ = {
        "kind": "line",
        "legend": True,
        "sharex": False,
        "x": "year_month",
        "xlabel": "Month",
        "rot": 45,
    }

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(10, 6))
    fig.suptitle("E3SM Cumulative Data Requests and Downloads by File Format Type")
    formatter = FuncFormatter(_ax_in_millions)

    for index, project in enumerate(PROJECTS):
        df_proj = df.loc[df.project == project]
        df_proj = df_proj.sort_values(by="year_month")

        # Plot the cumulative requests on the first y-axis.
        ax = df_proj.plot(
            **base_config,
            ax=axes[index],
            title=f"{project} Format",
            y="Cumulative Requests",
        )

        # Configure y-axis.
        ax.set_ylabel("# of Requests (Millions)")
        ax.yaxis.set_major_formatter(formatter)

        # Hide the x-axis label
        x_axis = ax.xaxis
        x_axis.label.set_visible(False)

        # Plot the cumulative downloads on the secondary y-axis.
        df_proj.plot(
            "year_month", "Cumulative Downloads", secondary_y=True, ax=ax, color="r"
        )
        ax.right_ax.set_ylabel("Downloads (TB)")
        # Align the secondary y-axis using the same ticks.
        ax.right_ax.set_yticks(np.arange(0, df["Cumulative Downloads"].max(), 40))

        # Perform general figure modifications and save.
        _modify_fig(fig)

    # Add disclaimer caption about logs possibly being missing.
    text = (  # noqa: W605
        "** Metrics are aggregated from ESGF access logs available starting in 2019-07 "
        "to now."
    )
    fig.text(0.5, 0.01, text, wrap=True, ha="center", fontsize=10)
    text2 = (
        "Results might be higher since logs could not be recovered from before 2019-07."
    )
    fig.text(0.5, -0.014, text2, wrap=True, ha="center", fontsize=10)

    # These values might change since logs are parsed on a weekly basis.
    min_date, max_date = df.year_month.min(), df.year_month.max()
    _save_metrics_and_plots(
        fig, df, f"E3SM Cumulative Metrics ({min_date} to {max_date})"
    )


def plot_fiscal_cumsum_by_project(df: pd.DataFrame):
    """
    Plots the cumulative sums of the number of requests and downloads by
    project and fiscal year.

    Parameters
    ----------
    df : pd.DataFrame
        The monthly metrics by project.
    """
    base_config: pd.DataFrame.plot.__init__ = {
        "kind": "line",
        "legend": False,
        "style": ".-",
        "sharex": False,
        "x": "fiscal_month",
        "xticks": range(1, 13),
        "xlabel": "Month",
        "rot": 0,
    }

    for project in PROJECTS:
        df_project = df.loc[df.project == project]
        fiscal_years: List[str] = df_project.fiscal_year.unique()

        for fiscal_year in fiscal_years:
            fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(16, 12))

            df_fy = df_project.loc[df_project.fiscal_year == fiscal_year]
            df_fy.plot(
                **base_config,
                ax=ax[0],
                title=f"{project} FY{fiscal_year} Cumulative HTTP Requests",
                y="cumulative_requests",
                ylabel="Requests",
            )
            df_fy.plot(
                **base_config,
                ax=ax[1],
                title=f"{project} FY{fiscal_year} Cumulative Downloads",
                y="cumulative_get_requests",
                ylabel="Downloads",
            )
            df_fy.plot(
                **base_config,
                ax=ax[2],
                title=f"{project} Cumulative Download Size",
                y="cumulative_gb",
                ylabel="GB",
            )

            _modify_fig(fig)
            _modify_xtick_labels(fig, ax, int(fiscal_year))
            _save_metrics_and_plots(fig, df_fy, project, fiscal_year)


def plot_cumsum_by_facet(metrics_by_facet: LogParser.FiscalFacetMetrics):
    """
    Plots the cumulative sums for the number of requests and data downloads by
    fiscal year and facet.

    Parameters
    ----------
    metrics_by_facet : FiscalFacetMetrics
        The monthly metrics by project and facet.
    """
    base_config: pd.DataFrame.plot.__init__ = {
        "kind": "line",
        "legend": False,
        "style": ".-",
        "sharex": False,
        "xticks": range(1, 13),
        "xlabel": "Month",
        "rot": 0,
    }
    for project, facet_metrics in metrics_by_facet.items():
        for facet, df_metrics in facet_metrics.items():
            fiscal_years: List[str] = df_metrics.fiscal_year.unique()
            for fiscal_year in fiscal_years:
                fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(16, 12))
                df_fy = df_metrics.loc[df_metrics.fiscal_year == fiscal_year]

                pivot_table = (
                    pd.pivot_table(
                        df_fy,
                        index="fiscal_month",
                        values=[
                            "cumulative_requests",
                            "cumulative_get_requests",
                            "cumulative_gb",
                        ],
                        columns=facet,
                        aggfunc="sum",
                    )
                    .fillna(0)
                    .cumsum()
                )

                pivot_table.cumulative_requests.plot(
                    **base_config,
                    ax=ax[0],
                    title=f"{project} FY{fiscal_year} Cumulative HTTP Requests by `{facet}`",
                    ylabel="Requests",
                )
                pivot_table.cumulative_get_requests.plot(
                    **base_config,
                    ax=ax[1],
                    title=f"{project} FY{fiscal_year} Cumulative Downloads by `{facet}`",
                    ylabel="Downloads",
                )
                pivot_table.cumulative_gb.plot(
                    **base_config,
                    ax=ax[2],
                    title=f"{project} FY{fiscal_year} Cumulative Download Size by `{facet}`",
                    ylabel="GB",
                )

                fig = _modify_fig(fig, legend_labels=df_fy[facet].unique())
                ax = _modify_xtick_labels(fig, ax, int(fiscal_year))
                _save_metrics_and_plots(fig, df_fy, project, fiscal_year, facet)


def _modify_fig(fig: "Figure", legend_labels: Optional[List[str]] = None) -> "Figure":
    """Modifies the figure with additional configuration options.

    Parameters
    ----------
    fig : Figure
        The Figure object.
    legend_labels : Optional[List[str]], optional
        Labels for the legend, which are the unique facet option names, by
        default None.

    Returns
    -------
    Figure
        The modified Figure object.
    """
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    if legend_labels is not None:
        fig.legend(labels=legend_labels, loc="lower center", ncol=len(legend_labels))

    return fig


def _modify_xtick_labels(fig: "Figure", ax: "Axes", fiscal_year: int) -> "Figure":
    """Modifies the xtick labels to display the calendar month/year as a str.

    It also adds a vertical line to separate each quarter.

    Example for FY2021: first xtick is "07/2020" and the last xtick is "06/2021"


    Parameters
    ----------
    fig : Figure
        The Figure object.
    ax : Axes
        The Axes object of the Figure.
    fiscal_year : int
        The fiscal year.

    Returns
    -------
    Figure
        The modified Figure object.
    """
    xticklabels = _get_xticklabels(fiscal_year)

    for i in range(len(fig.axes)):
        ax[i].set_xticklabels(xticklabels)
        for tick in range(1, 13):
            end_of_quarter = tick % 3 == 0
            if end_of_quarter:
                ax[i].axvline(x=tick, color="gray", linestyle="--", lw=2)

    return fig


def _get_xticklabels(fiscal_year: int) -> List[str]:
    """Gets a list of xtick labels based on the E3SM CY to FY mapping.

    This is function is useful for cases where data is not available for a month
    or the rest of the year (displays value as 0).

    Parameters
    ----------
    fiscal_year : int
        The fiscal year.

    Returns
    -------
    List[str]
        The list of labels based on the fiscal year.
    """

    labels: List[str] = []

    months = E3SM_CY_TO_FY_MAP.keys()
    mons_in_prev_yr = range(7, 13)

    for month in months:
        if month in mons_in_prev_yr:
            label = f"{month}/{fiscal_year-1}"
        else:
            label = f"{month}/{fiscal_year}"
        labels.append(label)

    return labels


def _save_metrics_and_plots(
    fig: "Figure",
    df: pd.DataFrame,
    filename: str,
    fiscal_year: Optional[str] = None,
    facet: Optional[str] = None,
):
    """Saves the metrics and plots to the outputs directory.

    Parameters
    ----------
    fig : Figure
        The Figure object.
    df : pd.DataFrame
        The metrics DataFrame.
    filename : str
        The base filename.
    fiscal_year : str
        The fiscal year for the project.
    facet : Optional[str], optional
        The name of the facet (if the metrics are based by project and
        facets), by default None.
    """
    filename = _get_filename(filename, fiscal_year, facet)
    df.to_csv(f"{filename}.csv", index=False)
    fig.savefig(filename, dpi=fig.dpi, facecolor="w", bbox_inches="tight")

    logger.info(f"Saved plot path: {filename}")


def _get_filename(
    filename: str, fiscal_year: Optional[str], facet: Optional[str]
) -> str:
    """Gets the name of the output file.

    Parameters
    ----------
    filename : str
        The base filename.
    fiscal_year : Optional[str]
        The fiscal year for the metrics.
    facet : Optional[str]
        The facet, if the metrics are by facet.

    Returns
    -------
    str
        The output filename.
    """
    sub_dir = "metrics_by_project"

    filename = f"{filename}"

    if fiscal_year:
        filename = filename + f"_FY{fiscal_year}"

    if facet:
        sub_dir = "metrics_by_facet"
        filename = filename + f"_by_{facet}"

    directory = f"{OUTPUT_DIR}/{sub_dir}"
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

    return f"{directory}/{filename}"


def _ax_in_millions(x_val, position=None):
    return "%1.1fM" % (x_val * 1e-6)
