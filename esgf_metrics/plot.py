"""Plot module for plotting ESGF metrics."""
from typing import TYPE_CHECKING, List, Optional

import pandas as pd
from matplotlib import pyplot as plt

from esgf_metrics.facets import PROJECTS, ProjectTitle
from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.parse import E3SM_CY_TO_FY_MAP, LogParser
from esgf_metrics.settings import OUTPUT_DIR

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure


logger = setup_custom_logger(__name__)


def plot_cumsum_by_project(df: pd.DataFrame):
    """
    Plots the cumulative sums of the number of requests and downloads by
    project.

    Parameters
    ----------
    df : pd.DataFrame
        The monthly metrics by project.
    """
    df["calendar_year_month"] = df["calendar_year_month"].astype(str)
    df["cumulative_requests"] = df.groupby(["project"])["total_requests"].cumsum()
    df["cumulative_get_requests"] = df.groupby(["project"])[
        "total_get_requests"
    ].cumsum()
    df["cumulative_gb"] = df.groupby(["project"])["total_gb"].cumsum()

    for project in PROJECTS:
        df_project = df.loc[df.project == project]
        fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(16, 12))

        df_project.plot(
            ax=ax[0],
            title=f"{project} Cumulative HTTP Requests",
            x="calendar_year_month",
            y="cumulative_requests",
            xlabel="Year and Month",
            ylabel="Requests",
            legend=False,
        )
        df_project.plot(
            ax=ax[1],
            title=f"{project} Cumulative Downloads",
            x="calendar_year_month",
            y="cumulative_get_requests",
            xlabel="Year and Month",
            ylabel="Downloads",
            legend=False,
        )
        df_project.plot(
            ax=ax[2],
            title=f"{project} Cumulative Download Size",
            x="calendar_year_month",
            y="cumulative_gb",
            xlabel="Year and Month",
            ylabel="GB",
            legend=False,
        )

        _modify_fig(fig)
        _save_metrics_and_plots(fig, df_project, project)


def plot_fiscal_cumsum_by_project(df: pd.DataFrame):
    """
    Plots the cumulative sums of the number of requests and downloads by
    project and fiscal year.

    Parameters
    ----------
    df : pd.DataFrame
        The monthly metrics by project.
    """
    for project in PROJECTS:
        df_project = df.loc[df.project == project]
        fiscal_years: List[str] = df_project.fiscal_year.unique()

        for fiscal_year in fiscal_years:
            df_fy = df_project.loc[df_project.fiscal_year == fiscal_year]

            fig, ax = plt.subplots(nrows=3, ncols=1, figsize=(16, 12))

            df_fy.plot(
                ax=ax[0],
                title=f"{project} FY{fiscal_year} Cumulative HTTP Requests",
                x="fiscal_month",
                y="cumulative_requests",
                xticks=range(1, 13),
                xlabel="Month",
                ylabel="Requests",
                legend=False,
            )
            df_fy.plot(
                ax=ax[1],
                title=f"{project} FY{fiscal_year} Cumulative Downloads",
                x="fiscal_month",
                y="cumulative_get_requests",
                xticks=range(1, 13),
                xlabel="Month",
                ylabel="Downloads",
                legend=False,
            )

            df_project.plot(
                ax=ax[2],
                title=f"{project} Cumulative Download Size",
                x="fiscal_month",
                y="cumulative_gb",
                xlabel="Month",
                ylabel="GB",
                legend=False,
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
        "sharex": True,
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
    project_title: ProjectTitle,
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
    project_title : ProjectTitle
        The project title.
    fiscal_year : str
        The fiscal year for the project.
    facet : Optional[str], optional
        The name of the facet (if the metrics are based by project and
        facets), by default None.
    """
    filename = _get_filename(project_title, fiscal_year, facet)
    df.to_csv(f"{filename}.csv", index=False)
    fig.savefig(filename, dpi=fig.dpi, facecolor="w")


def _get_filename(
    project_title: ProjectTitle, fiscal_year: Optional[str], facet: Optional[str]
) -> str:
    """Gets the name of the output file.

    Parameters
    ----------
    project_title : ProjectTitle
        The project title.
    fiscal_year : Optional[str]
        The fiscal year for the metrics.
    facet : Optional[str]
        The facet, if the metrics are by facet.

    Returns
    -------
    str
        The output filename.
    """
    sub_dir = "/metrics_by_project"

    filename = f"{project_title}"

    if fiscal_year:
        filename = filename + f"_FY{fiscal_year}"

    if facet:
        sub_dir = "/metrics_by_facet"
        filename = filename + f"_by_{facet}"

    return f"{OUTPUT_DIR}/{sub_dir}/{filename}"
