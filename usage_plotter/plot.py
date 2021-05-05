import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.parse import FiscalYear, ProjectTitle


def plot_report(
    df: pd.DataFrame,
    project: ProjectTitle,
    fiscal_year: FiscalYear,
    facet: str,
    filename: str,
):
    """Generates a figure of subplots consisting of stacked (by facet) line plots.

    :param df: DataFrame containing monthly report
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param fiscal_year: Fiscal year of the report
    :type fiscal_year: FiscalYear
    :param facet: Facet to stack line charts on
    :type facet: str
    :param filename: Name of the output file
    :type filename: str
    """
    pivot_table = pd.pivot_table(
        df,
        index="fiscal_month",
        values=["requests", "gb"],
        columns=facet,
        aggfunc="sum",
    )

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 8))
    # https://pandas.pydata.org/pandas-docs/version/0.15.2/generated/pandas.DataFrame.plot.html
    base_config: pd.DataFrame.plot.__init__ = {
        "kind": "line",
        "stacked": True,
        "legend": False,
        "style": ".-",
        "sharex": True,
        "xticks": (range(1, 13)),
        "rot": 0,
    }

    pivot_table.requests.plot(
        **base_config,
        ax=axes[0],
        title=f"{project} FY{fiscal_year} Requests by Month ({facet})",
        xlabel="Fiscal Month (July - June)",
        ylabel="Requests",
    )

    pivot_table.gb.plot(
        **base_config,
        ax=axes[1],
        title=f"{project} {fiscal_year} Data Access by Month ({facet})",
        xlabel="Fiscal Month (July - June)",
        ylabel="Data Access (GB)",
    )

    # Add vertical lines to represent quarters
    for i in range(len(fig.axes)):
        axes[i].axvline(x=3, color="blue", linestyle="--", lw=2)
        axes[i].axvline(x=6, color="blue", linestyle="--", lw=2)
        axes[i].axvline(x=9, color="blue", linestyle="--", lw=2)

    # Add legend labels at the bottom to avoid legends overlapping plot values.
    legend_labels = df[facet].unique()
    fig.legend(labels=legend_labels, loc="lower center", ncol=len(legend_labels))
    fig.tight_layout()
    fig.subplots_adjust(bottom=0.1)

    # Save figure to file for analysis
    fig.savefig(filename, dpi=fig.dpi, facecolor="w")
