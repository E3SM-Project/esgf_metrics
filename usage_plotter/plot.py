import pandas as pd
from matplotlib import pyplot as plt

from usage_plotter.log import logger
from usage_plotter.parse import ProjectTitle


def gen_filename(project_title: ProjectTitle, fiscal_yr: int) -> str:
    """Generates the filename for output files (e.g., .csv and .png).

    :param project_title: The title of the project
    :type project_title: ProjectTitle
    :param fiscal_yr: Fiscal year
    :type fiscal_year: int
    :return: The name of the file
    :rtype: str
    """
    output_dir = "outputs"
    filename = (
        f"{output_dir}/FY{fiscal_yr}_{project_title.replace(' ', '_')}_quarterly_report"
    )

    return filename


def plot_report(
    df: pd.DataFrame,
    project_title: ProjectTitle,
    facet: str,
):
    """Generates a figure of subplots consisting of stacked (by facet) line plots.

    :param df: DataFrame containing monthly report
    :type df: pd.DataFrame
    :param project: Name of the project for the subplot titles
    :type project: Project
    :param facet: Facet to stack line charts on
    :type facet: str
    """
    fiscal_yrs = df.fiscal_yr.unique()

    for fiscal_yr in fiscal_yrs:
        logger.info(f"\nGenerating report and plot for {project_title} FY{fiscal_yr}")
        filename = gen_filename(project_title, fiscal_yr)
        df_fy = df[df.fiscal_yr == fiscal_yr]
        df_fy.to_csv(f"{filename}.csv")

        pivot_table = pd.pivot_table(
            df_fy,
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
            title=f"{project_title} FY{fiscal_yr} Requests by Month ({facet})",
            xlabel="Fiscal Month (July - June)",
            ylabel="Requests",
        )

        pivot_table.gb.plot(
            **base_config,
            ax=axes[1],
            title=f"{project_title} FY{fiscal_yr} Data Access by Month ({facet})",
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
