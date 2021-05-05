import argparse

import pandas as pd

from usage_plotter import log
from usage_plotter.parse import FiscalYear, ProjectTitle, gen_report, parse_logs
from usage_plotter.plot import plot_report


def parse_args(console: bool = False) -> argparse.Namespace:
    """Parses command line arguments to configure the software.

    :param console: Bypass argparse when using Python interactive consoles, returns default values
    :type console: bool, default False
    :return: Command line arguments
    :rtype: argparse.NameSpace
    """
    parser = argparse.ArgumentParser()
    # TODO: Generate report for all available fiscal years to avoid rerunning the code.
    parser.add_argument(
        "--fiscal_year",
        "-fy",
        type=str,
        choices=("2019", "2020", "2021"),
        help="A string for reporting E3SM Infrastructure Group fiscal year(default: 2021).",
        required=True,
    )
    parser.add_argument(
        "--logs_path",
        "-l",
        type=str,
        default="access_logs",
        help="The string path to the ESGF Apache access logs (default: access_logs).",
        required=False,
    )

    if console:
        return parser.parse_args([])
    return parser.parse_args()


def gen_filename(project: ProjectTitle, fiscal_year: FiscalYear) -> str:
    """Generates the filename for output files (e.g., .csv and .png).

    :param project: [description]
    :type project: ProjectTitle
    :param fiscal_year: [description]
    :type fiscal_year: FiscalYear
    :return: [description]
    :rtype: str
    """
    output_dir = "outputs"
    filename = (
        f"{output_dir}/{project.replace(' ', '_')}_quarterly_report_FY{fiscal_year}"
    )

    return filename


def main():
    logger = log.setup_custom_logger("root")

    # Configuration
    # =============
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    fiscal_year = parsed_args.fiscal_year
    logger.info(
        f"\nGenerating report with the following config:\n- Logs Path: {logs_path}\n- Fiscal Year: {fiscal_year}\n"
    )

    # Initial log parsing
    # ===================
    logger.info("Parsing access logs...")
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM report
    # ===========
    logger.info(f"Generating FY{fiscal_year} CSV report and plots...")
    e3sm_title: ProjectTitle = "E3SM"
    e3sm_filename = gen_filename(e3sm_title, fiscal_year)
    df_e3sm = df[df.project == "E3SM"]

    # By time frequency
    df_e3sm_tf = gen_report(df_e3sm, facet="time_frequency")
    df_e3sm_tf.to_csv(f"{e3sm_filename}.csv")
    plot_report(
        df_e3sm_tf,
        project="E3SM",
        fiscal_year=fiscal_year,
        facet="time_frequency",
        filename=e3sm_filename,
    )

    # E3SM in CMIP6 report
    # ====================
    e3sm_cmip6_title: ProjectTitle = "E3SM in CMIP6"
    e3sm_cmip6_filename = gen_filename(e3sm_cmip6_title, fiscal_year)
    df_e3sm_cmip6 = df[df.project == "E3SM in CMIP6"]

    # By activity
    df_e3sm_cmip6_activity = gen_report(df_e3sm_cmip6, facet="activity")
    df_e3sm_cmip6_activity.to_csv(f"{e3sm_cmip6_filename}.csv")
    plot_report(
        df_e3sm_cmip6_activity,
        project=e3sm_cmip6_title,
        fiscal_year=fiscal_year,
        facet="activity",
        filename=e3sm_cmip6_filename,
    )

    logger.info("Completed, check the /outputs directory.")


if __name__ == "__main__":
    main()
