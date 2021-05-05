import argparse

import pandas as pd

from usage_plotter.log import logger
from usage_plotter.parse import ProjectTitle, gen_report, parse_logs
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


def main():

    # Configuration
    # =============
    parsed_args = parse_args()
    logs_path = parsed_args.logs_path
    logger.info(f"\nGenerating report for access logs in `{logs_path}`\n")

    # Initial log parsing
    # ===================
    logger.info("\nParsing access logs...")
    df: pd.DataFrame = parse_logs(logs_path)

    # E3SM report
    # ===========
    e3sm_title: ProjectTitle = "E3SM"
    df_e3sm = df[df.project == e3sm_title]

    # By time frequency
    df_e3sm_tf = gen_report(df_e3sm, facet="time_frequency")
    plot_report(
        df_e3sm_tf,
        project_title=e3sm_title,
        facet="time_frequency",
    )

    # E3SM in CMIP6 report
    # ====================
    e3sm_cmip6_title: ProjectTitle = "E3SM in CMIP6"
    df_e3sm_cmip6 = df[df.project == e3sm_cmip6_title]

    # By activity
    df_e3sm_cmip6_activity = gen_report(df_e3sm_cmip6, facet="activity")
    plot_report(
        df_e3sm_cmip6_activity,
        project_title=e3sm_cmip6_title,
        facet="activity",
    )

    logger.info("\nCompleted, check the /outputs directory.")


if __name__ == "__main__":
    main()
