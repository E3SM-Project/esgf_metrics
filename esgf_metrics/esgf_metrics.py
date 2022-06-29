from esgf_metrics.parse import LogParser
from esgf_metrics.plot import (
    plot_cumsum_by_facet,
    plot_cumsum_by_project,
    plot_fiscal_cumsum_by_project,
)
from esgf_metrics.settings import LOGS_DIR, OUTPUT_DIR


def main():
    log_parser = LogParser(LOGS_DIR, OUTPUT_DIR)

    # Validate and parse the logs
    log_parser.qa_logs()
    log_parser.parse_logs(to_csv=False)

    # Generate metrics
    log_parser.get_metrics()

    # Plot monthly cumulative sum metrics by project.
    plot_cumsum_by_project(log_parser.df_monthly_metrics)

    # Plot fiscal cumulative sum metrics by project.
    plot_fiscal_cumsum_by_project(log_parser.df_fiscal_metrics)

    # Plot monthly cumulative sums metrics by project type and facet.
    plot_cumsum_by_facet(log_parser.fiscal_facet_metrics)


if __name__ == "__main__":
    main()
