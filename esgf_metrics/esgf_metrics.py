from esgf_metrics.parse import LogParser
from esgf_metrics.plot import plot_cumsum_by_facet, plot_cumsum_by_project
from esgf_metrics.settings import LOGS_DIR, OUTPUT_DIR


def main():
    # Parse logs and generate metrics
    log_parser = LogParser(LOGS_DIR, OUTPUT_DIR)
    log_parser.qa_logs()
    log_parser.parse_logs()
    log_parser.generate_metrics()

    # Plot monthly cumulative sum metrics by project.
    plot_cumsum_by_project(log_parser.df_fiscal_metrics)

    # Plot monthly cumulative sums metrics by project type and facet.
    plot_cumsum_by_facet(log_parser.fiscal_facet_metrics)


if __name__ == "__main__":
    main()
