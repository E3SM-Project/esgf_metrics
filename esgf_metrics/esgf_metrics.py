from esgf_metrics.parse import LogParser
from esgf_metrics.plot import plot_cumsum_by_facet, plot_cumsum_by_project


def main():
    # Parse logs and generate metrics
    log_parser = LogParser()
    log_parser.parse_logs()
    log_parser.generate_metrics()

    # Plot monthly cumulative sum metrics by project.
    plot_cumsum_by_project(log_parser.metrics)

    # Plot monthly sums metrics by project type and facet.
    plot_cumsum_by_facet(log_parser.metrics_by_facet)


if __name__ == "__main__":
    main()
