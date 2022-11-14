from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.metrics import MetricsGenerator
from esgf_metrics.parse import LogParser
from esgf_metrics.plot import plot_cumsum_by_project

logger = setup_custom_logger(__name__)


def main():
    # Parse logs and save to the database.
    parser = LogParser()

    if parser.df_log_file is not None and parser.df_log_request is not None:
        parser.to_sql()

    # Generate the metrics using parsed logs.
    metrics = MetricsGenerator()
    metrics.get_metrics()
    metrics.to_sql()

    # Generate plot using metrics.
    plot_cumsum_by_project()


if __name__ == "__main__":
    main()
