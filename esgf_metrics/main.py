"""The main module for running esgf_metrics.

This module is executed in the `esgf_metrics` docker-compose container.

It can also be executed in a local Python development, which is useful if the
acme1 server is restarted and the docker-compose containers are not restarted
via supervisorctl.

To run this module in a local dev environment:
  1. First create and activate the mamba/conda dev environment, `dev.yml`.
  2. Copy `.env.template` as `.env`
  3. Comment the lines for postgres config with
  4. Uncomment the lines for the postgres config for local dev outside of docker.
"""
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
