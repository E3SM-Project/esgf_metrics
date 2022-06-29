from esgf_metrics.logger import setup_custom_logger
from esgf_metrics.parse import LogParser

logger = setup_custom_logger(__name__)


def main():
    parser = LogParser()

    if parser.df_log_file is not None and parser.df_log_request is not None:
        parser.to_sql()


if __name__ == "__main__":
    main()
