import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

# esgf_metrics settings
LOGS_DIR = os.environ.get("LOGS_DIR", "access_logs")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")

# Postgres settings
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
