import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(), override=True)

LOGS_DIR = os.environ.get("LOGS_DIR", "access_logs")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
