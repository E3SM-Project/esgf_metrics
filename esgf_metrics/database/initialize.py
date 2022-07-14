"""This script initializes the SQLAlchemy modules in `models.py` to create the
related SQL tables in the Postgres `public` schema.
"""
from esgf_metrics.database.models import Base
from esgf_metrics.database.settings import engine
from esgf_metrics.logger import setup_custom_logger

logger = setup_custom_logger(__name__)

logger.info("Running database migrations.")
Base.metadata.create_all(engine)
