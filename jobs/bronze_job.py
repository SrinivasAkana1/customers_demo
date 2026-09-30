"""Bronze job runner."""

from configs.source_config import CATALOG_NAME, BRONZE_TABLES
from ingestion.bronze_loader import BronzeLoader
from framework.logger import setup_logger

logger = setup_logger(__name__)


def run_bronze_job(spark):
    """Run the bronze load sequence against the given Databricks Spark session."""
    logger.info("Starting bronze job for catalog %s", CATALOG_NAME)
    loader = BronzeLoader(spark, type("Config", (), {"CATALOG_NAME": CATALOG_NAME, "BRONZE_TABLES": BRONZE_TABLES})())
    return loader.run()
