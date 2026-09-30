"""Silver job runner."""

from customers_demo import config
from customers_demo.quality import apply_dq_to_silver, silver_row_counts
from framework.logger import setup_logger

logger = setup_logger(__name__)


def run_silver_job(spark):
    """Apply configured DQ rules and write silver tables using the given Spark session."""
    logger.info("Starting silver job for catalog %s", config.CATALOG_NAME)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG_NAME}.silver")
    summary = apply_dq_to_silver(spark)
    row_counts = silver_row_counts(spark)
    logger.info("Silver job completed for catalog %s", config.CATALOG_NAME)
    return {"dq_summary": summary, "silver_row_counts": row_counts}
