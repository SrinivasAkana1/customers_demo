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


def main():
    """Create or reuse Spark and run the bronze job as a Python task."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = run_bronze_job(spark)
    logger.info("Bronze job results: %s", results)
    return results


if __name__ == "__main__":
    main()
