"""Single Databricks entry point for the S3-to-bronze-to-silver pipeline."""

from customers_demo.ingest import run_bronze_pipeline
from customers_demo.quality import copy_bronze_to_silver
from framework.logger import setup_logger

logger = setup_logger(__name__)


def run_pipeline(spark):
    """Load all configured S3 sources to bronze, then copy bronze to silver."""
    logger.info("Starting S3-to-bronze stage")
    bronze_results = run_bronze_pipeline(spark)
    logger.info("Bronze stage completed: %s", bronze_results)

    logger.info("Starting bronze-to-silver stage")
    silver_results = copy_bronze_to_silver(spark)
    logger.info("Silver stage completed: %s", silver_results)

    return {"bronze": bronze_results, "silver": silver_results}


def main():
    """Run the complete pipeline as one Databricks Python file task."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return run_pipeline(spark)


if __name__ == "__main__":
    main()