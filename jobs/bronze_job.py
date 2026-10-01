"""Bronze job runner."""

from customers_demo import config
from customers_demo.ingest import create_catalog_and_schemas
from ingestion.bronze_loader import BronzeLoader
from framework.logger import setup_logger

logger = setup_logger(__name__)


def run_bronze_job(spark):
    """Run the bronze load sequence against the given Databricks Spark session."""
    logger.info("Starting bronze job for catalog %s", config.CATALOG_NAME)
    create_catalog_and_schemas(spark)
    for table_name in config.OBSOLETE_BRONZE_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    results = BronzeLoader(spark, config).run()
    failures = [result for result in results if result["status"] != "success"]
    if failures:
        raise RuntimeError(f"Bronze load failed for tables: {failures}")
    return results


def main():
    """Create or reuse Spark and run the bronze job as a Python task."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    results = run_bronze_job(spark)
    logger.info("Bronze job results: %s", results)
    return results


if __name__ == "__main__":
    main()




    
