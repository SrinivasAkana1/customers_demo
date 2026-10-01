"""Single Databricks entry point for the S3-to-bronze-to-silver pipeline."""

from customers_demo.ingest import run_bronze_pipeline
from customers_demo.quality import copy_bronze_to_silver
from framework.logger import setup_logger
from jobs.neo4j_job import get_neo4j_password, run_neo4j_job

logger = setup_logger(__name__)


def run_pipeline(spark, neo4j_password=None):
    """Load S3 to bronze, copy bronze to silver, then load the graph."""
    logger.info("Starting S3-to-bronze stage")
    bronze_results = run_bronze_pipeline(spark)
    logger.info("Bronze stage completed: %s", bronze_results)

    logger.info("Starting bronze-to-silver stage")
    silver_results = copy_bronze_to_silver(spark)
    logger.info("Silver stage completed: %s", silver_results)

    password = neo4j_password or get_neo4j_password(spark)
    logger.info("Starting silver-to-Neo4j stage")
    neo4j_results = run_neo4j_job(spark, password)
    logger.info("Neo4j stage completed: %s", neo4j_results)

    return {
        "bronze": bronze_results,
        "silver": silver_results,
        "neo4j": neo4j_results,
    }


def main():
    """Run the complete pipeline as one Databricks Python file task."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    return run_pipeline(spark)


if __name__ == "__main__":
    main()