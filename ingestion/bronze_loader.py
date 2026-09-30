"""Reusable bronze loader for ingestion jobs."""

from datetime import datetime

from framework.logger import setup_logger

logger = setup_logger(__name__)


class BronzeLoader:
    """Load raw source files from S3 into the bronze layer."""

    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def load_table(self, table_name: str, source_path: str):
        """Read a raw table from an S3 path and persist it to the bronze schema."""
        logger.info("Loading %s from %s", table_name, source_path)
        start_time = datetime.utcnow()
        try:
            df = self.spark.read.format("csv").option("header", True).option("inferSchema", True).load(source_path)
            target_table = f"{self.config.CATALOG_NAME}.bronze.{table_name}"
            df.write.mode("overwrite").format("delta").saveAsTable(target_table)
            end_time = datetime.utcnow()
            logger.info("Loaded %s into %s successfully", table_name, target_table)
            return {"table_name": table_name, "status": "success", "start_time": start_time, "end_time": end_time}
        except Exception as exc:  # pragma: no cover
            logger.exception("Failed to load %s", table_name)
            return {"table_name": table_name, "status": "failed", "error": str(exc)}

    def run(self):
        """Run all configured bronze loads."""
        results = []
        for table_name, source_path in self.config.BRONZE_TABLES.items():
            results.append(self.load_table(table_name, source_path))
        return results
