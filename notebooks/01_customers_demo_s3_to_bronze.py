"""Bronze pipeline entry point for customers_demo.

This file is kept as a Databricks-friendly notebook script while reusing the
shared Python framework in the customers_demo package.
"""

try:
    from pyspark.shell import spark
except Exception:  # pragma: no cover - handled when Spark is not available
    spark = None

from customers_demo.ingest import run_bronze_pipeline

# Databricks provides a `spark` variable automatically.
if spark is not None:
    run_bronze_pipeline(spark)
elif "spark" in globals():
    run_bronze_pipeline(globals()["spark"])
else:
    raise RuntimeError(
        "A live Spark session is required. Run this file in Databricks or provide a SparkSession named 'spark'."
    )
