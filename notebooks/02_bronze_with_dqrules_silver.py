"""Silver transformation entry point for customers_demo."""

from jobs.silver_job import run_silver_job

if "spark" in globals():
    results = run_silver_job(spark)
    print("DQ summary:", results["dq_summary"])
    print("Silver row counts:", results["silver_row_counts"])
else:
    raise RuntimeError(
        "A live Spark session is required. Run this file in Databricks or provide a SparkSession named 'spark'."
    )
