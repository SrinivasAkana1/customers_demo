"""Silver transformation entry point for customers_demo."""

from customers_demo.quality import apply_dq_to_silver, silver_row_counts

if "spark" in globals():
    spark.sql("CREATE SCHEMA IF NOT EXISTS customers_demo.silver")
    summary = apply_dq_to_silver(spark)
    print("DQ summary:", summary)
    print("Silver row counts:", silver_row_counts(spark))
else:
    raise RuntimeError(
        "A live Spark session is required. Run this file in Databricks or provide a SparkSession named 'spark'."
    )
