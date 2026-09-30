"""Silver table inspection entry point for customers_demo."""

from customers_demo.quality import silver_row_counts

if "spark" in globals():
    print("Silver row counts:")
    print(silver_row_counts(spark))

else:
    raise RuntimeError(
        "A live Spark session is required. Run this file in Databricks or provide a SparkSession named 'spark'."
    )
