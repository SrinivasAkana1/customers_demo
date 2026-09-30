"""Silver table inspection entry point for customers_demo."""

from customers_demo.access import grant_select_access, list_select_users
from customers_demo.quality import silver_row_counts

if "spark" in globals():
    print("Silver row counts:")
    print(silver_row_counts(spark))

    print("\nUsers with SELECT access:")
    print(list_select_users(spark))

    # Example: grant access to a user.
    # grant_select_access(spark, "user@example.com")
else:
    raise RuntimeError(
        "A live Spark session is required. Run this file in Databricks or provide a SparkSession named 'spark'."
    )
