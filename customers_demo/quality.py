"""Data quality and silver layer transformation utilities."""

from customers_demo import config


def copy_bronze_to_silver(spark):
    """Copy every configured bronze table to silver without filtering rows or columns."""
    summary = []
    for table_name in config.OBSOLETE_SILVER_TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    for table_name in config.BRONZE_TABLES:
        bronze_table = f"{config.CATALOG_NAME}.bronze.{table_name}"
        silver_table = f"{config.CATALOG_NAME}.silver.{table_name}"
        df = spark.table(bronze_table)
        bronze_count = df.count()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        silver_count = spark.table(silver_table).count()
        summary.append((bronze_table, silver_table, bronze_count, silver_count))
    return summary


def apply_dq_to_silver(spark):
    """Compatibility wrapper; this pipeline currently copies rows without DQ filtering."""
    return copy_bronze_to_silver(spark)


def summarize_dq(spark):
    """Compatibility wrapper returning the bronze-to-silver copy summary."""
    return copy_bronze_to_silver(spark)


def silver_row_counts(spark):
    """Return row counts for silver tables in a reusable structure."""
    results = []
    for full_name, short_name in config.SILVER_TABLES:
        results.append((full_name, short_name, spark.table(full_name).count()))
    return results
