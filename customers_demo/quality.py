"""Data quality and silver layer transformation utilities."""

from customers_demo import config


def apply_dq_to_silver(spark):
    """Apply a null-drop DQ rule and write clean rows to the silver layer."""
    summary = []
    for bronze_table, silver_table, business_columns in config.DQ_TABLES:
        df = spark.table(bronze_table).select(*business_columns)
        total = df.count()
        null_condition = " OR ".join([f"`{col}` IS NULL" for col in business_columns])
        null_count = df.filter(null_condition).count()
        clean_rows = total - null_count
        df_clean = df.dropna(subset=business_columns)
        df_clean.write.mode("overwrite").saveAsTable(silver_table)
        silver_count = spark.table(silver_table).count()
        summary.append((bronze_table, total, null_count, clean_rows, silver_count))
    return summary


def summarize_dq(spark):
    """Return a summary DataFrame payload for the DQ process."""
    summary = apply_dq_to_silver(spark)
    return summary


def silver_row_counts(spark):
    """Return row counts for silver tables in a reusable structure."""
    results = []
    for full_name, short_name in config.SILVER_TABLES:
        results.append((full_name, short_name, spark.table(full_name).count()))
    return results
