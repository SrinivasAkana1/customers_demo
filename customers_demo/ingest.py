"""Bronze ingestion pipeline utilities for the customers_demo lakehouse."""

from customers_demo import config


def create_catalog_and_schemas(spark):
    """Create the catalog and required schemas if they do not exist."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.CATALOG_NAME}")
    for schema in config.SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.CATALOG_NAME}.{schema}")
    return True


def create_external_location(spark, external_location_name=None, storage_credential=None):
    """Create the external location used by the bronze ingestion pipeline."""
    location_name = external_location_name or config.EXTERNAL_LOCATION_NAME
    credential = storage_credential or config.STORAGE_CREDENTIAL
    spark.sql(
        f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS {location_name}
        URL '{config.S3_BUCKET}'
        WITH (STORAGE CREDENTIAL `{credential}`)
        COMMENT 'External location for customers demo S3 data'
        """
    )
    return True


def grant_permissions(spark, authorized_users=None):
    """Grant external table and external volume permissions to authorized users."""
    users = authorized_users or config.AUTHORIZED_USERS
    for user in users:
        spark.sql(
            f"""
            GRANT CREATE EXTERNAL TABLE, CREATE EXTERNAL VOLUME
            ON EXTERNAL LOCATION {config.PERMISSION_EXT_LOCATION}
            TO `{user}`
            """
        )
    return users


def reset_raw_data_table(spark):
    spark.sql(f"DROP TABLE IF EXISTS {config.CATALOG_NAME}.bronze.raw_data")
    return True


def load_bronze_tables(spark):
    """Load all source files from S3 into bronze tables."""
    for table_name, source_path in config.BRONZE_TABLES.items():
        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {config.CATALOG_NAME}.bronze.{table_name}
            AS SELECT * FROM read_files(
              '{source_path}',
              format => 'csv',
              header => true
            )
            """
        )
    return config.BRONZE_TABLES


def verify_bronze_tables(spark):
    """Return row counts for each bronze table."""
    results = []
    for table_name in config.BRONZE_TABLES:
        full_table = f"{config.CATALOG_NAME}.bronze.{table_name}"
        results.append((full_table, spark.table(full_table).count()))
    return results


def run_bronze_pipeline(spark):
    """Execute the bronze pipeline end-to-end."""
    create_catalog_and_schemas(spark)
    create_external_location(spark)
    grant_permissions(spark)
    reset_raw_data_table(spark)
    load_bronze_tables(spark)
    return verify_bronze_tables(spark)
