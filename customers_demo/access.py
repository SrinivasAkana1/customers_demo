"""Grant access utilities for silver tables."""

import pandas as pd


def list_select_users(spark, table_name="customers_demo.silver.customers"):
    """Return the unique users with SELECT access on a silver table."""
    grants_df = spark.sql(f"SHOW GRANTS ON TABLE {table_name}").toPandas()
    select_grants = grants_df[grants_df["ActionType"] == "SELECT"]
    users = select_grants["Principal"].unique().tolist()
    return sorted(users)


def grant_select_access(spark, user_email, silver_tables=None):
    """Grant SELECT access to the provided user on all silver tables."""
    if silver_tables is None:
        silver_tables = [
            "customers_demo.silver.agent_event_details",
            "customers_demo.silver.agents",
            "customers_demo.silver.call_types",
            "customers_demo.silver.customers",
            "customers_demo.silver.route_call_details",
            "customers_demo.silver.terminate_calls",
        ]

    existing_users = list_select_users(spark)
    if user_email in existing_users:
        return {"status": "exists", "user": user_email, "tables": silver_tables}

    for table_name in silver_tables:
        grant_sql = f"GRANT SELECT ON TABLE {table_name} TO `{user_email}`"
        spark.sql(grant_sql)

    return {"status": "granted", "user": user_email, "tables": silver_tables}
