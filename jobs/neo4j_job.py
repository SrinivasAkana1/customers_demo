"""Databricks Python job that loads silver tables into Neo4j Aura."""

from datetime import date, datetime
from decimal import Decimal
import re

from neo4j import GraphDatabase
from pyspark.sql import functions as F

NEO4J_URI = "neo4j+s://f9a06d1f.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_SCOPE = "neo4j"
NEO4J_PASSWORD_KEY = "password"
NEO4J_DATABASE = "neo4j"
BATCH_SIZE = 1000

SILVER_TABLES = [
    ("customers_demo.silver.agent_event_details", "agent_event_details"),
    ("customers_demo.silver.agents", "agents"),
    ("customers_demo.silver.call_types", "call_types"),
    ("customers_demo.silver.customers", "customers"),
    ("customers_demo.silver.route_call_details", "route_call_details"),
    ("customers_demo.silver.terminate_calls", "terminate_calls"),
]

NODE_CONFIG = {
    "agent_event_details": ("AgentEvent", "RKey"),
    "agents": ("Agent", "TID"),
    "call_types": ("CallType", "CTID"),
    "customers": ("Customer", "email"),
    "route_call_details": ("Recovery", "RecoveryKey"),
    "terminate_calls": ("Call", "RCalKey"),
}


def _property_name(column_name):
    return re.sub(r"[^0-9a-zA-Z_]+", "_", column_name).strip("_").lower()


def _neo4j_value(value):
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_neo4j_value(item) for item in value]
    return value


def _node_batches(dataframe, key_column):
    batch = []
    for spark_row in dataframe.toLocalIterator():
        values = spark_row.asDict(recursive=True)
        key_value = values.get(key_column)
        if key_value is None:
            continue

        node_id = str(_neo4j_value(key_value))
        properties = {
            _property_name(column): _neo4j_value(value)
            for column, value in values.items()
        }
        properties["id"] = node_id
        batch.append({"id": node_id, "properties": properties})

        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []

    if batch:
        yield batch


def _relationship_batches(dataframe):
    batch = []
    for spark_row in dataframe.toLocalIterator():
        source_id = spark_row["source_id"]
        target_id = spark_row["target_id"]
        if source_id is None or target_id is None:
            continue
        batch.append({"source_id": str(source_id), "target_id": str(target_id)})
        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []

    if batch:
        yield batch


def _build_relationships(spark):
    agent_ids = (
        spark.table("customers_demo.silver.agents")
        .select(F.col("TID").cast("string").alias("join_id"))
        .filter(F.col("join_id").isNotNull())
        .dropDuplicates(["join_id"])
    )
    handled_links = (
        spark.table("customers_demo.silver.terminate_calls")
        .select(
            F.col("AgntSkllTrgtID").cast("string").alias("join_id"),
            F.col("RCalKey").cast("string").alias("target_id"),
        )
        .filter(F.col("join_id").isNotNull() & F.col("target_id").isNotNull())
        .join(agent_ids, on="join_id", how="inner")
        .select(F.col("join_id").alias("source_id"), "target_id")
    )

    call_type_ids = (
        spark.table("customers_demo.silver.call_types")
        .select(F.col("CTID").cast("string").alias("join_id"))
        .filter(F.col("join_id").isNotNull())
        .dropDuplicates(["join_id"])
    )
    call_type_links = (
        spark.table("customers_demo.silver.terminate_calls")
        .select(
            F.col("RCalKey").cast("string").alias("source_id"),
            F.col("CTID").cast("string").alias("join_id"),
        )
        .filter(F.col("source_id").isNotNull() & F.col("join_id").isNotNull())
        .join(call_type_ids, on="join_id", how="inner")
        .select("source_id", F.col("join_id").alias("target_id"))
    )
    recovery_type_links = (
        spark.table("customers_demo.silver.route_call_details")
        .select(
            F.col("RecoveryKey").cast("string").alias("source_id"),
            F.col("CallTypeID").cast("string").alias("join_id"),
        )
        .filter(F.col("source_id").isNotNull() & F.col("join_id").isNotNull())
        .join(call_type_ids, on="join_id", how="inner")
        .select("source_id", F.col("join_id").alias("target_id"))
    )

    return [
        (
            "HANDLED",
            handled_links,
            "MATCH (source:Agent {id: row.source_id}) "
            "MATCH (target:Call {id: row.target_id}) "
            "MERGE (source)-[:HANDLED]->(target)",
        ),
        (
            "HAS_CALL_TYPE",
            call_type_links,
            "MATCH (source:Call {id: row.source_id}) "
            "MATCH (target:CallType {id: row.target_id}) "
            "MERGE (source)-[:HAS_CALL_TYPE]->(target)",
        ),
        (
            "HAS_CALL_TYPE",
            recovery_type_links,
            "MATCH (source:Recovery {id: row.source_id}) "
            "MATCH (target:CallType {id: row.target_id}) "
            "MERGE (source)-[:HAS_CALL_TYPE]->(target)",
        ),
    ]


def run_neo4j_job(spark, password):
    """Upsert all configured silver rows into Neo4j and return load counts."""
    if not password:
        raise ValueError("A Neo4j password is required; retrieve it from Databricks Secrets.")

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, password),
    )
    try:
        driver.verify_connectivity()
        print("Connected to Neo4j Aura.")

        node_counts = []
        relationship_counts = []
        with driver.session(database=NEO4J_DATABASE) as session:
            for _, short_name in SILVER_TABLES:
                label, _ = NODE_CONFIG[short_name]
                constraint_name = f"{label.lower()}_id_unique"
                session.run(
                    f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
                    f"FOR (node:{label}) REQUIRE node.id IS UNIQUE"
                ).consume()

            for table_name, short_name in SILVER_TABLES:
                label, key_column = NODE_CONFIG[short_name]
                dataframe = spark.table(table_name)
                if key_column not in dataframe.columns:
                    raise ValueError(f"Expected key column {key_column!r} in {table_name}")

                loaded = 0
                query = (
                    f"UNWIND $data AS row "
                    f"MERGE (node:{label} {{id: row.id}}) "
                    "SET node = row.properties"
                )
                for batch in _node_batches(dataframe, key_column):
                    session.run(query, data=batch).consume()
                    loaded += len(batch)
                node_counts.append((table_name, label, loaded))

            for relationship_type, dataframe, cypher in _build_relationships(spark):
                loaded = 0
                query = f"UNWIND $data AS row {cypher}"
                for batch in _relationship_batches(dataframe):
                    session.run(query, data=batch).consume()
                    loaded += len(batch)
                relationship_counts.append((relationship_type, loaded))

            final_node_counts = []
            for _, short_name in SILVER_TABLES:
                label, _ = NODE_CONFIG[short_name]
                count = session.run(
                    f"MATCH (node:{label}) RETURN count(node) AS count"
                ).single()["count"]
                final_node_counts.append((label, count))
    finally:
        driver.close()

    return {
        "nodes_upserted": node_counts,
        "relationships_merged": relationship_counts,
        "node_counts": final_node_counts,
    }


def main():
    """Run as a Databricks Python file task."""
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    password = DBUtils(spark).secrets.get(scope=NEO4J_SCOPE, key=NEO4J_PASSWORD_KEY)
    results = run_neo4j_job(spark, password)
    for section, values in results.items():
        print(f"{section}: {values}")
    return results


if __name__ == "__main__":
    main()