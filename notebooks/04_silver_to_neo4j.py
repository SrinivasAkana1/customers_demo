# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Neo4j
# MAGIC
# MAGIC Loads every configured Unity Catalog silver row as a Neo4j node and writes all source columns as node properties.
# MAGIC Re-runs use `MERGE`, so existing nodes are updated rather than duplicated. This notebook does not delete existing graph data.
# MAGIC
# MAGIC Before running:
# MAGIC - Create a Databricks secret scope named `neo4j` with keys `uri`, `username`, and `password`.
# MAGIC - Set `uri` to a Neo4j Aura endpoint reachable from Databricks. A Neo4j Desktop address such as `127.0.0.1` is local to your computer and is not reachable from the Databricks cluster.
# MAGIC - Run notebooks 01 and 02 first so the silver tables exist.
# MAGIC
# MAGIC Silver rows become `Customer`, `Agent`, `CallType`, `AgentEvent`, `Recovery`, and `Call` nodes.
# MAGIC Relationships are created only from matching keys present in the silver data. Customer-to-call and customer-to-recovery links are not fabricated because these tables do not provide a customer key.

# COMMAND ----------

# MAGIC %pip install neo4j

# COMMAND ----------

from datetime import date, datetime
from decimal import Decimal
import re

from neo4j import GraphDatabase
from pyspark.sql import functions as F

from customers_demo import config

NEO4J_SCOPE = "neo4j"
NEO4J_URI = dbutils.secrets.get(scope=NEO4J_SCOPE, key="uri")
NEO4J_USERNAME = dbutils.secrets.get(scope=NEO4J_SCOPE, key="neo4j")
NEO4J_PASSWORD = dbutils.secrets.get(scope=NEO4J_SCOPE, key="YFXTKpr4jtJTLd6rEa1z0TI3ic_xiWOyXNFmTAZhLbA")
NEO4J_DATABASE = "neo4j"
BATCH_SIZE = 1000

NODE_CONFIG = {
    "agent_event_details": ("AgentEvent", "RKey"),
    "agents": ("Agent", "TID"),
    "call_types": ("CallType", "CTID"),
    "customers": ("Customer", "email"),
    "route_call_details": ("Recovery", "RecoveryKey"),
    "terminate_calls": ("Call", "RCalKey"),
}

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
driver.verify_connectivity()
print("Connected to Neo4j.")

# COMMAND ----------

def _property_name(column_name):
    """Normalize source column names to readable Neo4j property names."""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", column_name).strip("_").lower()


def _neo4j_value(value):
    """Convert Spark row values to values supported by the Neo4j driver."""
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
    """Yield bounded batches containing a stable id and every source column."""
    batch = []
    for spark_row in dataframe.toLocalIterator():
        values = spark_row.asDict(recursive=True)
        key_value = values.get(key_column)
        if key_value is None:
            continue

        properties = {
            _property_name(column): _neo4j_value(value)
            for column, value in values.items()
        }
        node_id = str(_neo4j_value(key_value))
        properties["id"] = node_id
        batch.append({"id": node_id, "properties": properties})

        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []

    if batch:
        yield batch


def _relationship_batches(dataframe, source_column, target_column):
    """Yield bounded relationship batches from a Spark join result."""
    batch = []
    for spark_row in dataframe.toLocalIterator():
        source_id = spark_row[source_column]
        target_id = spark_row[target_column]
        if source_id is None or target_id is None:
            continue
        batch.append({
            "source_id": str(source_id),
            "target_id": str(target_id),
        })
        if len(batch) >= BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _load_relationship(session, dataframe, query):
    count = 0
    for batch in _relationship_batches(dataframe, "source_id", "target_id"):
        session.run(query, data=batch).consume()
        count += len(batch)
    return count


# COMMAND ----------

with driver.session(database=NEO4J_DATABASE) as session:
    for table_name, short_name in config.SILVER_TABLES:
        label, _ = NODE_CONFIG[short_name]
        constraint_name = f"{label.lower()}_id_unique"
        session.run(
            f"CREATE CONSTRAINT {constraint_name} IF NOT EXISTS "
            f"FOR (node:{label}) REQUIRE node.id IS UNIQUE"
        ).consume()

    load_summary = []
    for table_name, short_name in config.SILVER_TABLES:
        label, key_column = NODE_CONFIG[short_name]
        dataframe = spark.table(table_name)
        if key_column not in dataframe.columns:
            raise ValueError(f"Expected key column {key_column!r} in {table_name}")

        loaded = 0
        for batch in _node_batches(dataframe, key_column):
            query = (
                f"UNWIND $data AS row "
                f"MERGE (node:{label} {{id: row.id}}) "
                "SET node = row.properties"
            )
            session.run(query, data=batch).consume()
            loaded += len(batch)
        load_summary.append((table_name, label, loaded))

    display(
        spark.createDataFrame(
            load_summary,
            ["silver_table", "neo4j_label", "rows_upserted"],
        )
    )

# COMMAND ----------

# Create only relationships supported by source keys in silver.
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

relationship_specs = [
    (
        handled_links,
        "MATCH (source:Agent {id: row.source_id}) "
        "MATCH (target:Call {id: row.target_id}) "
        "MERGE (source)-[:HANDLED]->(target)",
    ),
    (
        call_type_links,
        "MATCH (source:Call {id: row.source_id}) "
        "MATCH (target:CallType {id: row.target_id}) "
        "MERGE (source)-[:HAS_CALL_TYPE]->(target)",
    ),
    (
        recovery_type_links,
        "MATCH (source:Recovery {id: row.source_id}) "
        "MATCH (target:CallType {id: row.target_id}) "
        "MERGE (source)-[:HAS_CALL_TYPE]->(target)",
    ),
]

with driver.session(database=NEO4J_DATABASE) as session:
    for dataframe, cypher in relationship_specs:
        linked = _load_relationship(session, dataframe, f"UNWIND $data AS row {cypher}")
        print(f"Relationship rows merged: {linked}")

# COMMAND ----------

# Check graph node counts from this notebook's six silver tables.
with driver.session(database=NEO4J_DATABASE) as session:
    for _, short_name in config.SILVER_TABLES:
        label, _ = NODE_CONFIG[short_name]
        count = session.run(
            f"MATCH (node:{label}) RETURN count(node) AS count"
        ).single()["count"]
        print(f"{label}: {count} nodes")

driver.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the graph
# MAGIC
# MAGIC Run these queries in Neo4j Browser or Neo4j Desktop's Query pane:
# MAGIC
# MAGIC ```cypher
# MAGIC MATCH (n) RETURN n LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ```cypher
# MAGIC MATCH p=()-[]->() RETURN p LIMIT 100;
# MAGIC ```
# MAGIC
# MAGIC ```cypher
# MAGIC MATCH (a:Agent)-[r:HANDLED]->(c:Call)
# MAGIC RETURN a, r, c LIMIT 50;
# MAGIC ```