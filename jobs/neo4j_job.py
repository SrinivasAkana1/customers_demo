"""Databricks Python job that loads silver tables into Neo4j Aura."""

from datetime import date, datetime
from decimal import Decimal
import re

from customers_demo import config

NEO4J_URI = "neo4j+s://f9a06d1f.databases.neo4j.io"
NEO4J_USERNAME = "neo4j"
NEO4J_SCOPE = "neo4j"
NEO4J_PASSWORD_KEY = "password"
NEO4J_DATABASE = "neo4j"
BATCH_SIZE = 1000

SILVER_TABLES = [
    (f"{config.CATALOG_NAME}.silver.{table_name}", table_name)
    for table_name in config.BRONZE_TABLES
]

NODE_CONFIG = {
    "party": ("Party", "party_sk"),
    "policy": ("Policy", "policy_sk"),
    "product": ("Product", "product_sk"),
    "coverage": ("Coverage", "coverage_sk"),
    "service_request": ("ServiceRequest", "service_request_sk"),
    "fact_service_request": ("FactServiceRequest", "service_fact_sk"),
    "fact_claim": ("FactClaim", "claim_sk"),
    "claim_payment": ("ClaimPayment", "payment_sk"),
    "call_types": ("CallType", "call_type_sk"),
    "route_call_details": ("RouteCallDetail", "route_call_sk"),
    "terminate_calls": ("TerminationCallDetail", "termination_call_sk"),
    "agents": ("Agent", "agent_sk"),
    "agent_event_details": ("AgentEventDetail", "agent_event_sk"),
}

RELATIONSHIP_RULES = [
    ("party", "policy", "party_sk", "OWNS", "1:M"),
    ("product", "policy", "product_sk", "PRODUCT_FOR", "1:M"),
    ("policy", "coverage", "policy_sk", "HAS_COVERAGE", "1:M"),
    ("party", "service_request", "party_sk", "RAISES", "1:M"),
    ("service_request", "fact_service_request", "service_request_sk", "GENERATES_EVENT", "1:M"),
    ("policy", "fact_claim", "policy_sk", "GENERATES_CLAIM", "1:M"),
    ("fact_claim", "claim_payment", "claim_sk", "HAS_PAYMENT", "1:M"),
    ("party", "route_call_details", "customer_sk", "MAKES_CONTACT", "1:M"),
    ("call_types", "route_call_details", "call_type_sk", "CLASSIFIES", "1:M"),
    ("route_call_details", "agent_event_details", "route_call_sk", "HAS_AGENT_EVENT", "1:M"),
    ("agents", "agent_event_details", "agent_sk", "PERFORMS", "1:M"),
    ("route_call_details", "terminate_calls", "route_call_sk", "HAS_TERMINATION", "1:1"),
]


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
    from pyspark.sql import functions as F

    relationships = []
    for parent_table, child_table, child_fk, relationship_type, cardinality in RELATIONSHIP_RULES:
        parent_label, parent_pk = NODE_CONFIG[parent_table]
        child_label, child_pk = NODE_CONFIG[child_table]
        parents = (
            spark.table(f"{config.CATALOG_NAME}.silver.{parent_table}")
            .select(F.col(parent_pk).cast("string").alias("source_id"))
            .filter(F.col("source_id").isNotNull())
            .dropDuplicates(["source_id"])
        )
        children = spark.table(f"{config.CATALOG_NAME}.silver.{child_table}").select(
            F.col(child_fk).cast("string").alias("source_id"),
            F.col(child_pk).cast("string").alias("target_id"),
        )

        invalid_keys = children.filter(
            F.col("source_id").isNull() | F.col("target_id").isNull()
        ).count()
        keyed_children = children.filter(
            F.col("source_id").isNotNull() & F.col("target_id").isNotNull()
        )
        unmatched_children = keyed_children.join(parents, on="source_id", how="left_anti").count()
        child_parent_keys = keyed_children.select("source_id").dropDuplicates(["source_id"])
        missing_children = parents.join(
            child_parent_keys,
            on="source_id",
            how="left_anti",
        ).count()

        if cardinality == "1:1":
            duplicate_parent_refs = (
                keyed_children.groupBy("source_id")
                .count()
                .filter(F.col("count") > 1)
                .count()
            )
            if invalid_keys or unmatched_children or missing_children or duplicate_parent_refs:
                raise ValueError(
                    f"{relationship_type} violates 1:1 cardinality: "
                    f"invalid_keys={invalid_keys}, unmatched_children={unmatched_children}, "
                    f"missing_children={missing_children}, duplicate_parent_refs={duplicate_parent_refs}"
                )

        link_rows = keyed_children.join(parents, on="source_id", how="inner").dropDuplicates(
            ["source_id", "target_id"]
        )
        if invalid_keys or unmatched_children:
            print(
                f"{relationship_type}: skipped invalid/unmatched child rows="
                f"{invalid_keys + unmatched_children}"
            )
        query = (
            f"MATCH (parent:{parent_label} {{id: row.source_id}}) "
            f"MATCH (child:{child_label} {{id: row.target_id}}) "
            f"MERGE (parent)-[:{relationship_type}]->(child)"
        )
        relationships.append((relationship_type, link_rows, query))

    return relationships


def run_neo4j_job(spark, password):
    """Upsert all configured silver rows into Neo4j and return load counts."""
    if not password:
        raise ValueError("A Neo4j password is required; retrieve it from Databricks Secrets.")

    from neo4j import GraphDatabase

    relationship_dataframes = _build_relationships(spark)
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

            for relationship_type, dataframe, cypher in relationship_dataframes:
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


def verify_neo4j_connection(password):
    """Check Neo4j Aura credentials and network access before data loads begin."""
    if not password:
        raise ValueError("Neo4j password is empty; check the Databricks secret value.")

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USERNAME, password),
    )
    try:
        driver.verify_connectivity()
    finally:
        driver.close()


def main():
    """Run as a Databricks Python file task."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    password = get_neo4j_password(spark)
    results = run_neo4j_job(spark, password)
    for section, values in results.items():
        print(f"{section}: {values}")
    return results


def get_neo4j_password(spark):
    """Read the Neo4j password from the configured Databricks secret scope."""
    from pyspark.dbutils import DBUtils

    try:
        return DBUtils(spark).secrets.get(scope=NEO4J_SCOPE, key=NEO4J_PASSWORD_KEY)
    except Exception as exc:
        raise RuntimeError(
            "Cannot read Neo4j credentials. Create Databricks secret "
            f"scope '{NEO4J_SCOPE}' with key '{NEO4J_PASSWORD_KEY}' and grant READ "
            "permission to the job's Run as identity."
        ) from exc


if __name__ == "__main__":
    main()



    