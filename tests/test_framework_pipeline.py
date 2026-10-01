"""Tests for the single bronze-to-silver orchestration entry point."""

from framework import pipeline
from customers_demo.config import BRONZE_TABLES
from jobs.neo4j_job import NODE_CONFIG, RELATIONSHIP_RULES, SILVER_TABLES


def test_run_pipeline_executes_bronze_before_silver(monkeypatch):
    calls = []
    spark = object()

    def run_bronze(received_spark):
        assert received_spark is spark
        calls.append("bronze")
        return ["bronze result"]

    def run_silver(received_spark):
        assert received_spark is spark
        calls.append("silver")
        return ["silver result"]

    def run_neo4j(received_spark, password):
        assert received_spark is spark
        assert password == "test-password"
        calls.append("neo4j")
        return ["neo4j result"]

    monkeypatch.setattr(pipeline, "run_bronze_pipeline", run_bronze)
    monkeypatch.setattr(pipeline, "copy_bronze_to_silver", run_silver)
    monkeypatch.setattr(pipeline, "run_neo4j_job", run_neo4j)

    result = pipeline.run_pipeline(spark, "test-password")

    assert calls == ["bronze", "silver", "neo4j"]
    assert result == {
        "bronze": ["bronze result"],
        "silver": ["silver result"],
        "neo4j": ["neo4j result"],
    }


def test_run_pipeline_retrieves_secret_after_silver(monkeypatch):
    calls = []
    spark = object()

    monkeypatch.setattr(pipeline, "run_bronze_pipeline", lambda _: calls.append("bronze"))
    monkeypatch.setattr(pipeline, "copy_bronze_to_silver", lambda _: calls.append("silver"))
    monkeypatch.setattr(
        pipeline,
        "get_neo4j_password",
        lambda _: calls.append("get_password") or "secret-value",
    )
    monkeypatch.setattr(
        pipeline,
        "run_neo4j_job",
        lambda _, password: calls.append(("neo4j", password)) or {"loaded": True},
    )

    pipeline.run_pipeline(spark)

    assert calls == ["bronze", "silver", "get_password", ("neo4j", "secret-value")]


def test_neo4j_mapping_covers_all_configured_silver_tables():
    assert set(NODE_CONFIG) == set(BRONZE_TABLES)
    assert len(SILVER_TABLES) == 13


def test_neo4j_mapping_contains_all_requested_relationships():
    assert len(RELATIONSHIP_RULES) == 12
    assert RELATIONSHIP_RULES[-1] == (
        "route_call_details",
        "terminate_calls",
        "route_call_sk",
        "HAS_TERMINATION",
        "1:1",
    )