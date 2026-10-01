"""Tests for the single bronze-to-silver orchestration entry point."""

from framework import pipeline


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

    monkeypatch.setattr(pipeline, "run_bronze_pipeline", run_bronze)
    monkeypatch.setattr(pipeline, "copy_bronze_to_silver", run_silver)

    result = pipeline.run_pipeline(spark)

    assert calls == ["bronze", "silver"]
    assert result == {"bronze": ["bronze result"], "silver": ["silver result"]}