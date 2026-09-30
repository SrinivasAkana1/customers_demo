"""Smoke tests for the bronze loader structure."""

from ingestion.bronze_loader import BronzeLoader


def test_bronze_loader_has_run_method():
    assert hasattr(BronzeLoader, "run")
