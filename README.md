# Customers Demo Lakehouse Pipeline

This repository packages the Databricks pipeline logic for the `customers_demo` lakehouse into a reusable Python framework that can be run in Databricks or imported into other Python jobs.

## Repository structure

```text
customers_demo/
├── README.md
├── pyproject.toml
├── requirements.txt
├── customers_demo/
│   ├── __init__.py
│   ├── __main__.py
│   ├── config.py
│   ├── ingest.py
│   ├── quality.py
│   └── access.py
├── configs/
│   ├── __init__.py
│   └── source_config.py
├── framework/
│   ├── __init__.py
│   ├── logger.py
│   └── audit_logger.py
├── ingestion/
│   ├── __init__.py
│   └── bronze_loader.py
├── jobs/
│   ├── __init__.py
│   ├── bronze_job.py
│   ├── silver_job.py
│   └── neo4j_job.py
├── notebooks/
│   ├── 01_customers_demo_s3_to_bronze.py
│   ├── 02_bronze_with_dqrules_silver.py
│   ├── 03_silver_tables.py
│   └── 04_silver_to_neo4j.py
├── tests/
│   └── test_bronze_loader.py
├── .gitignore
└── .venv/
```

## Reusable Python framework

This project uses a hybrid pattern so it stays both reusable and reviewer-friendly:

- `customers_demo/` — original installable package for versioned library usage
- `configs/` — centralized source and catalog configuration
- `framework/` — shared logging and execution helpers
- `ingestion/` — bronze loader class and table ingestion logic
- `jobs/` — entry-point runners such as `run_bronze_job`

The main reusable runtime pieces are:

- `customers_demo.config` — centralized configuration and table metadata
- `customers_demo.ingest` — bronze catalog creation, S3 ingestion, and validation
- `customers_demo.quality` — DQ checks and silver-layer transformations
- `customers_demo.access` — user access and grant management
- `ingestion.bronze_loader.BronzeLoader` — modular bronze job object aligned with a framework-style runner pattern

## Install locally

For a standard local Python environment, install the package without pulling Spark dependencies:

```bash
python -m pip install -e . --no-deps
```

If you are running in Databricks or another Spark-enabled environment, install the optional Databricks dependencies as well:

```bash
python -m pip install -e ".[databricks,neo4j]"
```

## Run in Databricks

Upload the configured CSV folders under the S3 `raw/` prefix. To run bronze and silver as one job, configure a single Databricks Python file task with this path:

```text
framework/pipeline.py
```

The framework entry point runs bronze, copies every row and column to silver without null filtering, then loads the graph to Neo4j Aura. A failed stage prevents later stages from running. Notebooks 01 and 02 remain available for manual stage-by-stage runs.

For the single Databricks task, install the `neo4j` driver (for example, the `neo4j` optional dependency) and create the secret scope `neo4j` with the rotated Aura password under key `password`. The graph job uses the relationship mapping documented in `DQrules.md` and validates the declared one-to-one route-call/termination relationship.

Notebook 04 is the earlier call-center graph demo; use `framework/pipeline.py` for the current insurance and interaction graph mapping.

Notebook 01 loads all 13 configured S3 prefixes into bronze. Notebook 02 copies every bronze table and column to silver without null filtering. The obsolete `customers_demo.bronze.customers` and `customers_demo.silver.customers` tables are dropped when their corresponding layer runs; this does not delete S3 objects.

## Prerequisites

- Databricks cluster with Spark enabled
- Access to the S3 bucket `s3://customers-demo-data/`
- Storage credential `assurant-s3-credential`
- Metastore/admin permissions for catalog and external location creation

## Notes

- Bronze ingestion loads raw CSVs from S3 into Delta tables in the `customers_demo.bronze` schema.
- Silver processing currently copies bronze rows and columns as-is, including nulls.
- Access management helper functions can be reused for team grants and validation.
