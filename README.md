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
python -m pip install -e .[databricks]
```

## Run in Databricks

Execute these files in order:

```text
notebooks/01_customers_demo_s3_to_bronze.py
notebooks/02_bronze_with_dqrules_silver.py
notebooks/03_silver_tables.py
notebooks/04_silver_to_neo4j.py
```

To schedule the Neo4j load, configure a Databricks Python file task for `jobs/neo4j_job.py` after the silver job. Add the `neo4j` Python driver as a task or cluster dependency, and create the Databricks secret scope `neo4j` with the rotated Aura password stored under key `password`. The job uses the Aura URI and username configured in the file, loads all configured silver rows as nodes, and creates relationships only from matching silver keys. It uses `MERGE` and does not clear the graph.

Notebook 04 is also available for interactive runs from a Databricks Git folder.

Each notebook entry point expects a live `spark` session and calls the shared package logic instead of duplicating SQL inline.

## Prerequisites

- Databricks cluster with Spark enabled
- Access to the S3 bucket `s3://customers-demo-data/`
- Storage credential `assurant-s3-credential`
- Metastore/admin permissions for catalog and external location creation

## Notes

- Bronze ingestion loads raw CSVs from S3 into Delta tables in the `customers_demo.bronze` schema.
- Silver processing drops rows where any business-critical field is null.
- Access management helper functions can be reused for team grants and validation.
