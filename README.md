# SQL Project: Vietnamese Firm Panel Data Pipeline

This project is an end-to-end SQL and data engineering workflow for building a structured firm-year panel dataset of Vietnamese listed companies. The repository combines relational database design, Python ETL scripts, market data collection, quality control checks, and an Airflow-based OCR pipeline to turn raw financial reports into a clean and research-ready dataset.

The main goal of the project is to standardize data from multiple sources into one consistent warehouse model, then produce a final panel containing 39 business, financial, ownership, market, innovation, and firm-level variables. In practice, the project covers both the database layer and the operational workflow needed to collect, clean, validate, and export the final dataset.

## What This Project Does

- Designs a MySQL warehouse using a dimensional model with dimension, fact, snapshot, and audit tables.
- Loads firm metadata, source metadata, and panel data from Excel into SQL.
- Tracks data versions through yearly snapshots.
- Fetches external share prices for selected Vietnamese tickers.
- Runs QC checks to detect missing values, logical inconsistencies, accounting imbalances, and outliers.
- Applies quick fixes while keeping an override log for traceability.
- Exports the latest firm-year panel from SQL to CSV.
- Automates OCR extraction from annual-report PDFs stored in Google Cloud Storage.
- Pushes OCR outputs to Google Sheets for manual review, consolidation, and missing-task tracking with Airflow.

## Data Model Overview

The warehouse is built around the `vn_firm_panel_test` database and follows a fact-and-dimension structure:

- Dimension tables store exchanges, industries, data sources, and firm master data.
- Fact tables store yearly ownership, financial, cashflow, market, innovation, and firm metadata.
- Snapshot tables record versioned data loads by fiscal year and source.
- Audit tables log manual corrections for transparency.
- The view `vw_firm_panel_latest` produces the latest firm-year panel by selecting the newest snapshot for each company-year combination.

The final dataset is organized around firm-year observations and includes variable groups such as:

- Ownership structure
- Financial statement variables
- Cash flow indicators
- Market indicators
- Innovation dummies and evidence notes
- Firm characteristics such as age and employee count

## Project Workflow

The current workflow in this repository looks like this:

1. Create the warehouse schema with [`etl/schema_and_seed.sql`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/schema_and_seed.sql).
2. Load company and source metadata with [`etl/import_firms.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/import_firms.py).
3. Create yearly data snapshots with [`etl/create_snapshot.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/create_snapshot.py).
4. Import the consolidated panel into fact tables with [`etl/import_panel.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/import_panel.py).
5. Enrich market data using [`etl/fetch_prices.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/fetch_prices.py).
6. Run validation checks through [`etl/qc_checks.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/qc_checks.py).
7. Apply corrections and log overrides with [`etl/quick_fix.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/quick_fix.py).
8. Export the final dataset from [`etl/export_panel.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/export_panel.py).

Alongside the local ETL flow, the Airflow pipelines automate document processing:

- [`airflow/dags/ocr_pipeline_dag.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/airflow/dags/ocr_pipeline_dag.py) processes PDFs from Google Cloud Storage, converts them into CSV outputs, syncs them to Google Sheets, and prepares manual review files.
- [`airflow/dags/manual_collect_merge_dag.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/airflow/dags/manual_collect_merge_dag.py) collects manual inputs, merges them into the master 39-variable sheet, and generates missing-task tracking.

## Repository Structure

```text
SQL-project/
|-- README.md
|-- external_share_prices.csv
|-- etl/
|   |-- schema_and_seed.sql
|   |-- database_setup.py
|   |-- import_firms.py
|   |-- create_snapshot.py
|   |-- import_panel.py
|   |-- fetch_prices.py
|   |-- qc_checks.py
|   |-- quick_fix.py
|   `-- export_panel.py
`-- airflow/
    |-- docker-compose.yaml
    |-- dockerfile
    |-- requirements.txt
    |-- dags/
    `-- include/
```

## Tech Stack

- SQL / MySQL
- Python
- Pandas
- SQLAlchemy and PyMySQL
- Apache Airflow
- Docker Compose
- Google Cloud Storage
- Google Sheets integrations via `gspread`
- OCR / document extraction workflow using Google AI tooling

## Why This Project Matters

This repository shows how SQL can be used as the backbone of a real data pipeline, not just for isolated queries. Instead of stopping at schema design, the project connects database modeling with automated ingestion, data validation, manual review, and repeatable exports. The result is a practical workflow for transforming raw annual reports and supporting files into a usable analytical dataset.

## Notes

- The project currently mixes local ETL scripts and cloud-based orchestration, which makes it useful both for learning SQL data warehousing and for building a production-style data workflow.
- Some scripts still assume local credentials, local file names, or environment-specific settings. Before sharing or deploying, those values should be moved into environment variables or configuration files.
