<div align="center">

# SQL Project: Vietnamese Firm Panel Data Pipeline

</div>

This project is an end-to-end SQL and data engineering workflow for building a structured firm-year panel dataset of Vietnamese listed companies. The repository combines relational database design, Python ETL scripts, market data collection, quality control checks, and an Airflow-based LLM pipeline to turn raw financial reports into a clean and research-ready dataset.

The main goal of the project is to standardize data from multiple sources into one consistent warehouse model, then produce a final panel containing 39 business, financial, ownership, market, innovation, and firm-level variables. In practice, the project covers both the database layer and the operational workflow needed to collect, clean, validate, and export the final dataset.

---

## What This Project Does

- Extracts financial data from annual-report PDFs stored in Google Cloud Storage.
- Converts LLM output into sheet-based review workflows for manual cleanup.
- Merges reviewed data into a consolidated master sheet with 39 variables.
- Designs and loads a MySQL warehouse for firm-year panel analysis.
- Creates versioned yearly snapshots for traceable data loads.
- Imports firm metadata, source metadata, and panel values into SQL fact tables.
- Runs QC checks and supports documented manual fixes.
- Exports the latest research-ready panel from SQL to CSV.

---

## Core Workflow

This repository works in two stages:

### Stage 1: Airflow extraction and consolidation

The `airflow/` folder is the starting point of the project.

1. [`airflow/dags/llm_pipeline_dag.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/airflow/dags/llm_pipeline_dag.py) lists PDFs in Google Cloud Storage, runs LLM extraction, writes CSV outputs, syncs results to Google Sheets, and prepares manual review files.
2. The same DAG consolidates LL output into a shared worksheet and splits it into team-specific manual collection sheets.
3. [`airflow/dags/manual_collect_merge_dag.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/airflow/dags/manual_collect_merge_dag.py) collects manual edits, merges them into the master 39-variable sheet, and generates missing-task tracking.

Output of this stage:
The reviewed and merged spreadsheet that becomes the source input for the downstream SQL ETL process.

### Stage 2: SQL ETL and export

After Airflow has produced the reviewed dataset, the `etl/` scripts load and manage it in MySQL.

1. Create the warehouse schema with [`etl/schema_and_seed.sql`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/schema_and_seed.sql).
2. Configure the database connection with [`etl/database_setup.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/database_setup.py).
3. Load firm and source metadata with [`etl/import_firms.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/import_firms.py).
4. Create yearly snapshot records with [`etl/create_snapshot.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/create_snapshot.py).
5. Import the consolidated panel into fact tables with [`etl/import_panel.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/import_panel.py).
6. Enrich market data with [`etl/fetch_prices.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/fetch_prices.py).
7. Run validation checks through [`etl/qc_checks.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/qc_checks.py).
8. Apply documented corrections with [`etl/quick_fix.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/quick_fix.py).
9. Export the final dataset with [`etl/export_panel.py`](/C:/Users/Admin/Downloads/SQL/SQL-project/etl/export_panel.py).

---

## Data Model Overview

The warehouse is built around the `vn_firm_panel_test` database and follows a dimensional design:

- Dimension tables store firms, exchanges, industries, and data sources.
- Fact tables store yearly ownership, market, cashflow, financial, innovation, and firm metadata.
- Snapshot tables version each fiscal-year load by source and tag.
- Audit-style correction logic helps preserve traceability when quick fixes are applied.
- The view `vw_firm_panel_latest` exposes the latest firm-year version for export and analysis.

The final panel is organized at the firm-year level and includes variables across:

- Ownership structure
- Financial statements
- Cash flow
- Market indicators
- Innovation flags and evidence notes
- Firm characteristics such as age and employee count

---

## Repository Structure

```text
SQL-project/
|-- README.md
|-- external_share_prices.csv
|-- airflow/
|   |-- docker-compose.yaml
|   |-- dockerfile
|   |-- requirements.txt
|   |-- dags/
|   |   |-- llm_pipeline_dag.py
|   |   `-- manual_collect_merge_dag.py
|   `-- include/
|-- assets/
`-- etl/
    |-- schema_and_seed.sql
    |-- database_setup.py
    |-- import_firms.py
    |-- create_snapshot.py
    |-- import_panel.py
    |-- fetch_prices.py
    |-- qc_checks.py
    |-- quick_fix.py
    `-- export_panel.py
```

---

## Tech Stack

- SQL / MySQL
- Python
- Pandas
- SQLAlchemy and PyMySQL
- Apache Airflow
- Docker Compose
- Google Cloud Storage
- Google Sheets via `gspread`

---

## Notes

- Some scripts still contain environment-specific paths, credentials, and local file assumptions that should be moved into environment variables or config before broader use.
- The Airflow layer and ETL layer are tightly connected: changing the review-sheet structure upstream can affect downstream ETL expectations.
