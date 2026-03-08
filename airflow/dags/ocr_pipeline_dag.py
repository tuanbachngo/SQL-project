from __future__ import annotations

import sys
import os
import json
from typing import Optional, List
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from google.cloud import storage

sys.path.append("/opt/airflow")

from include import config
from include.collect_upload_gcs import list_pdfs_in_gcs
from include.processing import process_one_pdf_gcs_to_csv
from include.gcs_to_sheets import upload_csvs_to_sheets
from include.gop_du_lieu import process_and_upload_financial_data
from include.split_manual_files import split_consolidated_to_manual_files

SRC_BUCKET = "raw_information"
SRC_PREFIX = ""  
DEST_BUCKET = "text_ocr_output"

SKIP_IF_EXISTS = True

YEARS: Optional[List[int]] = None

def infer_ticker_from_gs_uri(gs_pdf_uri: str) -> str:
    assert gs_pdf_uri.startswith("gs://")
    path = gs_pdf_uri.split("gs://", 1)[1].split("/", 1)[1] 
    ticker = path.split("/", 1)[0]
    return ticker


with DAG(
    dag_id="gcs_pdf_to_csv_gemini_all_folders",
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    tags=["gcs", "gemini", "ocr", "csv"],
) as dag:

    @task
    def list_all_pdfs() -> list[str]:
        client = storage.Client()
        return list_pdfs_in_gcs(SRC_BUCKET, SRC_PREFIX, client)

    @task(pool="vertex_pool", retries=8, retry_delay=timedelta(minutes=1))
    def process_one(gs_pdf_uri: str) -> str:
        ticker = infer_ticker_from_gs_uri(gs_pdf_uri)
        dest_prefix = f"1_raw_ocr/{ticker}_csv"

        client = storage.Client()

        out_uri = process_one_pdf_gcs_to_csv(
            gs_pdf_uri=gs_pdf_uri,
            dest_bucket=DEST_BUCKET,
            dest_prefix=dest_prefix,
            client=client,
            skip_if_exists=SKIP_IF_EXISTS,
            company=ticker,  
            years=YEARS,     
        )
        return out_uri
    
    @task
    def sync_to_sheets_individual():
        upload_csvs_to_sheets(
            spreadsheet_title=config.DICTIONARY_SPREADSHEET,
            project_id=os.getenv("GOOGLE_CLOUD_PROJECT", ""),
        )
        return "Sync to Sheets Completed"
    
    @task
    def sync_to_sheets_consolidated():
        process_and_upload_financial_data(
            project_id=os.getenv("GOOGLE_CLOUD_PROJECT", ""),
            secret_path=config.SECRET_PATH , 
            spreadsheet_url=config.MASTER_SPREADSHEET_URL,
            worksheet_name=config.WS_CONSOLIDATED_OCR,
            mode="overwrite",
        )
        return "Consolidated Sheet Sync Completed"
    
    @task
    def split_to_manual_files():
        existing_ids = json.loads(Variable.get("MANUAL_SHEET_IDS_JSON", default_var="{}") or "{}")
        ids = split_consolidated_to_manual_files(
            secret_path=config.SECRET_PATH,
            master_spreadsheet_url=config.MASTER_SPREADSHEET_URL,
            consolidated_ws_name=config.WS_CONSOLIDATED_OCR,
            groups=config.GROUPS,
            out_tab_name=config.MANUAL_TAB_NAME,
            share_with_emails=config.TEAM_EDITORS,          
            existing_manual_ids=existing_ids if existing_ids else None,     
        )
        Variable.set("MANUAL_SHEET_IDS_JSON", json.dumps(ids))
        return ids

    pdfs = list_all_pdfs()
    out_csvs = process_one.expand(gs_pdf_uri=pdfs)

    individual = sync_to_sheets_individual()
    consolidated = sync_to_sheets_consolidated()

    out_csvs >> [individual, consolidated]

    manual_ids = split_to_manual_files()
    consolidated >> manual_ids 