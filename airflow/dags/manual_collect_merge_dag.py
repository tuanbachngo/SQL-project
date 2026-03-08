from __future__ import annotations

import json
import sys
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

sys.path.append("/opt/airflow")

from include import config
from include.manual_collect import collect_and_write_manual_agg
from include.master_merge import merge_and_write_master_39
from include.missing_tasks import generate_missing_tasks


TZ = pendulum.timezone("Asia/Bangkok")

VAR_MANUAL_IDS = "MANUAL_SHEET_IDS_JSON"

def _get_required_var(name: str) -> str:
    v = Variable.get(name, default_var="")
    if not v:
        raise ValueError(f"Missing Airflow Variable: {name}")
    return v

with DAG(
    dag_id="manual_collect_merge_master_daily",
    start_date=datetime(2026, 2, 1, tzinfo=TZ),
    schedule="0 9 * * *", 
    catchup=False,
    tags=["sheets", "manual", "merge", "master"],
) as dag:

    @task
    def collect_manual():
        master_url = config.MASTER_SPREADSHEET_URL
        manual_ids_json = _get_required_var(VAR_MANUAL_IDS)
        manual_ids = json.loads(manual_ids_json)

        secret_path = config.SECRET_PATH
        manual_tab = config.MANUAL_TAB_NAME

        ws_manual_agg = config.WS_MANUAL_AGG

        collect_and_write_manual_agg(
            secret_path=secret_path,
            manual_spreadsheet_ids=manual_ids,  
            manual_tab_name=manual_tab,
            master_spreadsheet_url=master_url,
            out_ws_name=ws_manual_agg,
        )
        return "manual aggregated"

    @task
    def merge_master():
        master_url = config.MASTER_SPREADSHEET_URL

        secret_path = config.SECRET_PATH

        ws_manual_agg = config.WS_MANUAL_AGG
        ws_extra9 = config.WS_EXTRA9
        ws_master_39 = config.WS_MASTER_39
        ws_ocr = config.WS_CONSOLIDATED_OCR

        extra9_url = None
        fill_from_ocr = True

        merge_and_write_master_39(
            secret_path=secret_path,
            master_spreadsheet_url=master_url,
            manual_agg_ws=ws_manual_agg,
            extra9_ws=ws_extra9,
            master_ws=ws_master_39,
            ocr_ws=ws_ocr,
            extra9_spreadsheet_url=extra9_url,
            fill_from_ocr_if_blank=fill_from_ocr,
        )
        return "master merged"

    @task
    def build_missing():
        master_url = config.MASTER_SPREADSHEET_URL

        secret_path = config.SECRET_PATH

        ws_master_39 = config.WS_MASTER_39
        ws_missing = config.WS_MISSING_TASKS

        assign_map = {t: owner for owner, tickers in config.GROUPS.items() for t in tickers}

        generate_missing_tasks(
            secret_path=secret_path,
            master_spreadsheet_url=master_url,
            master_ws=ws_master_39,
            out_ws=ws_missing,
            required_cols=None,  
            assign_map=assign_map,
        )
        return "missing tasks updated"

    collect_manual() >> merge_master() >> build_missing()