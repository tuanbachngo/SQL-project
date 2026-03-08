from __future__ import annotations

from typing import Dict

MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1A9HWNzbaXdikJsOGZIRQzOUoYOAaxLh5YshDPjqyN_c/edit?usp=sharing"

DICTIONARY_SPREADSHEET = "OCR_Financial_Reports_Dictionary"

WS_CONSOLIDATED_OCR = "CONSOLIDATED_30_OCR"
WS_MANUAL_AGG = "CONSOLIDATED_30_MANUAL_AGG"
WS_EXTRA9 = "EXTRA9_INPUT"
WS_MASTER_39 = "MASTER_39"
WS_MISSING_TASKS = "MISSING_TASKS"

MANUAL_TAB_NAME = "Sheet1"

SECRET_PATH = "/opt/airflow/secrets/gsheet-editor.json"

TEAM_EDITORS = [
                "duynguyen260406@gmail.com",
                "vuanhson06@gmail.com", 
                "binhthai0203@gmail.com",
                "huyzed170206@gmail.com",
                "tuanbachngo@gmail.com",
                ]

GROUPS = {"Duy" : ["BHN","BSR","DBC","DGC"], 
          "Son" : ["DPM","NKG","PC1","PVC"],
          "Bach" : ["PVD","PVS","RAL","TMS"],
          "Binh" : ["DHG","GEE","GMD","GVR"],
          "Huy" : ["VCG","VGC","VHC","VSC"]
          }

MANUAL_SHEET_IDS: Dict[str, str] = {}
