import time
import pandas as pd
import gcsfs

from include import config
from include.sheet_utils import (
    get_gspread_client,
    open_or_create_worksheet,
    write_df_to_worksheet,
)

def upload_csvs_to_sheets(
    *,
    spreadsheet_title: str = "OCR_Financial_Reports_Dictionary",
    project_id: str,
    gcs_glob_path: str = "gs://text_ocr_output/1_raw_ocr/**/*.csv",
    sleep_seconds: float = 2.0,
) -> None:
    gc = get_gspread_client(config.SECRET_PATH)
    
    sh = gc.open(spreadsheet_title)

    existing_sheet_names = [ws.title for ws in sh.worksheets()]

    fs = gcsfs.GCSFileSystem(project=project_id)
    all_files = fs.glob(gcs_glob_path)

    for file_path in all_files:
        file_path_str = str(file_path)
        sheet_name = file_path_str.split('/')[-1].replace('.csv', '')
        
        if sheet_name in existing_sheet_names:
            print(f"Bỏ qua: {sheet_name} đã tồn tại trên Sheets.")
            continue

        gs_path = file_path_str if file_path_str.startswith("gs://") else f"gs://{file_path_str}"
        df = pd.read_csv(gs_path).fillna("")

        try:
            ws = open_or_create_worksheet(
                sh,
                title=sheet_name,
                rows=max(100, len(df) + 5),
                cols=max(20, len(df.columns) + 5),
            )
            write_df_to_worksheet(ws, df, clear_first=True, include_index=False)
            print(f"Đã đẩy thành công file mới: {sheet_name}")
            time.sleep(sleep_seconds)
        except Exception as e:
            print(f"Lỗi khi xử lý {sheet_name}: {e}")