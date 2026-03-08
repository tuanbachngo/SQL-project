import pandas as pd
import re
import os
import logging
from typing import Literal, Optional, List

import gcsfs

from include.sheet_utils import (
    get_gspread_client,
    open_spreadsheet,
    open_or_create_worksheet,
    read_worksheet_to_df,
    write_df_to_worksheet,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def clean_currency_string(val):
   
    if pd.isna(val):
        return None
    s = str(val).strip()
    if "(" in s and ")" in s:
        s = s.replace("(", "-").replace(")", "")
    s = s.replace(".", "")
    s = re.sub(r"[^\d\-]", "", s)
    try:
        return float(s)
    except ValueError:
        return None


def _fallback_find_csvs(fs: gcsfs.GCSFileSystem, gcs_glob_path: str, debug_samples: int = 3) -> List[str]:

    m = re.match(r"gs://([^/]+)/(.+?)/\*\*/\*\.csv", gcs_glob_path)
    if not m:
        logging.warning("Glob ra 0 file và không suy ra được prefix để find. Bạn nên đổi glob hoặc truyền prefix rõ hơn.")
        return []

    bucket = m.group(1)
    prefix = m.group(2)
    logging.warning(f"Glob ra 0 file. Thử fs.find() với prefix: {bucket}/{prefix}")
    found = fs.find(f"{bucket}/{prefix}")
    csvs = [f for f in found if f.endswith(".csv")]
    logging.info(f"fs.find found {len(csvs)} csv files. Sample={csvs[:min(debug_samples, len(csvs))]}")
    return csvs


def _resolve_duplicates_company_year(df: pd.DataFrame, source_col: str = "_source_file") -> pd.DataFrame:

    if df.empty:
        return df

    if source_col not in df.columns:
        return df.drop_duplicates(subset=["ticker", "fiscal_year"], keep="last")

    df = df.sort_values(by=["ticker", "fiscal_year", source_col]).copy()

    def _pick_last_filled(g: pd.DataFrame) -> pd.Series:
        g2 = g.ffill()
        return g2.iloc[-1]

    out = df.groupby(["ticker", "fiscal_year"], as_index=False).apply(_pick_last_filled)
    out = out.reset_index(drop=True)

    return out


def process_and_upload_financial_data(
    project_id: str,
    secret_path: str,
    spreadsheet_url: str = "",
    worksheet_name: str = "CONSOLIDATED_30_OCR",
    gcs_glob_path: str = "gs://text_ocr_output/1_raw_ocr/**/*.csv",
    debug_samples: int = 3,
    mode: Literal["overwrite", "append_new_keys"] = "overwrite",
):
 
    try:
        logging.info("Kết nối Google Sheets...")
        gc = get_gspread_client(secret_path)
        sh = open_spreadsheet(gc, spreadsheet_url)
        ws = open_or_create_worksheet(sh, worksheet_name, rows=2000, cols=80)

        df_existing = pd.DataFrame()
        existing_keys = set()

        if mode == "append_new_keys":
            df_existing = read_worksheet_to_df(ws)
            if not df_existing.empty and "ticker" in df_existing.columns and "fiscal_year" in df_existing.columns:
                existing_keys = set(df_existing["ticker"].astype(str) + "_" + df_existing["fiscal_year"].astype(str))
                logging.info(f"Phát hiện {len(existing_keys)} bản ghi đã tồn tại trên Sheets.")
            else:
                logging.info("Sheet hiện tại đang trống (append mode).")

        logging.info("Đang quét CSV trên GCS...")
        fs = gcsfs.GCSFileSystem(project=project_id, token=secret_path)

        logging.info(f"GCS glob pattern = {gcs_glob_path}")
        all_files = fs.glob(gcs_glob_path)
        logging.info(f"GCS glob found {len(all_files)} files")
        if all_files:
            logging.info(f"GCS sample files = {all_files[:min(debug_samples, len(all_files))]}")

        if len(all_files) == 0:
            all_files = _fallback_find_csvs(fs, gcs_glob_path, debug_samples=debug_samples)

        if len(all_files) == 0:
            logging.warning("Không tìm thấy file CSV nào trên GCS theo pattern/prefix đã cho.")
            return

        processed_data = []

        cnt_total = 0
        cnt_skip_no_year_in_name = 0
        cnt_read_ok = 0
        cnt_read_fail = 0
        cnt_year_filter_empty = 0
        cnt_pivot_ok = 0
        cnt_pivot_fail = 0

        for file_path in all_files:
            cnt_total += 1
            filename_only = os.path.basename(file_path)

            match = re.search(r"(\d{4})", filename_only)
            if not match:
                cnt_skip_no_year_in_name += 1
                if cnt_skip_no_year_in_name <= debug_samples:
                    logging.warning(f"SKIP(no year in filename): {filename_only}")
                continue

            target_year = int(match.group(1))

            if cnt_total <= debug_samples:
                logging.info(f"Processing file_path={file_path} (filename={filename_only}, target_year={target_year})")

            try:
                read_path = file_path if str(file_path).startswith("gs://") else f"gs://{file_path}"

                df = pd.read_csv(
                    read_path,
                    header=None,
                    usecols=[0, 1, 2, 4],
                    names=["ticker", "fiscal_year", "key", "value"],
                    storage_options={"token": secret_path},
                )
                cnt_read_ok += 1

                if cnt_read_ok <= debug_samples:
                    logging.info(
                        f"READ_OK {filename_only}: shape={df.shape}, "
                        f"year_dtype={df['fiscal_year'].dtype}, year_head={df['fiscal_year'].head(5).tolist()}, "
                        f"key_head={df['key'].head(5).tolist()}"
                    )

                df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce")
                df = df.dropna(subset=["fiscal_year"])
                df["fiscal_year"] = df["fiscal_year"].astype(int)

                df_filtered = df[df["fiscal_year"] == target_year].copy()
                if df_filtered.empty:
                    cnt_year_filter_empty += 1
                    if cnt_year_filter_empty <= debug_samples:
                        years_present = sorted(df["fiscal_year"].unique().tolist())
                        logging.warning(
                            f"FILTER_EMPTY {filename_only}: target_year={target_year}, "
                            f"years_present(sample up to 10)={years_present[:10]}"
                        )
                    continue

                df_filtered["value"] = df_filtered["value"].apply(clean_currency_string)

                try:
                    df_pivot = (
                        df_filtered.pivot_table(
                            index=["ticker", "fiscal_year"],
                            columns="key",
                            values="value",
                            aggfunc="first",
                        )
                        .reset_index()
                    )
                    df_pivot["_source_file"] = str(file_path)
                    cnt_pivot_ok += 1

                    if cnt_pivot_ok <= debug_samples:
                        logging.info(
                            f"PIVOT_OK {filename_only}: pivot_shape={df_pivot.shape}, "
                            f"columns_sample={list(df_pivot.columns)[:10]}"
                        )

                    processed_data.append(df_pivot)
                except Exception as pe:
                    cnt_pivot_fail += 1
                    logging.error(f"PIVOT_FAIL {filename_only}: {pe}", exc_info=True)

            except Exception as e:
                cnt_read_fail += 1
                logging.error(f"READ_FAIL {filename_only} (path={file_path}): {e}", exc_info=True)

        logging.info(
            "GCS PROCESS SUMMARY | "
            f"total_files={cnt_total} | "
            f"skip_no_year_in_filename={cnt_skip_no_year_in_name} | "
            f"read_ok={cnt_read_ok} | read_fail={cnt_read_fail} | "
            f"year_filter_empty={cnt_year_filter_empty} | "
            f"pivot_ok={cnt_pivot_ok} | pivot_fail={cnt_pivot_fail} | "
            f"processed_data_count={len(processed_data)}"
        )

        if not processed_data:
            logging.warning("Không có dữ liệu hợp lệ nào được lấy từ GCS (processed_data rỗng).")
            return

        final_df = pd.concat(processed_data, ignore_index=True)

        final_df["ticker"] = final_df["ticker"].astype(str).str.strip().str.upper()
        final_df["fiscal_year"] = pd.to_numeric(final_df["fiscal_year"], errors="coerce").astype("Int64")

        final_df = _resolve_duplicates_company_year(final_df, source_col="_source_file")

        if mode == "append_new_keys":
            final_df["temp_key"] = final_df["ticker"].astype(str) + "_" + final_df["fiscal_year"].astype(str)
            new_data = final_df[~final_df["temp_key"].isin(existing_keys)].copy()
            new_data.drop(columns=["temp_key"], inplace=True)

            if new_data.empty:
                logging.info("BỎ QUA: Tất cả dữ liệu đã có sẵn trên Sheets. Không có gì mới để cập nhật.")
                return

            combined_df = pd.concat([df_existing, new_data], ignore_index=True) if not df_existing.empty else new_data
        else:
            combined_df = final_df.copy()

        if "_source_file" in combined_df.columns:
            combined_df.drop(columns=["_source_file"], inplace=True, errors="ignore")

        combined_df = combined_df.dropna(subset=["ticker", "fiscal_year"])
        combined_df["fiscal_year"] = combined_df["fiscal_year"].astype(int)
        combined_df.sort_values(by=["ticker", "fiscal_year"], inplace=True)
        combined_df.fillna("", inplace=True)

        first_two = ["ticker", "fiscal_year"]
        for c in first_two:
            if c not in combined_df.columns:
                combined_df[c] = ""

        rest = sorted([c for c in combined_df.columns if c not in first_two])
        combined_df = combined_df[first_two + rest]

        write_df_to_worksheet(ws, combined_df, clear_first=True)
        logging.info(f"HOÀN THÀNH! Đã upload dữ liệu lên worksheet '{worksheet_name}' (mode={mode}).")

    except Exception as main_e:
        logging.error(f"Đã xảy ra lỗi nghiêm trọng trong luồng xử lý: {main_e}", exc_info=True)
        raise