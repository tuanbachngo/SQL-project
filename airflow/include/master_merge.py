from __future__ import annotations

from typing import Optional

import pandas as pd

from .sheet_utils import (
    DEFAULT_SCOPES,
    detect_key_cols,
    get_gspread_client,
    normalize_ticker_year,
    open_or_create_worksheet,
    open_spreadsheet,
    read_worksheet_to_df,
    write_df_to_worksheet,
)


def _is_blank_series(s: pd.Series) -> pd.Series:
    return s.isna() | (s.astype(str).str.strip() == "")


def _coalesce_fill_blank(base: pd.DataFrame, incoming: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in incoming.columns:
            continue
        if c not in base.columns:
            base[c] = ""
        mask = _is_blank_series(base[c])
        base.loc[mask, c] = incoming.loc[mask, c]
    return base


def merge_master_39_df(
    manual_df: pd.DataFrame,
    extra9_df: pd.DataFrame,
    ocr_df: Optional[pd.DataFrame] = None,
    *,
    fill_from_ocr_if_blank: bool = True,
) -> pd.DataFrame:
    if manual_df.empty and extra9_df.empty and (ocr_df is None or ocr_df.empty):
        return pd.DataFrame()

    key = detect_key_cols(manual_df if not manual_df.empty else (extra9_df if not extra9_df.empty else ocr_df))
    ticker_col, year_col = key.ticker, key.year

    def prep(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        normalize_ticker_year(df, ticker_col, year_col, upper=True)
        df = df[df[year_col].notna()].copy()
        df[year_col] = df[year_col].astype(int)
        drop_cols = [c for c in df.columns if c.startswith("_") and c not in [ticker_col, year_col]]
        if drop_cols:
            df.drop(columns=drop_cols, inplace=True, errors="ignore")
        return df

    manual = prep(manual_df)
    extra9 = prep(extra9_df)
    ocr = prep(ocr_df) if ocr_df is not None else pd.DataFrame()

    if not manual.empty:
        manual = manual.set_index([ticker_col, year_col])
    if not extra9.empty:
        extra9 = extra9.set_index([ticker_col, year_col])
    if not ocr.empty:
        ocr = ocr.set_index([ticker_col, year_col])

    idx = None
    for df in [manual, extra9, ocr]:
        if df is None or df.empty:
            continue
        idx = df.index if idx is None else idx.union(df.index)

    if idx is None:
        return pd.DataFrame()

    if manual.empty:
        base = pd.DataFrame(index=idx)
    else:
        base = manual.reindex(idx)

    if not extra9.empty:
        for c in extra9.columns:
            if c not in base.columns:
                base[c] = ""

    if not extra9.empty:
        extra9_aligned = extra9.reindex(idx)
        base = _coalesce_fill_blank(base, extra9_aligned, cols=list(extra9_aligned.columns))

    if fill_from_ocr_if_blank and not ocr.empty:
        ocr_aligned = ocr.reindex(idx)
        base = _coalesce_fill_blank(base, ocr_aligned, cols=list(ocr_aligned.columns))

    base = base.reset_index()
    front = [ticker_col, year_col]
    rest = sorted([c for c in base.columns if c not in front])
    out = base[front + rest]

    out.sort_values(by=[ticker_col, year_col], inplace=True)
    out.fillna("", inplace=True)
    return out


def merge_and_write_master_39(
    *,
    secret_path: str,
    master_spreadsheet_url: str,
    manual_agg_ws: str,
    extra9_ws: str,
    master_ws: str,
    ocr_ws: Optional[str] = None,
    extra9_spreadsheet_url: Optional[str] = None,
    fill_from_ocr_if_blank: bool = True,
) -> None:
    gc = get_gspread_client(secret_path, scopes=DEFAULT_SCOPES)
    master = open_spreadsheet(gc, master_spreadsheet_url)

    manual_df = read_worksheet_to_df(master.worksheet(manual_agg_ws)) if manual_agg_ws else pd.DataFrame()
    
    if extra9_spreadsheet_url:
        extra_sh = open_spreadsheet(gc, extra9_spreadsheet_url)
        extra_df = read_worksheet_to_df(extra_sh.worksheet(extra9_ws))
    else:
        extra_df = read_worksheet_to_df(master.worksheet(extra9_ws)) if extra9_ws else pd.DataFrame()

    ocr_df = pd.DataFrame()
    if ocr_ws:
        ocr_df = read_worksheet_to_df(master.worksheet(ocr_ws))

    merged = merge_master_39_df(
        manual_df=manual_df,
        extra9_df=extra_df,
        ocr_df=ocr_df,
        fill_from_ocr_if_blank=fill_from_ocr_if_blank,
    )

    out_ws = open_or_create_worksheet(master, master_ws, rows=max(2000, len(merged) + 50), cols=max(60, len(merged.columns) + 5))
    write_df_to_worksheet(out_ws, merged, clear_first=True)
