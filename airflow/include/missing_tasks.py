from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

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


def _is_blank(v) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s.lower() in {"nan", "none", "null"}


def generate_missing_tasks_df(
    master_df: pd.DataFrame,
    required_cols: Optional[Sequence[str]] = None,
    assign_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    if master_df.empty:
        return pd.DataFrame(columns=["ticker", "fiscal_year", "missing_var", "assigned_to", "status", "note"])

    key = detect_key_cols(master_df)
    ticker_col, year_col = key.ticker, key.year

    df = master_df.copy()
    normalize_ticker_year(df, ticker_col, year_col, upper=True)
    df = df[df[year_col].notna()].copy()
    df[year_col] = df[year_col].astype(int)

    if required_cols is None:
        required_cols = [c for c in df.columns if c not in [ticker_col, year_col]]

    rows: List[Dict[str, str]] = []
    assign_map = assign_map or {}

    for _, r in df.iterrows():
        ticker = str(r[ticker_col]).strip().upper()
        year = int(r[year_col])
        for col in required_cols:
            if col not in df.columns:
                continue
            if _is_blank(r[col]):
                rows.append(
                    {
                        "ticker": ticker,
                        "fiscal_year": year,
                        "missing_var": col,
                        "assigned_to": assign_map.get(ticker, ""),
                        "status": "todo",
                        "note": "",
                    }
                )

    out = pd.DataFrame(rows)
    if not out.empty:
        out.sort_values(by=["assigned_to", "ticker", "fiscal_year", "missing_var"], inplace=True)
    return out


def generate_missing_tasks(
    *,
    secret_path: str,
    master_spreadsheet_url: str,
    master_ws: str,
    out_ws: str,
    required_cols: Optional[Sequence[str]] = None,
    assign_map: Optional[Dict[str, str]] = None,
) -> None:
    gc = get_gspread_client(secret_path, scopes=DEFAULT_SCOPES)
    sh = open_spreadsheet(gc, master_spreadsheet_url)
    df = read_worksheet_to_df(sh.worksheet(master_ws))

    tasks_df = generate_missing_tasks_df(df, required_cols=required_cols, assign_map=assign_map)

    outw = open_or_create_worksheet(sh, out_ws, rows=max(2000, len(tasks_df) + 50), cols=max(20, len(tasks_df.columns) + 2))
    write_df_to_worksheet(outw, tasks_df, clear_first=True)
