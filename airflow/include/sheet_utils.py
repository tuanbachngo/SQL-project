from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe


DEFAULT_SCOPES: List[str] = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


@dataclass(frozen=True)
class KeyCols:
    ticker: str = "ticker"
    year: str = "fiscal_year"


def get_gspread_client(secret_path: str, scopes: Optional[Sequence[str]] = None) -> gspread.Client:
    return gspread.service_account(filename=secret_path, scopes=list(scopes or DEFAULT_SCOPES))


def open_spreadsheet(gc: gspread.Client, spreadsheet_url_or_id: str) -> gspread.Spreadsheet:
    if spreadsheet_url_or_id.startswith("http"):
        return gc.open_by_url(spreadsheet_url_or_id)
    return gc.open_by_key(spreadsheet_url_or_id)


def open_or_create_worksheet(
    sh: gspread.Spreadsheet,
    title: str,
    rows: int = 2000,
    cols: int = 60,
) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def read_worksheet_to_df(ws: gspread.Worksheet) -> pd.DataFrame:
    records = ws.get_all_records()
    if not records:
        values = ws.get_all_values()
        if not values:
            return pd.DataFrame()
        header = values[0]
        return pd.DataFrame(columns=header)
    return pd.DataFrame(records)


def write_df_to_worksheet(
    ws: gspread.Worksheet,
    df: pd.DataFrame,
    clear_first: bool = True,
    include_index: bool = False,
) -> None:
    if clear_first:
        ws.clear()
    if df is None:
        return
    if df.empty:
        if len(df.columns) > 0:
            ws.append_row(list(df.columns))
        return
    set_with_dataframe(ws, df, include_index=include_index, resize=True)


def normalize_ticker_year(df: pd.DataFrame, ticker_col: str, year_col: str, upper: bool = True) -> pd.DataFrame:
    if df.empty:
        return df
    df[ticker_col] = df[ticker_col].astype(str).str.strip()
    if upper:
        df[ticker_col] = df[ticker_col].str.upper()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    return df


def detect_key_cols(df: pd.DataFrame) -> KeyCols:
    if df.empty:
        return KeyCols()
    cols = {c.strip(): c for c in df.columns}
    ticker = cols.get("company") or cols.get("ticker") or "company"
    year = cols.get("year") or cols.get("fiscal_year") or "year"
    return KeyCols(ticker=ticker, year=year)
