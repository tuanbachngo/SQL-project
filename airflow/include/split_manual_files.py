from __future__ import annotations

from typing import Dict, List, Optional, Sequence

import pandas as pd
import gspread

from .sheet_utils import (
    DEFAULT_SCOPES,
    detect_key_cols,
    get_gspread_client,
    open_or_create_worksheet,
    open_spreadsheet,
    read_worksheet_to_df,
    write_df_to_worksheet,
    normalize_ticker_year,
)


def _open_or_create_spreadsheet(
    gc: gspread.Client,
    title: str,
    existing_id: Optional[str] = None,
) -> gspread.Spreadsheet:
    if existing_id:
        try:
            return gc.open_by_key(existing_id)
        except Exception:
            pass
    return gc.create(title)

def _append_missing_company_year_rows(
    ws: gspread.Worksheet,
    df_new_source: pd.DataFrame,
    ticker_col: str,
    year_col: str,
) -> int:
    df_exist = read_worksheet_to_df(ws)
    if df_exist.empty:
        write_df_to_worksheet(ws, df_new_source, clear_first=True)
        return len(df_new_source)

    key_e = detect_key_cols(df_exist)
    normalize_ticker_year(df_exist, key_e.ticker, key_e.year, upper=True)
    df_exist = df_exist[df_exist[key_e.year].notna()].copy()
    df_exist[key_e.year] = df_exist[key_e.year].astype(int)

    existing_keys = set(
        df_exist[key_e.ticker].astype(str) + "_" + df_exist[key_e.year].astype(str)
    )

    df_in = df_new_source.copy()
    normalize_ticker_year(df_in, ticker_col, year_col, upper=True)
    df_in = df_in[df_in[year_col].notna()].copy()
    df_in[year_col] = df_in[year_col].astype(int)

    df_in["__k"] = df_in[ticker_col].astype(str) + "_" + df_in[year_col].astype(str)
    df_missing = df_in[~df_in["__k"].isin(existing_keys)].drop(columns="__k")

    if df_missing.empty:
        return 0

    header = list(df_exist.columns)

    extra_cols = [c for c in df_missing.columns if c not in header]
    if extra_cols:
        header2 = header + extra_cols
        ws.update("A1", [header2])
        header = header2

    df_missing = df_missing.reindex(columns=header)
    df_missing = df_missing.fillna("")

    rows = df_missing.values.tolist()
    ws.append_rows(rows, value_input_option="RAW")
    return len(rows)

def split_consolidated_to_manual_files(
    *,
    secret_path: str,
    master_spreadsheet_url: str,
    consolidated_ws_name: str,
    groups: Dict[str, List[str]],
    out_tab_name: str = "Sheet1",
    existing_manual_ids: Optional[Dict[str, str]] = None,
    share_with_emails: Optional[Sequence[str]] = None,
    manual_title_prefix: str = "MANUAL_30_",
) -> Dict[str, str]:
    if not master_spreadsheet_url:
        raise ValueError("master_spreadsheet_url is empty. Set it in your DAG or env.")

    gc = get_gspread_client(secret_path, scopes=DEFAULT_SCOPES)
    master = open_spreadsheet(gc, master_spreadsheet_url)
    ws = master.worksheet(consolidated_ws_name)

    df = read_worksheet_to_df(ws)
    if df.empty:
        raise ValueError(f"Consolidated sheet '{consolidated_ws_name}' is empty; nothing to split.")

    key = detect_key_cols(df)
    normalize_ticker_year(df, key.ticker, key.year, upper=True)

    df = df[df[key.year].notna()].copy()
    df[key.year] = df[key.year].astype(int)

    out_ids: Dict[str, str] = {}

    existing_manual_ids = existing_manual_ids or {}

    for group_name, tickers in groups.items():
        tickers_norm = [str(t).strip().upper() for t in tickers]
        df_g = df[df[key.ticker].isin(tickers_norm)].copy()

        title = f"{manual_title_prefix}{group_name}"
        sh = _open_or_create_spreadsheet(gc, title, existing_id=existing_manual_ids.get(group_name))

        out_ws = open_or_create_worksheet(
            sh, out_tab_name, rows=max(2000, len(df_g) + 50), cols=max(60, len(df.columns) + 5)
        )

        if not df_g.empty:
            df_g.sort_values(by=[key.ticker, key.year], inplace=True)
            df_g.fillna("", inplace=True)

        if df_g.empty:
            if not read_worksheet_to_df(out_ws).empty:
                pass
            else:
                write_df_to_worksheet(out_ws, df.head(0), clear_first=True)
        else:
            appended = _append_missing_company_year_rows(
                out_ws,
                df_g,
                ticker_col=key.ticker,
                year_col=key.year,
            )
            if appended:
                print(f"[{group_name}] appended {appended} new rows")

        if share_with_emails:
            for email in share_with_emails:
                try:
                    sh.share(email, perm_type="user", role="writer", notify=False)
                except Exception:
                    pass

        out_ids[group_name] = sh.id

    return out_ids
