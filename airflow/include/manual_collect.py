from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple, Union

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


def _ensure_columns_union(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    all_cols: List[str] = []
    seen = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                all_cols.append(c)
                seen.add(c)
    aligned = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        for c in missing:
            df[c] = ""
        aligned.append(df[all_cols])
    return aligned


def collect_manual_to_agg_df(
    *,
    secret_path: str,
    manual_spreadsheet_ids: Union[Sequence[str], Dict[str, str]],
    manual_tab_name: str = "Sheet1",
    prefer_order: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    gc = get_gspread_client(secret_path, scopes=DEFAULT_SCOPES)

    sources: List[Tuple[str, str]] = []
    if isinstance(manual_spreadsheet_ids, dict):
        for k, v in manual_spreadsheet_ids.items():
            sources.append((k, v))
    else:
        sources = [(f"source_{i+1}", sid) for i, sid in enumerate(list(manual_spreadsheet_ids))]

    if prefer_order:
        if isinstance(manual_spreadsheet_ids, dict):
            order_ids = []
            for name in prefer_order:
                if name in manual_spreadsheet_ids:
                    order_ids.append(manual_spreadsheet_ids[name])
            order_ids += [sid for _, sid in sources if sid not in order_ids]
        else:
            order_ids = list(prefer_order)
        priority_rank = {sid: i for i, sid in enumerate(order_ids)}
    else:
        priority_rank = {sid: i for i, (_, sid) in enumerate(sources)}

    dfs: List[pd.DataFrame] = []
    for source_name, sid in sources:
        sh = open_spreadsheet(gc, sid)
        try:
            ws = sh.worksheet(manual_tab_name)
        except Exception as e:
            raise ValueError(f"Manual spreadsheet {sid} missing tab '{manual_tab_name}': {e}") from e

        df = read_worksheet_to_df(ws)
        if df.empty:
            continue
        df["_manual_source"] = source_name
        df["_manual_source_id"] = sid
        df["_source_rank"] = priority_rank.get(sid, 9999)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    dfs = _ensure_columns_union(dfs)
    combined = pd.concat(dfs, ignore_index=True)

    key = detect_key_cols(combined)
    normalize_ticker_year(combined, key.ticker, key.year, upper=True)
    combined = combined[combined[key.year].notna()].copy()
    combined[key.year] = combined[key.year].astype(int)

    combined.sort_values(by=["_source_rank"], inplace=True)
    combined = combined.drop_duplicates(subset=[key.ticker, key.year], keep="first")

    combined.drop(columns=[c for c in ["_source_rank"] if c in combined.columns], inplace=True)

    front = [c for c in [key.ticker, key.year] if c in combined.columns]
    rest = [c for c in combined.columns if c not in front]
    combined = combined[front + rest]

    return combined


def collect_and_write_manual_agg(
    *,
    secret_path: str,
    manual_spreadsheet_ids: Union[Sequence[str], Dict[str, str]],
    manual_tab_name: str,
    master_spreadsheet_url: str,
    out_ws_name: str,
    prefer_order: Optional[Sequence[str]] = None,
) -> None:
    df = collect_manual_to_agg_df(
        secret_path=secret_path,
        manual_spreadsheet_ids=manual_spreadsheet_ids,
        manual_tab_name=manual_tab_name,
        prefer_order=prefer_order,
    )

    gc = get_gspread_client(secret_path, scopes=DEFAULT_SCOPES)
    master = open_spreadsheet(gc, master_spreadsheet_url)
    out_ws = open_or_create_worksheet(master, out_ws_name, rows=max(2000, len(df) + 50), cols=max(60, len(df.columns) + 5))

    if not df.empty:
        df.fillna("", inplace=True)

    write_df_to_worksheet(out_ws, df, clear_first=True)
