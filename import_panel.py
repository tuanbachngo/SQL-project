import argparse
import re
from datetime import date
from typing import Any, Dict, List, Optional, Tuple
import os
import pandas as pd
import pymysql
import math
from datetime import date

print(">>> import_panel.py loaded")
# ----------------------------
# Cleaning / parsing helpers
# ----------------------------
NULL_LIKE = {"", " ", "-", "na", "n/a", "null", "none", "thiếu", "thieu", "nan"}

def norm_colname(c: str) -> str:
    return str(c).strip()

def norm_ticker(x: Any) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip().upper()
    return s or None

def to_null(x: Any) -> Any:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s.lower() in NULL_LIKE:
        return None
    return x

def smart_to_number(x: Any) -> Optional[float]:
    """
    Robust numeric parser for:
    - 1,234,567.89 (US)
    - 1.234.567,89 (VN/EU)
    - -12.358.193.845.42 (rare messy)
    """
    x = to_null(x)
    if x is None:
        return None
    if isinstance(x, (int, float)) and not pd.isna(x):
        return float(x)

    s = str(x).strip().replace(" ", "")
    if not s:
        return None

    # Remove currency symbols if any
    s = re.sub(r"[^\d\-\+\.,]", "", s)

    # Heuristics
    if s.count(",") > 0 and s.count(".") > 0:
        # Decide decimal by last separator position
        if s.rfind(".") > s.rfind(","):
            # decimal is ".", thousands ","
            s = s.replace(",", "")
        else:
            # decimal is ",", thousands "."
            s = s.replace(".", "")
            s = s.replace(",", ".")
    elif s.count(".") > 1 and s.count(",") == 0:
        # many dots -> treat last dot as decimal, others thousands
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]
    elif s.count(",") > 1 and s.count(".") == 0:
        # many commas -> treat last comma as decimal, others thousands
        parts = s.split(",")
        s = "".join(parts[:-1]) + "." + parts[-1]
    elif s.count(",") == 1 and s.count(".") == 0:
        # single comma -> likely decimal comma
        s = s.replace(",", ".")
    else:
        # no special case: remove thousands commas
        s = s.replace(",", "")

    try:
        return float(s)
    except ValueError:
        return None

def to_int(x: Any) -> Optional[int]:
    n = smart_to_number(x)
    if n is None:
        return None
    try:
        return int(round(n))
    except Exception:
        return None

INNOV_RE = re.compile(r"^\s*([01])\s*(?:\((.*)\))?\s*$", re.DOTALL)

def parse_innovation_cell(val: Any) -> Tuple[Optional[int], Optional[str]]:
    """
    Accept:
      0/1
      "1 (evidence...)"
      "0(evidence...)"
      "Yes/No", "Có/Không"
    Return: (dummy, note)
    """
    val = to_null(val)
    if val is None:
        return None, None

    if isinstance(val, (int, float)) and not pd.isna(val):
        d = int(val)
        return (d if d in (0, 1) else None), None

    s = str(val).strip()
    m = INNOV_RE.match(s)
    if m:
        dummy = int(m.group(1))
        note = (m.group(2) or "").strip() or None
        return dummy, note

    sl = s.lower()
    if sl in {"yes", "true", "có", "co"}:
        return 1, None
    if sl in {"no", "false", "không", "khong"}:
        return 0, None

    return None, s


# ----------------------------
# MySQL helpers
# ----------------------------
def mysql_connect(host: str, port: int, user: str, password: str, db: str):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def fetch_firm_id_map(conn, tickers: List[str]) -> Dict[str, int]:
    if not tickers:
        return {}
    placeholders = ",".join(["%s"] * len(tickers))
    sql = f"SELECT firm_id, ticker FROM dim_firm WHERE ticker IN ({placeholders})"
    with conn.cursor() as cur:
        cur.execute(sql, tickers)
        rows = cur.fetchall()
    return {r["ticker"].upper(): int(r["firm_id"]) for r in rows}

def upsert_many(conn, table: str, cols: List[str], rows: List[Tuple[Any, ...]]) -> int:
    """
    INSERT ... ON DUPLICATE KEY UPDATE ...
    Assumes PK is (firm_id, fiscal_year, snapshot_id).
    """
    if not rows:
        return 0

    col_list = ", ".join(cols)
    ph = ", ".join(["%s"] * len(cols))

    pk = {"firm_id", "fiscal_year", "snapshot_id"}
    update_cols = [c for c in cols if c not in pk]
    update_expr = ", ".join([f"{c}=VALUES({c})" for c in update_cols]) if update_cols else ""

    sql = f"INSERT INTO {table} ({col_list}) VALUES ({ph})"
    if update_expr:
        sql += f" ON DUPLICATE KEY UPDATE {update_expr}"

    def _fix_nan(v):
        # pandas/numpy NaN => None
        if v is None:
            return None
        if isinstance(v, float) and math.isnan(v):
            return None
        return v

    with conn.cursor() as cur:
        safe_rows = [tuple(_fix_nan(x) for x in row) for row in rows]
        cur.executemany(sql, safe_rows)
        return cur.rowcount
    
def get_data_source_id(conn, source_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT source_id FROM dim_data_source WHERE source_name=%s", (source_name,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(
                f"❌ source_name '{source_name}' not found in dim_data_source. "
                f"Please choose an existing source_name (e.g., BCTC_Audited)."
            )
        return int(row["source_id"])

def get_snapshot_id(conn, source_id: int, fiscal_year: int, version_tag: str) -> int:
    """
    Pick the latest snapshot for (source_id, fiscal_year, version_tag).
    'Latest' is by snapshot_date then snapshot_id (tie-breaker).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT snapshot_id, snapshot_date
            FROM fact_data_snapshot
            WHERE source_id=%s AND fiscal_year=%s AND version_tag=%s
            ORDER BY snapshot_date DESC, snapshot_id DESC
            LIMIT 1
            """,
            (source_id, fiscal_year, version_tag),
        )
        row = cur.fetchone()
        if not row:
            raise SystemExit(
                f"❌ Missing snapshot for year={fiscal_year}, source_id={source_id}, version_tag={version_tag}. "
                f"Please run Script B first."
            )
        return int(row["snapshot_id"])


# ----------------------------
# Main load
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Load FINAL Excel (39 vars) into vn_firm_panel FACT tables.")

    ap.add_argument("--excel", default="data/Final Gộp 39 trường dữ liệu (FINAL).xlsx")
    ap.add_argument("--sheet", default="master_39")

    ap.add_argument("--db-host", default="localhost")
    ap.add_argument("--db-port", type=int, default=3306)
    ap.add_argument("--db-user", default="root")
    ap.add_argument("--db-pass", default="1234")
    ap.add_argument("--db-name", default="vn_firm_panel_test")

    ap.add_argument("--source-name", default="Vietstock")
    ap.add_argument("--version-tag", default="v1.0_initial")
    ap.add_argument("--created-by", default="Group_Member")

    ap.add_argument("--currency-code", default="VND")
    ap.add_argument("--unit-scale", type=int, default=1)
    ap.add_argument("--price-reference", default="close_year_end")

    args = ap.parse_args()
    print(">>> args =", args)

    df = pd.read_excel(args.excel, sheet_name=args.sheet)
    df.columns = [norm_colname(c) for c in df.columns]  # strip weird spaces in headers
    print(">>> df rows =", len(df))
    print(">>> df cols =", list(df.columns))

    # Require minimal keys
    if "ticker" not in df.columns or "fiscal_year" not in df.columns:
        raise SystemExit("Excel must contain columns: ticker, fiscal_year")

    # Normalize keys
    df["ticker"] = df["ticker"].apply(norm_ticker)
    df["fiscal_year"] = df["fiscal_year"].apply(to_int)

    # Drop blank rows
    df = df.dropna(subset=["ticker", "fiscal_year"]).copy()
    print(">>> df rows after drop blank =", len(df))
    print(">>> years found =", sorted(df["fiscal_year"].dropna().unique().tolist()))
    print(">>> tickers found =", sorted(df["ticker"].dropna().unique().tolist())[:10], "...")

    # Common rename to match schema
    rename_map = {
        "dividend_payment": "dividend_cash_paid",
        "eps": "eps_basic",
        "total_inventory": "inventory",
        "net_cash_from_operating": "net_cfo",
        "cash_flows_from_investing": "net_cfi",
        "capital_expenditure": "capex",
        "employees": "employees_count",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

    # Parse innovation (keep in two new columns + evidence note)
    if "product_innovation" in df.columns:
        parsed = df["product_innovation"].apply(parse_innovation_cell)
        df["_prod_dummy"] = parsed.apply(lambda x: x[0])
        df["_prod_note"] = parsed.apply(lambda x: x[1])
    else:
        df["_prod_dummy"], df["_prod_note"] = None, None

    if "process_innovation" in df.columns:
        parsed = df["process_innovation"].apply(parse_innovation_cell)
        df["_proc_dummy"] = parsed.apply(lambda x: x[0])
        df["_proc_note"] = parsed.apply(lambda x: x[1])
    else:
        df["_proc_dummy"], df["_proc_note"] = None, None

    # Connect DB
    conn = mysql_connect(args.db_host, args.db_port, args.db_user, args.db_pass, args.db_name)

    # --- DEBUG STEP 3: verify DB connection + schema ---
    with conn.cursor() as cur:
        cur.execute("SELECT DATABASE() AS db")
        print(">>> Connected DB =", cur.fetchone()["db"])

        cur.execute("SELECT COUNT(*) AS n FROM dim_firm")
        print(">>> dim_firm rows =", cur.fetchone()["n"])

        cur.execute("SHOW TABLES LIKE 'fact_financial_year'")
        print(">>> has fact_financial_year =", cur.fetchone() is not None)
    # --- END DEBUG ---

    try:
        # Firm mapping
        tickers = sorted(df["ticker"].unique().tolist())
        firm_map = fetch_firm_id_map(conn, tickers)
        missing = [t for t in tickers if t not in firm_map]
        if missing:
            raise SystemExit(f"Tickers not found in dim_firm (run import_firms first): {missing}")

        df["firm_id"] = df["ticker"].map(firm_map).astype(int)

        # Data source + snapshots per year
        source_id = get_data_source_id(conn, args.source_name)
        years = sorted(df["fiscal_year"].dropna().unique().tolist())

        snapshots: Dict[int, int] = {}
        for y in years:
            y = int(y)
            tag = args.version_tag
            snap_id = get_snapshot_id(
                conn,
                source_id=source_id,
                fiscal_year=y,
                version_tag=tag,
            )
            snapshots[y] = snap_id
            print(f">>> picked snapshot: year={y}, source_id={source_id}, version_tag={tag} -> snapshot_id={snap_id}")
           

        # Fill common metadata defaults
        df["_currency_code"] = args.currency_code
        df["_unit_scale"] = int(args.unit_scale)
        df["_price_reference"] = args.price_reference

        # Columns per FACT table (schema column names)
        key_cols = ["firm_id", "fiscal_year", "snapshot_id"]

        ownership_fields = ["managerial_inside_own", "state_own", "institutional_own", "foreign_own"]
        market_fields = ["shares_outstanding", "share_price", "market_value_equity", "dividend_cash_paid", "eps_basic"]
        cashflow_fields = ["net_cfo", "capex", "net_cfi"]
        financial_fields = [
            "net_sales", "total_assets", "selling_expenses", "general_admin_expenses",
            "intangible_assets_net", "manufacturing_overhead", "net_operating_income",
            "raw_material_consumption", "merchandise_purchase_year", "wip_goods_purchase",
            "outside_manufacturing_expenses", "production_cost", "rnd_expenses", "net_income",
            "total_equity", "total_liabilities", "cash_and_equivalents", "long_term_debt",
            "current_assets", "current_liabilities", "growth_ratio", "inventory", "net_ppe"
        ]
        meta_fields = ["employees_count", "firm_age"]

        # Convert numeric columns safely (only if exist)
        def ensure_cols(cols: List[str], default=None):
            for c in cols:
                if c not in df.columns:
                    df[c] = default

        ensure_cols(ownership_fields)
        ensure_cols(market_fields)
        ensure_cols(cashflow_fields)
        ensure_cols(financial_fields)
        ensure_cols(meta_fields)

        # Numeric conversions
        for c in ownership_fields + ["share_price", "market_value_equity", "dividend_cash_paid", "eps_basic"] + cashflow_fields + financial_fields:
            if c in df.columns:
                df[c] = df[c].apply(smart_to_number)

        for c in ["shares_outstanding", "employees_count", "firm_age"]:
            if c in df.columns:
                df[c] = df[c].apply(to_int)

        # Optional: if share_price missing but have market cap + shares, auto compute (comment if you don't want)
        if "share_price" in df.columns:
            mask = df["share_price"].isna() & df["market_value_equity"].notna() & df["shares_outstanding"].notna() & (df["shares_outstanding"] != 0)
            df.loc[mask, "share_price"] = df.loc[mask, "market_value_equity"] / df.loc[mask, "shares_outstanding"]

        # Insert per year so snapshot_id matches year snapshot
        stats = {t: 0 for t in [
            "fact_ownership_year", "fact_market_year", "fact_cashflow_year",
            "fact_financial_year", "fact_innovation_year", "fact_firm_year_meta"
        ]}

        for y in years:
            y = int(y)
            snap_id = snapshots[y]
            dyy = df[df["fiscal_year"] == y].copy()
            dyy["snapshot_id"] = snap_id

            # 1) ownership

            s = pd.to_numeric(dyy["managerial_inside_own"], errors="coerce")
            print(">>> managerial_inside_own min/max:", s.min(), s.max())

            bad = dyy[s.notna() & ((s < 0) | (s > 1))][["ticker","fiscal_year","managerial_inside_own"]]
            print(">>> bad managerial rows:", len(bad))
            if len(bad):
                print(bad.head(20).to_string(index=False))

            ownership_cols = key_cols + ownership_fields
            ownership_rows = [tuple(r.get(c) for c in ownership_cols) for _, r in dyy.iterrows()]
            stats["fact_ownership_year"] += upsert_many(conn, "fact_ownership_year", ownership_cols, ownership_rows)

            # 2) market
            market_cols = key_cols + [
                "shares_outstanding", "price_reference", "share_price", "market_value_equity",
                "dividend_cash_paid", "eps_basic", "currency_code"
            ]
            dyy["price_reference"] = dyy["_price_reference"]
            dyy["currency_code"] = dyy["_currency_code"]
            market_rows = [tuple(r.get(c) for c in market_cols) for _, r in dyy.iterrows()]
            stats["fact_market_year"] += upsert_many(conn, "fact_market_year", market_cols, market_rows)

            # 3) cashflow
            cashflow_cols = key_cols + ["unit_scale", "currency_code"] + cashflow_fields
            dyy["unit_scale"] = dyy["_unit_scale"]
            dyy["currency_code"] = dyy["_currency_code"]
            cashflow_rows = [tuple(r.get(c) for c in cashflow_cols) for _, r in dyy.iterrows()]
            stats["fact_cashflow_year"] += upsert_many(conn, "fact_cashflow_year", cashflow_cols, cashflow_rows)

            # 4) financial
            financial_cols = key_cols + ["unit_scale", "currency_code"] + financial_fields
            dyy["unit_scale"] = dyy["_unit_scale"]
            dyy["currency_code"] = dyy["_currency_code"]
            financial_rows = [tuple(r.get(c) for c in financial_cols) for _, r in dyy.iterrows()]
            stats["fact_financial_year"] += upsert_many(conn, "fact_financial_year", financial_cols, financial_rows)

            # 5) innovation
            innov_cols = key_cols + ["product_innovation", "process_innovation", "evidence_source_id", "evidence_note"]
            dyy["product_innovation"] = dyy["_prod_dummy"]
            dyy["process_innovation"] = dyy["_proc_dummy"]
            dyy["evidence_source_id"] = None

            def merge_notes(r):
                notes = []
                if r.get("_prod_note"):
                    notes.append("Product: " + str(r["_prod_note"]).strip())
                if r.get("_proc_note"):
                    notes.append("Process: " + str(r["_proc_note"]).strip())
                return " | ".join(notes) if notes else None

            dyy["evidence_note"] = dyy.apply(merge_notes, axis=1)
            innov_rows = [tuple(r.get(c) for c in innov_cols) for _, r in dyy.iterrows()]
            stats["fact_innovation_year"] += upsert_many(conn, "fact_innovation_year", innov_cols, innov_rows)

            # 6) meta
            meta_cols = key_cols + meta_fields
            meta_rows = [tuple(r.get(c) for c in meta_cols) for _, r in dyy.iterrows()]
            stats["fact_firm_year_meta"] += upsert_many(conn, "fact_firm_year_meta", meta_cols, meta_rows)

        print(">>> stats so far =", stats)
        conn.commit()

        print("✅ DONE. Rowcount (includes updates):")
        for k, v in stats.items():
            print(f"  - {k}: {v}")

        print("✅ Snapshots created:", snapshots)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
