"""
Python Data Pipeline Engineering Lab
Omnichannel Retail Data Warehouse — Incremental & Idempotent ETL

Builds a Star Schema (dim_customer, dim_product, dim_date, fact_sales) in
SQLite from messy, multi-batch order data, quarantining bad records instead
of letting the pipeline fail.

Usage:
    python pipeline.py
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pipeline")


# ==========================================================================
# TASK 1 — Pipeline Configuration & Extract
# ==========================================================================
@dataclass
class PipelineConfig:
    """Central configuration for a pipeline run."""

    input_path: str                                   # path to the source workbook
    output_database: str                               # path to the target SQLite DB
    batches: list[int] = field(default_factory=lambda: [1, 2, 3])
    error_mode: str = "quarantine"                      # "quarantine" | "strict"
    quarantine_csv: str = "quarantine.csv"
    run_log_csv: str = "pipeline_run_log.csv"

    def __post_init__(self) -> None:
        if self.error_mode not in {"quarantine", "strict"}:
            raise ValueError("error_mode must be 'quarantine' or 'strict'")


class ExtractError(Exception):
    """Raised when a source file/sheet cannot be read at all."""


def extract_dimension(config: PipelineConfig, sheet_name: str) -> pd.DataFrame:
    """Read a dimension sheet (customers / products) from the source workbook."""
    started = datetime.now(timezone.utc)
    try:
        df = pd.read_excel(config.input_path, sheet_name=sheet_name)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        log.info(
            "EXTRACT dimension=%-10s rows=%-4d elapsed=%.3fs", sheet_name, len(df), elapsed
        )
        return df
    except Exception as exc:  # noqa: BLE001 - deliberately broad, we log & re-raise typed
        log.error("EXTRACT FAILED dimension=%s error=%s", sheet_name, exc)
        raise ExtractError(f"could not read dimension sheet '{sheet_name}'") from exc


def extract_batch(config: PipelineConfig, batch: int) -> Optional[pd.DataFrame]:
    """Read one orders_batch_N sheet. Returns None (does not crash) if unreadable."""
    sheet_name = f"orders_batch_{batch}"
    started = datetime.now(timezone.utc)
    try:
        df = pd.read_excel(config.input_path, sheet_name=sheet_name)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        log.info(
            "EXTRACT batch=%-2d sheet=%-16s rows=%-4d elapsed=%.3fs start=%s end=%s",
            batch, sheet_name, len(df), elapsed, started.isoformat(timespec="seconds"),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
        )
        return df
    except Exception as exc:  # noqa: BLE001
        # A whole batch failing to load must not kill the rest of the pipeline.
        log.error("EXTRACT FAILED batch=%d error=%s", batch, exc)
        return None


# ==========================================================================
# TASK 2 — Transform & Data Quality
# ==========================================================================
PAYMENT_METHOD_MAP = {
    "cash": "Cash",
    "credit card": "Credit Card",
    "promptpay": "PromptPay",
    "bank transfer": "Bank Transfer",
}
SALES_CHANNEL_MAP = {
    "e-commerce": "Online",
    "online": "Online",
    "store": "Store",
    "marketplace": "Marketplace",
}


def _clean_price(value) -> float:
    """Strip currency prefixes like 'THB 979.4' and coerce to float."""
    if pd.isna(value):
        return float("nan")
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).upper().replace("THB", "").replace(",", "").strip()
    try:
        return float(text)
    except ValueError:
        return float("nan")


def _clean_quantity(value):
    """Coerce quantity to a nullable integer; non-numeric text -> NaN."""
    return pd.to_numeric(value, errors="coerce")


def transform_batch(
    df: pd.DataFrame,
    batch: int,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Clean, validate, and split one batch into (clean, quarantine) frames.
    `clean` still contains duplicate order_ids at this point — dedup happens
    in load_facts() so that rows_valid (this function's output) satisfies
    rows_read == rows_valid + rows_rejected *before* deduplication.
    """
    work = df.copy()
    work["source_batch"] = batch
    reasons: list[list[str]] = [[] for _ in range(len(work))]

    # --- safe type coercion -------------------------------------------------
    work["order_datetime_parsed"] = pd.to_datetime(work["order_datetime"], errors="coerce")
    work["updated_at_parsed"] = pd.to_datetime(work["updated_at"], errors="coerce")
    work["quantity_clean"] = _clean_quantity(work["quantity"])
    work["unit_price_clean"] = work["unit_price"].apply(_clean_price)
    work["discount_pct_clean"] = pd.to_numeric(work["discount_pct"], errors="coerce")

    # --- normalize categoricals ---------------------------------------------
    work["payment_method_clean"] = (
        work["payment_method"].astype(str).str.strip().str.lower().map(PAYMENT_METHOD_MAP)
    )
    work["sales_channel_clean"] = (
        work["sales_channel"].astype(str).str.strip().str.lower().map(SALES_CHANNEL_MAP)
    )

    # --- validation rules -----------------------------------------------------
    for i, row in work.iterrows():
        if pd.isna(row["order_datetime_parsed"]):
            reasons[i].append("INVALID_DATE")
        if pd.isna(row["updated_at_parsed"]):
            reasons[i].append("INVALID_UPDATED_AT")
        if pd.isna(row["quantity_clean"]) or not (0 < row["quantity_clean"] <= 20):
            reasons[i].append("INVALID_QUANTITY")
        if pd.isna(row["unit_price_clean"]) or row["unit_price_clean"] <= 0:
            reasons[i].append("INVALID_UNIT_PRICE")
        if pd.isna(row["discount_pct_clean"]) or not (0 <= row["discount_pct_clean"] <= 100):
            reasons[i].append("INVALID_DISCOUNT_PCT")
        if pd.isna(row["customer_id"]) or str(row["customer_id"]).strip() == "":
            reasons[i].append("MISSING_CUSTOMER_ID")
        elif row["customer_id"] not in set(customers["customer_id"]):
            reasons[i].append("CUSTOMER_NOT_FOUND")
        if pd.isna(row["product_id"]) or str(row["product_id"]).strip() == "":
            reasons[i].append("MISSING_PRODUCT_ID")
        elif row["product_id"] not in set(products["product_id"]):
            reasons[i].append("PRODUCT_NOT_FOUND")
        else:
            prod_row = products.loc[products["product_id"] == row["product_id"]].iloc[0]
            if str(prod_row["active_flag"]).strip().upper() != "Y":
                reasons[i].append("INACTIVE_PRODUCT")
        if pd.isna(row["payment_method_clean"]):
            reasons[i].append("INVALID_PAYMENT_METHOD")
        if pd.isna(row["sales_channel_clean"]):
            reasons[i].append("INVALID_SALES_CHANNEL")

    work["reason_code"] = ["; ".join(r) if r else "" for r in reasons]
    work["is_valid"] = work["reason_code"] == ""

    clean = work[work["is_valid"]].copy()
    quarantine = work[~work["is_valid"]].copy()

    # derived amounts, only meaningful for clean rows
    clean["gross_amount"] = (clean["quantity_clean"] * clean["unit_price_clean"]).round(2)
    clean["net_amount"] = (
        clean["gross_amount"] * (1 - clean["discount_pct_clean"] / 100)
    ).round(2)

    log.info(
        "TRANSFORM batch=%-2d read=%-4d valid=%-4d rejected=%-4d",
        batch, len(work), len(clean), len(quarantine),
    )
    return clean, quarantine


# ==========================================================================
# TASK 3 — Star Schema & Load
# ==========================================================================
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT UNIQUE NOT NULL,
    customer_name  TEXT,
    province       TEXT,
    segment        TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id     TEXT UNIQUE NOT NULL,
    product_name   TEXT,
    category       TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key   INTEGER PRIMARY KEY,
    full_date  TEXT UNIQUE NOT NULL,
    day        INTEGER,
    month      INTEGER,
    quarter    INTEGER,
    year       INTEGER
);

CREATE TABLE IF NOT EXISTS fact_sales (
    order_id       TEXT PRIMARY KEY,
    date_key       INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key   INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key    INTEGER NOT NULL REFERENCES dim_product(product_key),
    quantity       INTEGER NOT NULL,
    unit_price     REAL NOT NULL,
    discount_pct   REAL NOT NULL,
    gross_amount   REAL NOT NULL,
    net_amount     REAL NOT NULL,
    payment_method TEXT,
    sales_channel  TEXT,
    source_batch   INTEGER,
    updated_at     TEXT
);

CREATE TABLE IF NOT EXISTS quarantine (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id      TEXT,
    source_batch  INTEGER,
    reason_code   TEXT,
    order_datetime TEXT,
    customer_id   TEXT,
    product_id    TEXT,
    quantity      TEXT,
    unit_price    TEXT,
    discount_pct  TEXT,
    payment_method TEXT,
    sales_channel TEXT,
    updated_at    TEXT,
    quarantined_at TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch          INTEGER,
    started_at     TEXT,
    ended_at       TEXT,
    rows_read      INTEGER,
    rows_valid     INTEGER,
    rows_rejected  INTEGER,
    rows_duplicated INTEGER,
    rows_loaded    INTEGER,
    status         TEXT
);

CREATE TABLE IF NOT EXISTS batch_watermark (
    batch            INTEGER PRIMARY KEY,
    last_updated_at  TEXT,
    last_run_at      TEXT
);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def load_dimensions(
    conn: sqlite3.Connection, customers: pd.DataFrame, products: pd.DataFrame
) -> None:
    cur = conn.cursor()
    for _, r in customers.iterrows():
        cur.execute(
            """INSERT INTO dim_customer (customer_id, customer_name, province, segment)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(customer_id) DO UPDATE SET
                 customer_name=excluded.customer_name,
                 province=excluded.province,
                 segment=excluded.segment""",
            (r["customer_id"], r["customer_name"], r["province"], r["segment"]),
        )
    for _, r in products.iterrows():
        cur.execute(
            """INSERT INTO dim_product (product_id, product_name, category)
               VALUES (?, ?, ?)
               ON CONFLICT(product_id) DO UPDATE SET
                 product_name=excluded.product_name,
                 category=excluded.category""",
            (r["product_id"], r["product_name"], r["category"]),
        )
    conn.commit()
    log.info("LOAD dimensions customers=%d products=%d", len(customers), len(products))


def get_or_create_date_key(conn: sqlite3.Connection, ts: pd.Timestamp) -> int:
    date_key = int(ts.strftime("%Y%m%d"))
    conn.execute(
        """INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(date_key) DO NOTHING""",
        (
            date_key, ts.strftime("%Y-%m-%d"), ts.day, ts.month,
            (ts.month - 1) // 3 + 1, ts.year,
        ),
    )
    return date_key


def load_facts(conn: sqlite3.Connection, clean: pd.DataFrame) -> tuple[int, int]:
    """
    Deduplicate by order_id (keep latest updated_at) then upsert into
    fact_sales. Returns (rows_duplicated, rows_loaded).
    """
    if clean.empty:
        return 0, 0

    before = len(clean)
    deduped = (
        clean.sort_values("updated_at_parsed")
        .drop_duplicates(subset="order_id", keep="last")
    )
    rows_duplicated = before - len(deduped)

    cur = conn.cursor()
    customer_key = dict(
        conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall()
    )
    product_key = dict(
        conn.execute("SELECT product_id, product_key FROM dim_product").fetchall()
    )

    loaded = 0
    for _, r in deduped.iterrows():
        date_key = get_or_create_date_key(conn, r["order_datetime_parsed"])
        cur.execute(
            """INSERT INTO fact_sales
                 (order_id, date_key, customer_key, product_key, quantity, unit_price,
                  discount_pct, gross_amount, net_amount, payment_method, sales_channel,
                  source_batch, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(order_id) DO UPDATE SET
                 date_key=excluded.date_key,
                 customer_key=excluded.customer_key,
                 product_key=excluded.product_key,
                 quantity=excluded.quantity,
                 unit_price=excluded.unit_price,
                 discount_pct=excluded.discount_pct,
                 gross_amount=excluded.gross_amount,
                 net_amount=excluded.net_amount,
                 payment_method=excluded.payment_method,
                 sales_channel=excluded.sales_channel,
                 source_batch=excluded.source_batch,
                 updated_at=excluded.updated_at
               WHERE excluded.updated_at >= fact_sales.updated_at""",
            (
                r["order_id"], date_key, customer_key[r["customer_id"]],
                product_key[r["product_id"]], int(r["quantity_clean"]),
                float(r["unit_price_clean"]), float(r["discount_pct_clean"]),
                float(r["gross_amount"]), float(r["net_amount"]),
                r["payment_method_clean"], r["sales_channel_clean"],
                int(r["source_batch"]), r["updated_at_parsed"].isoformat(),
            ),
        )
        loaded += 1
    conn.commit()
    log.info(
        "LOAD facts rows_in=%-4d duplicates_removed=%-4d upserted=%-4d",
        before, rows_duplicated, loaded,
    )
    return rows_duplicated, loaded


def load_quarantine(conn: sqlite3.Connection, quarantine: pd.DataFrame) -> int:
    if quarantine.empty:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    for _, r in quarantine.iterrows():
        cur.execute(
            """INSERT INTO quarantine
                 (order_id, source_batch, reason_code, order_datetime, customer_id,
                  product_id, quantity, unit_price, discount_pct, payment_method,
                  sales_channel, updated_at, quarantined_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                r.get("order_id"), r.get("source_batch"), r.get("reason_code"),
                str(r.get("order_datetime")), r.get("customer_id"), r.get("product_id"),
                str(r.get("quantity")), str(r.get("unit_price")), str(r.get("discount_pct")),
                str(r.get("payment_method")), str(r.get("sales_channel")),
                str(r.get("updated_at")), now,
            ),
        )
    conn.commit()
    log.info("LOAD quarantine rows=%d", len(quarantine))
    return len(quarantine)


# ==========================================================================
# TASK 4 — Incremental watermark helpers
# ==========================================================================
def get_watermark(conn: sqlite3.Connection, batch: int) -> Optional[str]:
    row = conn.execute(
        "SELECT last_updated_at FROM batch_watermark WHERE batch = ?", (batch,)
    ).fetchone()
    return row[0] if row else None


def set_watermark(conn: sqlite3.Connection, batch: int, last_updated_at: Optional[str]) -> None:
    if last_updated_at is None:
        return
    conn.execute(
        """INSERT INTO batch_watermark (batch, last_updated_at, last_run_at)
           VALUES (?, ?, ?)
           ON CONFLICT(batch) DO UPDATE SET
             last_updated_at=excluded.last_updated_at,
             last_run_at=excluded.last_run_at""",
        (batch, last_updated_at, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def apply_watermark_filter(df: pd.DataFrame, watermark: Optional[str]) -> pd.DataFrame:
    """Keep only rows whose updated_at is strictly newer than the watermark.

    Rows with an unparseable updated_at are always kept so they still go
    through validation and land in quarantine with a reason code, rather
    than being silently dropped by the incremental filter.
    """
    if watermark is None:
        return df
    parsed = pd.to_datetime(df["updated_at"], errors="coerce")
    wm = pd.to_datetime(watermark)
    keep = parsed.isna() | (parsed > wm)
    return df[keep].copy()


# ==========================================================================
# TASK 5 — Orchestration
# ==========================================================================
def run_pipeline(config: PipelineConfig) -> dict:
    """
    extract -> transform -> validate -> load, once per configured batch.
    A single bad row is quarantined; a whole unreadable batch is logged as
    failed without destroying data already loaded from other batches.
    """
    Path(config.output_database).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(config.output_database)
    conn.execute("PRAGMA foreign_keys = ON;")
    init_schema(conn)

    try:
        customers = extract_dimension(config, "customers")
        products = extract_dimension(config, "products")
    except ExtractError:
        conn.close()
        raise  # dimensions are mandatory; nothing can load without them

    load_dimensions(conn, customers, products)

    run_log_rows: list[dict] = []
    kpi_totals = {"rows_read": 0, "rows_valid": 0, "rows_rejected": 0,
                  "rows_duplicated": 0, "rows_loaded": 0}

    for batch in config.batches:
        started_at = datetime.now(timezone.utc)
        raw = extract_batch(config, batch)

        if raw is None:
            run_log_rows.append({
                "batch": batch, "started_at": started_at.isoformat(),
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "rows_read": 0, "rows_valid": 0, "rows_rejected": 0,
                "rows_duplicated": 0, "rows_loaded": 0, "status": "FAILED",
            })
            log.error("BATCH FAILED batch=%d — skipped, prior loads preserved", batch)
            continue

        watermark = get_watermark(conn, batch)
        filtered = apply_watermark_filter(raw, watermark)
        skipped_by_watermark = len(raw) - len(filtered)
        if skipped_by_watermark:
            log.info(
                "INCREMENTAL batch=%d watermark=%s skipped_already_processed=%d",
                batch, watermark, skipped_by_watermark,
            )

        try:
            clean, quarantine = transform_batch(filtered, batch, customers, products)
        except Exception as exc:  # noqa: BLE001
            run_log_rows.append({
                "batch": batch, "started_at": started_at.isoformat(),
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "rows_read": len(filtered), "rows_valid": 0, "rows_rejected": 0,
                "rows_duplicated": 0, "rows_loaded": 0, "status": "FAILED",
            })
            log.error("TRANSFORM FAILED batch=%d error=%s", batch, exc)
            continue

        if config.error_mode == "strict" and not quarantine.empty:
            run_log_rows.append({
                "batch": batch, "started_at": started_at.isoformat(),
                "ended_at": datetime.now(timezone.utc).isoformat(),
                "rows_read": len(filtered), "rows_valid": len(clean),
                "rows_rejected": len(quarantine), "rows_duplicated": 0,
                "rows_loaded": 0, "status": "FAILED_STRICT",
            })
            log.error("STRICT MODE batch=%d has %d invalid rows — aborting batch",
                       batch, len(quarantine))
            continue

        rows_duplicated, rows_loaded = load_facts(conn, clean)
        rows_rejected = load_quarantine(conn, quarantine)

        # advance the watermark using every row *read* this run (including
        # rejected ones) so a rerun never reprocesses them.
        if not raw["updated_at"].isna().all():
            max_updated = pd.to_datetime(raw["updated_at"], errors="coerce").max()
            if pd.notna(max_updated):
                current = get_watermark(conn, batch)
                if current is None or max_updated > pd.to_datetime(current):
                    set_watermark(conn, batch, max_updated.isoformat())

        ended_at = datetime.now(timezone.utc)
        row_log = {
            "batch": batch, "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(), "rows_read": len(filtered),
            "rows_valid": len(clean), "rows_rejected": rows_rejected,
            "rows_duplicated": rows_duplicated, "rows_loaded": rows_loaded,
            "status": "SUCCESS",
        }
        run_log_rows.append(row_log)

        for k in kpi_totals:
            kpi_totals[k] += row_log[k]

        cur = conn.cursor()
        cur.execute(
            """INSERT INTO pipeline_run_log
                 (batch, started_at, ended_at, rows_read, rows_valid, rows_rejected,
                  rows_duplicated, rows_loaded, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (batch, row_log["started_at"], row_log["ended_at"], row_log["rows_read"],
             row_log["rows_valid"], row_log["rows_rejected"], row_log["rows_duplicated"],
             row_log["rows_loaded"], row_log["status"]),
        )
        conn.commit()

        log.info(
            "RUN batch=%-2d status=%-7s read=%-4d valid=%-4d rejected=%-4d "
            "duplicated=%-3d loaded=%-4d",
            batch, row_log["status"], row_log["rows_read"], row_log["rows_valid"],
            row_log["rows_rejected"], row_log["rows_duplicated"], row_log["rows_loaded"],
        )

    net_sales = conn.execute("SELECT COALESCE(SUM(net_amount), 0) FROM fact_sales").fetchone()[0]
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    conn.close()

    kpi_totals["fact_row_count"] = fact_count
    kpi_totals["net_sales_total"] = round(net_sales, 2)

    run_log_df = pd.DataFrame(run_log_rows)
    return {"run_log": run_log_df, "kpi": kpi_totals}


def export_quarantine_csv(db_path: str, out_path: str) -> None:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM quarantine ORDER BY id", conn)
    conn.close()
    df.to_csv(out_path, index=False)
    log.info("EXPORT quarantine.csv rows=%d", len(df))


def export_run_log_csv(db_path: str, out_path: str) -> None:
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM pipeline_run_log ORDER BY run_id", conn)
    conn.close()
    df.to_csv(out_path, index=False)
    log.info("EXPORT pipeline_run_log.csv rows=%d", len(df))


# ==========================================================================
# Main — demonstrates idempotency (batch_1 run twice) + incremental loading
# ==========================================================================
if __name__ == "__main__":
    config = PipelineConfig(
        input_path="Python_Data_Pipeline_Lab_Dataset (1).xlsx",
        output_database="retail_dw.db",
        batches=[1],  # first demonstrate batch_1 run once
    )

    Path(config.output_database).unlink(missing_ok=True)

    print("\n=== RUN 1: batch_1 (initial load) ===")
    result1 = run_pipeline(config)
    print(result1["run_log"].to_string(index=False))

    print("\n=== RUN 2: batch_1 again (idempotency check) ===")
    result2 = run_pipeline(config)
    print(result2["run_log"].to_string(index=False))

    conn = sqlite3.connect(config.output_database)
    fact_count_after_rerun = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    conn.close()
    print(f"\nfact_sales row count after rerun: {fact_count_after_rerun} "
          f"(must equal the count after RUN 1 — idempotent)")

    print("\n=== RUN 3: batch_2 ===")
    config.batches = [2]
    result3 = run_pipeline(config)
    print(result3["run_log"].to_string(index=False))

    print("\n=== RUN 4: batch_3 ===")
    config.batches = [3]
    result4 = run_pipeline(config)
    print(result4["run_log"].to_string(index=False))

    # ---- final KPI summary across all runs -------------------------------
    conn = sqlite3.connect(config.output_database)
    totals = conn.execute(
        """SELECT SUM(rows_read), SUM(rows_valid), SUM(rows_rejected),
                  SUM(rows_duplicated), SUM(rows_loaded)
           FROM pipeline_run_log"""
    ).fetchone()
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    net_sales_total = conn.execute(
        "SELECT COALESCE(SUM(net_amount), 0) FROM fact_sales"
    ).fetchone()[0]
    conn.close()

    print("\n=== FINAL KPI SUMMARY (all runs) ===")
    print(f"rows read       : {totals[0]}")
    print(f"rows valid      : {totals[1]}")
    print(f"rows rejected   : {totals[2]}")
    print(f"rows duplicated : {totals[3]}")
    print(f"rows loaded     : {totals[4]}")
    print(f"fact_sales rows : {fact_count}")
    print(f"net sales total : {net_sales_total:,.2f}")

    export_quarantine_csv(config.output_database, config.quarantine_csv)
    export_run_log_csv(config.output_database, config.run_log_csv)

    print("\nDone. Outputs: retail_dw.db, quarantine.csv, pipeline_run_log.csv")
