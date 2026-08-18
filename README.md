# Python Data Pipeline Engineering — Omnichannel Retail Data Warehouse

Incremental, idempotent ETL pipeline that turns three messy `orders_batch_*`
extracts into a clean Star Schema in SQLite, quarantining bad rows instead of
crashing.

## 1. Setup

```bash
pip install pandas openpyxl
```

Requires Python 3.10+ (uses `list[int]`, `tuple[...]`, `from __future__ import annotations`).

Place the source workbook next to `pipeline.py` as `source_data.xlsx`
(sheets: `customers`, `products`, `orders_batch_1`, `orders_batch_2`,
`orders_batch_3`, `data_dictionary`). The workbook is **never modified** —
the pipeline only reads from it via `pandas.read_excel`.

## 2. Run

```bash
python pipeline.py
```

This performs, in order:

1. **Run 1** — load `batch_1` into an empty `retail_dw.db`
2. **Run 2** — load `batch_1` again (idempotency check — `fact_sales` row
   count must not grow)
3. **Run 3** — load `batch_2`
4. **Run 4** — load `batch_3`

It prints a run log after each step, a final KPI summary, and writes:

| File | Contents |
|---|---|
| `retail_dw.db` | SQLite database — Star Schema, fully loaded |
| `quarantine.csv` | Every rejected row with a `reason_code` and `source_batch` |
| `pipeline_run_log.csv` | One row per run: rows read/valid/rejected/duplicated/loaded, timing, status |

To process a single batch programmatically:

```python
from pipeline import PipelineConfig, run_pipeline

config = PipelineConfig(
    input_path="source_data.xlsx",
    output_database="retail_dw.db",
    batches=[2],          # any subset of [1, 2, 3]
    error_mode="quarantine",  # or "strict" to abort a batch that has any bad row
)
result = run_pipeline(config)
print(result["kpi"])
```

## 3. Star Schema

```
dim_customer(customer_key PK, customer_id UNIQUE, customer_name, province, segment)
dim_product (product_key  PK, product_id  UNIQUE, product_name, category)
dim_date    (date_key     PK, full_date   UNIQUE, day, month, quarter, year)

fact_sales (
  order_id       PK,                       -- grain: one validated order per row
  date_key       FK -> dim_date,
  customer_key   FK -> dim_customer,
  product_key    FK -> dim_product,
  quantity, unit_price, discount_pct,
  gross_amount, net_amount,
  payment_method, sales_channel,
  source_batch, updated_at
)
```

**Grain of `fact_sales`**: one validated sale per `order_id`. Duplicate
`order_id`s (within a batch or across batches) are resolved by keeping the
row with the latest `updated_at` and **upserting** (`INSERT ... ON CONFLICT
DO UPDATE ... WHERE excluded.updated_at >= fact_sales.updated_at`), so
reprocessing never creates a second fact row for the same order.

Supporting tables:
- `quarantine` — rejected rows with `reason_code` (semicolon-joined if more
  than one rule failed) and `source_batch`.
- `pipeline_run_log` — one row per pipeline run with counts and status.
- `batch_watermark` — per-batch high-water mark on `updated_at`, used for
  incremental loading.

## 4. Data quality rules applied

| Check | Reason code |
|---|---|
| `order_datetime` / `updated_at` not parseable | `INVALID_DATE` / `INVALID_UPDATED_AT` |
| `quantity` not an integer in 1–20 | `INVALID_QUANTITY` |
| `unit_price` not numeric > 0 (strips `THB` prefixes first) | `INVALID_UNIT_PRICE` |
| `discount_pct` not in 0–100 | `INVALID_DISCOUNT_PCT` |
| `customer_id` missing / not found in `dim_customer` | `MISSING_CUSTOMER_ID` / `CUSTOMER_NOT_FOUND` |
| `product_id` missing / not found in `dim_product` | `MISSING_PRODUCT_ID` / `PRODUCT_NOT_FOUND` |
| product exists but `active_flag != 'Y'` | `INACTIVE_PRODUCT` |
| `payment_method` not one of Cash/Credit Card/PromptPay/Bank Transfer (case-insensitive) | `INVALID_PAYMENT_METHOD` |
| `sales_channel` not one of Store/Online/Marketplace (`E-Commerce` → `Online`) | `INVALID_SALES_CHANNEL` |

`gross_amount = quantity * unit_price` and `net_amount = gross_amount * (1 -
discount_pct/100)` are computed **only** for rows that pass every check
above, so they are never negative.

## 5. Idempotency & incremental loading

- **Idempotency**: `fact_sales.order_id` is the primary key, and loads use
  `INSERT ... ON CONFLICT(order_id) DO UPDATE`. Running the same batch twice
  updates the same rows in place instead of adding new ones.
- **Incremental loading**: `batch_watermark` stores the highest
  `updated_at` seen per batch. On each run, rows with `updated_at` at or
  before the watermark are skipped before validation even runs (rows with
  an unparseable `updated_at` are always kept, so they still reach
  quarantine with a reason code rather than being silently dropped). The
  watermark is advanced using every row *read* — including rejected ones —
  so a rerun never reprocesses a batch that already ran to completion.

Evidence: run `python pipeline.py` — the console output labels each of the
4 runs (`batch_1` initial, `batch_1` rerun, `batch_2`, `batch_3`) and prints
the run log table after each one; `pipeline_run_log.csv` keeps the same
evidence on disk.

## 6. KPI formula (Acceptance Test 7)

For every batch run: **`rows_read == rows_valid + rows_rejected`**, measured
*before* deduplication (`transform_batch` computes `rows_valid` /
`rows_rejected` on the full filtered extract). Deduplication happens
afterwards, inside `load_facts`, and is reported separately as
`rows_duplicated`; `rows_loaded = rows_valid - rows_duplicated` is how many
rows were actually upserted into `fact_sales`.

## 7. Reflection — why Availability usually beats Strictness in a production pipeline

A pipeline that halts the moment it meets one bad row optimizes for
correctness on paper but fails the business goal it exists for: keeping the
warehouse close to current. Source systems are messy by nature — a typo'd
discount, a currency prefix, a customer who hasn't synced yet — and none of
that is under the pipeline's control. If a single malformed row can take
down an entire batch, then one flaky upstream record blocks every other
valid record behind it, and dashboards silently go stale while someone
tracks down the offending row. Quarantining bad records with a clear
`reason_code` gets the 90%+ of good data into the warehouse on schedule and
turns the bad rows into a queue someone can triage later, rather than an
outage. Strictness still matters — it's just enforced at the row level
(reject that row) instead of the batch or pipeline level. The one place
strictness should win is when an error is structural rather than
data-level — e.g. a source file is missing or a whole batch is unreadable —
because guessing at malformed structure risks loading systematically wrong
data rather than a handful of bad rows. That's why this pipeline logs a
failed batch and moves on without touching data already loaded from other
batches, but never lets one bad row stop a batch that is otherwise readable.
