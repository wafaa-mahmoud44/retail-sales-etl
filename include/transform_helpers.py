
import os
import glob
import pandas as pd
from datetime import date


# ──────────────────────────────────────────────────────────────
# 1.  EXTRACT  – read all CSV files from the data/ folder
# ──────────────────────────────────────────────────────────────
def extract_csv_files(data_folder: str) -> pd.DataFrame:
    """
    Reads every sales_*.csv file in data_folder,
    tags each row with its source file name (batch_id),
    and returns one combined DataFrame.
    """
    pattern = os.path.join(data_folder, "sales_*.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise FileNotFoundError(f"No CSV files found in: {data_folder}")

    frames = []
    for f in files:
        df = pd.read_csv(f, dtype=str)          # read everything as text first
        df["batch_id"] = os.path.basename(f)    # e.g. sales_2025_01.csv
        frames.append(df)
        print(f"  ✓ Loaded {len(df)} rows from {os.path.basename(f)}")

    combined = pd.concat(frames, ignore_index=True)
    print(f"  → Total rows extracted: {len(combined)}")
    return combined


# ──────────────────────────────────────────────────────────────
# 2.  CLEAN / TRANSFORM
# ──────────────────────────────────────────────────────────────
def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Performs all cleaning steps and derives calculated columns.
    Returns a cleaned DataFrame ready for loading.
    """
    original_count = len(df)

    # ── 2a. Column names: strip spaces and lower-case ──────────
    df.columns = df.columns.str.strip().str.lower()

    # ── 2b. Remove fully duplicate rows ────────────────────────
    df = df.drop_duplicates()

    # ── 2c. Drop rows with null values in critical columns ──────
    critical_cols = ["order_id", "order_date", "customer_id",
                     "product_id", "quantity", "unit_price", "unit_cost"]
    df = df.dropna(subset=critical_cols)

    # ── 2d. Parse & validate types ──────────────────────────────
    df["order_date"]  = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])           # drop unparseable dates

    df["quantity"]    = pd.to_numeric(df["quantity"],   errors="coerce")
    df["unit_price"]  = pd.to_numeric(df["unit_price"], errors="coerce")
    df["unit_cost"]   = pd.to_numeric(df["unit_cost"],  errors="coerce")
    df = df.dropna(subset=["quantity", "unit_price", "unit_cost"])

    # ── 2e. Business rule: remove negative / zero quantities ────
    df = df[df["quantity"] > 0]
    df = df[df["unit_price"] > 0]

    # ── 2f. Strip leading/trailing whitespace from text columns ─
    str_cols = ["customer_id", "customer_name", "city", "country",
                "product_id", "product_name", "category"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # ── 2g. Title-case names and cities ────────────────────────
    df["customer_name"] = df["customer_name"].str.title()
    df["product_name"]  = df["product_name"].str.title()
    df["city"]          = df["city"].str.title()
    df["country"]       = df["country"].str.title()
    df["category"]      = df["category"].str.title()

    # ── 2h. Derive calculated columns ───────────────────────────
    df["sales_amount"]  = (df["quantity"] * df["unit_price"]).round(2)
    df["total_cost"]    = (df["quantity"] * df["unit_cost"]).round(2)
    df["profit_amount"] = (df["sales_amount"] - df["total_cost"]).round(2)

    # ── 2i. Add date_key (integer YYYYMMDD) ─────────────────────
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)

    cleaned_count = len(df)
    print(f"  → Cleaned: {original_count} → {cleaned_count} rows "
          f"(removed {original_count - cleaned_count} bad rows)")
    return df


# ──────────────────────────────────────────────────────────────
# 3.  BUILD DIM TABLES from the cleaned DataFrame
# ──────────────────────────────────────────────────────────────
def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Generate dim_date rows for every unique date in the data."""
    dates = df["order_date"].drop_duplicates().reset_index(drop=True)
    dim = pd.DataFrame()
    dim["full_date"]   = dates
    dim["date_key"]    = dates.dt.strftime("%Y%m%d").astype(int)
    dim["day_num"]     = dates.dt.day
    dim["month_num"]   = dates.dt.month
    dim["month_name"]  = dates.dt.strftime("%B")     # January, February …
    dim["quarter_num"] = dates.dt.quarter
    dim["year_num"]    = dates.dt.year
    dim["week_num"]    = dates.dt.isocalendar().week.astype(int)
    dim["is_weekend"]  = dates.dt.dayofweek >= 5     # True for Sat & Sun
    return dim.drop_duplicates(subset=["date_key"])


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build SCD Type 2 customer dimension.
    Detects changes in city/country → closes old record, opens new one.
    For initial load, every row is simply the current version.
    """
    # Take the LAST known record per customer (most recent order date)
    dim = (
        df.sort_values("order_date")
          .groupby("customer_id")
          .last()
          .reset_index()
    )[["customer_id", "customer_name", "city", "country", "order_date"]]

    dim = dim.rename(columns={"order_date": "effective_from"})
    dim["effective_from"] = dim["effective_from"].dt.date
    dim["effective_to"]   = None        # NULL = currently active
    dim["is_current"]     = True
    return dim


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build SCD Type 2 product dimension.
    Detects changes in product_name/category per product_id.
    """
    dim = (
        df.sort_values("order_date")
          .groupby("product_id")
          .last()
          .reset_index()
    )[["product_id", "product_name", "category", "order_date"]]

    dim = dim.rename(columns={"order_date": "effective_from"})
    dim["effective_from"] = dim["effective_from"].dt.date
    dim["effective_to"]   = None
    dim["is_current"]     = True
    return dim


# ──────────────────────────────────────────────────────────────
# 4.  DETECT SCD TYPE 2 CHANGES (used during incremental loads)
# ──────────────────────────────────────────────────────────────
def detect_customer_changes(new_row: dict, existing_row: dict) -> bool:
    """Returns True if customer city or country changed."""
    return (new_row["city"]    != existing_row["city"] or
            new_row["country"] != existing_row["country"])


def detect_product_changes(new_row: dict, existing_row: dict) -> bool:
    """Returns True if product_name or category changed."""
    return (new_row["product_name"] != existing_row["product_name"] or
            new_row["category"]     != existing_row["category"])
