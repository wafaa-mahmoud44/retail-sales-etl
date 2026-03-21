"""
File: dags/retail_sales_etl.py
Purpose: Airflow DAG - Full ETL pipeline for retail sales data.
Schema Layout:
    staging.stg_sales  -> raw landing zone
    dwh.dim_date       -> date dimension
    dwh.dim_customer   -> customer dimension (SCD2)
    dwh.dim_product    -> product dimension (SCD2)
    dwh.fact_sales     -> fact table
"""

import os
import sys
import glob
import shutil
import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ── Paths ──────────────────────────────────────────────────────────────────
DAG_FOLDER  = os.path.dirname(os.path.abspath(__file__))
ROOT_FOLDER = os.path.dirname(DAG_FOLDER)
DATA_FOLDER = os.path.join(ROOT_FOLDER, "data")
INCLUDE_DIR = os.path.join(ROOT_FOLDER, "include")
SQL_DIR     = os.path.join(INCLUDE_DIR, "sql")
ARCHIVE_DIR = os.path.join(DATA_FOLDER, "archive")
TMP_CLEAN   = "/tmp/retail_cleaned.parquet"

if INCLUDE_DIR not in sys.path:
    sys.path.insert(0, INCLUDE_DIR)

from transform_helpers import (
    extract_csv_files,
    clean_and_transform,
    build_dim_date,
    build_dim_customer,
    build_dim_product,
    detect_customer_changes,
    detect_product_changes,
)

POSTGRES_CONN_ID = "retail_dwh"
log = logging.getLogger(__name__)

default_args = {
    "owner"           : "data_team",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

with DAG(
    dag_id      = "retail_sales_etl",
    description = "End-to-end ETL: CSV to PostgreSQL Data Warehouse",
    default_args     = default_args,
    start_date       = datetime(2025, 1, 1),
    schedule         = "@monthly",
    catchup          = False,
    tags             = ["retail", "etl", "data-warehouse"],
) as dag:

    # ── TASK 1 ────────────────────────────────────────────────────────────
    def _create_dw_objects():
        sql_file = os.path.join(SQL_DIR, "create_dw_tables.sql")
        with open(sql_file, "r") as f:
            sql = f.read()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        hook.run(sql)
        log.info("All DW schemas and tables created / verified.")

    create_dw_objects = PythonOperator(
        task_id         = "create_dw_objects",
        python_callable = _create_dw_objects,
    )

    # ── TASK 2 ────────────────────────────────────────────────────────────
    def _extract_and_clean(**context):
        log.info(f"Reading CSVs from: {DATA_FOLDER}")
        raw_df     = extract_csv_files(DATA_FOLDER)
        cleaned_df = clean_and_transform(raw_df)
        cleaned_df.to_parquet(TMP_CLEAN, index=False)
        log.info(f"Cleaned data saved: {len(cleaned_df)} rows")
        context["ti"].xcom_push(key="row_count", value=len(cleaned_df))

    extract_and_clean = PythonOperator(
        task_id         = "extract_and_clean",
        python_callable = _extract_and_clean,
    )

    # ── TASK 3 ────────────────────────────────────────────────────────────
    def _load_staging():
        df   = pd.read_parquet(TMP_CLEAN)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute("TRUNCATE TABLE staging.stg_sales;")

        insert_sql = """
            INSERT INTO staging.stg_sales (
                source_file, batch_id,
                order_id, order_date, customer_id, customer_name,
                city, country, product_id, product_name, category,
                quantity, unit_price, unit_cost,
                sales_amount, total_cost, profit_amount
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        rows = [
            (
                row.batch_id,
                row.batch_id,
                row.order_id,
                row.order_date.date() if hasattr(row.order_date, "date") else row.order_date,
                row.customer_id, row.customer_name,
                row.city, row.country,
                row.product_id, row.product_name, row.category,
                int(row.quantity), float(row.unit_price), float(row.unit_cost),
                float(row.sales_amount), float(row.total_cost), float(row.profit_amount),
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_sql, rows)
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"{len(rows)} rows loaded into staging.stg_sales.")

    load_staging = PythonOperator(
        task_id         = "load_staging",
        python_callable = _load_staging,
    )

    # ── TASK 4 ────────────────────────────────────────────────────────────
    def _load_dimensions():
        df   = pd.read_parquet(TMP_CLEAN)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        # DIM_DATE
        dim_date_df = build_dim_date(df)
        date_insert = """
            INSERT INTO dwh.dim_date
                (date_key, full_date, day_num, month_num, month_name,
                 quarter_num, year_num, week_num, is_weekend)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_key) DO NOTHING;
        """
        date_rows = [
            (int(r.date_key),
             r.full_date.date() if hasattr(r.full_date, "date") else r.full_date,
             int(r.day_num), int(r.month_num), r.month_name,
             int(r.quarter_num), int(r.year_num), int(r.week_num), bool(r.is_weekend))
            for r in dim_date_df.itertuples(index=False)
        ]
        cur.executemany(date_insert, date_rows)

        # DIM_CUSTOMER (SCD2)
        dim_cust_df = build_dim_customer(df)
        for _, new in dim_cust_df.iterrows():
            cur.execute(
                "SELECT customer_key, city, country FROM dwh.dim_customer "
                "WHERE customer_id = %s AND is_current = TRUE",
                (new["customer_id"],)
            )
            existing = cur.fetchone()
            if existing is None:
                cur.execute(
                    """INSERT INTO dwh.dim_customer
                       (customer_id, customer_name, city, country,
                        effective_from, effective_to, is_current)
                       VALUES (%s,%s,%s,%s,%s,NULL,TRUE)""",
                    (new["customer_id"], new["customer_name"],
                     new["city"], new["country"], new["effective_from"])
                )
            else:
                old      = {"city": existing[1], "country": existing[2]}
                new_dict = {"city": new["city"], "country": new["country"]}
                if detect_customer_changes(new_dict, old):
                    cur.execute(
                        "UPDATE dwh.dim_customer SET effective_to=%s, is_current=FALSE "
                        "WHERE customer_key=%s",
                        (new["effective_from"], existing[0])
                    )
                    cur.execute(
                        """INSERT INTO dwh.dim_customer
                           (customer_id, customer_name, city, country,
                            effective_from, effective_to, is_current)
                           VALUES (%s,%s,%s,%s,%s,NULL,TRUE)""",
                        (new["customer_id"], new["customer_name"],
                         new["city"], new["country"], new["effective_from"])
                    )

        # DIM_PRODUCT (SCD2)
        dim_prod_df = build_dim_product(df)
        for _, new in dim_prod_df.iterrows():
            cur.execute(
                "SELECT product_key, product_name, category FROM dwh.dim_product "
                "WHERE product_id = %s AND is_current = TRUE",
                (new["product_id"],)
            )
            existing = cur.fetchone()
            if existing is None:
                cur.execute(
                    """INSERT INTO dwh.dim_product
                       (product_id, product_name, category,
                        effective_from, effective_to, is_current)
                       VALUES (%s,%s,%s,%s,NULL,TRUE)""",
                    (new["product_id"], new["product_name"],
                     new["category"], new["effective_from"])
                )
            else:
                old      = {"product_name": existing[1], "category": existing[2]}
                new_dict = {"product_name": new["product_name"], "category": new["category"]}
                if detect_product_changes(new_dict, old):
                    cur.execute(
                        "UPDATE dwh.dim_product SET effective_to=%s, is_current=FALSE "
                        "WHERE product_key=%s",
                        (new["effective_from"], existing[0])
                    )
                    cur.execute(
                        """INSERT INTO dwh.dim_product
                           (product_id, product_name, category,
                            effective_from, effective_to, is_current)
                           VALUES (%s,%s,%s,%s,NULL,TRUE)""",
                        (new["product_id"], new["product_name"],
                         new["category"], new["effective_from"])
                    )

        conn.commit()
        cur.close()
        conn.close()
        log.info("All dimensions loaded into dwh schema.")

    load_dimensions = PythonOperator(
        task_id         = "load_dimensions",
        python_callable = _load_dimensions,
    )

    # ── TASK 5 ────────────────────────────────────────────────────────────
    def _load_fact():
        df   = pd.read_parquet(TMP_CLEAN)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()
        inserted = 0
        skipped  = 0
        for row in df.itertuples(index=False):
            cur.execute(
                "SELECT customer_key FROM dwh.dim_customer "
                "WHERE customer_id=%s AND is_current=TRUE",
                (row.customer_id,)
            )
            cust_result = cur.fetchone()
            cur.execute(
                "SELECT product_key FROM dwh.dim_product "
                "WHERE product_id=%s AND is_current=TRUE",
                (row.product_id,)
            )
            prod_result = cur.fetchone()
            if not cust_result or not prod_result:
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO dwh.fact_sales (
                    order_id, date_key, customer_key, product_key,
                    quantity, unit_price, unit_cost,
                    sales_amount, total_cost, profit_amount, batch_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id) DO NOTHING
                """,
                (
                    row.order_id, int(row.date_key),
                    cust_result[0], prod_result[0],
                    int(row.quantity), float(row.unit_price), float(row.unit_cost),
                    float(row.sales_amount), float(row.total_cost),
                    float(row.profit_amount), row.batch_id,
                )
            )
            if cur.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"dwh.fact_sales: {inserted} inserted, {skipped} skipped.")

    load_fact = PythonOperator(
        task_id         = "load_fact",
        python_callable = _load_fact,
    )

    # ── TASK 6 ────────────────────────────────────────────────────────────
    def _data_quality(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()
        errors = []

        cur.execute("SELECT COUNT(*) FROM staging.stg_sales;")
        stg_count = cur.fetchone()[0]
        if stg_count == 0:
            errors.append("FAIL: staging.stg_sales is empty!")

        cur.execute("SELECT COUNT(*) FROM dwh.fact_sales;")
        fact_count = cur.fetchone()[0]
        if fact_count == 0:
            errors.append("FAIL: dwh.fact_sales is empty!")

        cur.execute("SELECT COUNT(*) FROM dwh.fact_sales WHERE profit_amount IS NULL;")
        if cur.fetchone()[0] > 0:
            errors.append("FAIL: NULL profit_amount found!")

        cur.execute("""
            SELECT COUNT(*) FROM dwh.fact_sales f
            LEFT JOIN dwh.dim_customer c ON f.customer_key = c.customer_key
            WHERE c.customer_key IS NULL;
        """)
        if cur.fetchone()[0] > 0:
            errors.append("FAIL: orphan customer keys!")

        cur.execute("""
            SELECT COUNT(*) FROM dwh.fact_sales f
            LEFT JOIN dwh.dim_product p ON f.product_key = p.product_key
            WHERE p.product_key IS NULL;
        """)
        if cur.fetchone()[0] > 0:
            errors.append("FAIL: orphan product keys!")

        cur.close()
        conn.close()

        if errors:
            raise ValueError("Data Quality FAILED:\n" + "\n".join(errors))
        log.info(f"Data quality passed. staging={stg_count}, fact={fact_count} rows.")

    data_quality = PythonOperator(
        task_id         = "data_quality",
        python_callable = _data_quality,
    )

    # ── TASK 7 ────────────────────────────────────────────────────────────
    def _archive_source_files(**context):
        run_date    = context["ds"]
        archive_dir = os.path.join(ARCHIVE_DIR, run_date)
        os.makedirs(archive_dir, exist_ok=True)
        csv_files = glob.glob(os.path.join(DATA_FOLDER, "sales_*.csv"))
        for f in csv_files:
            shutil.move(f, os.path.join(archive_dir, os.path.basename(f)))
        if os.path.exists(TMP_CLEAN):
            os.remove(TMP_CLEAN)
        log.info(f"{len(csv_files)} files archived.")

    archive_source_files = PythonOperator(
        task_id         = "archive_source_files",
        python_callable = _archive_source_files,
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    (
        create_dw_objects
        >> extract_and_clean
        >> load_staging
        >> load_dimensions
        >> load_fact
        >> data_quality
        >> archive_source_files
    )