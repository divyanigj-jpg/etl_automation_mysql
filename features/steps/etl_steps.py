from behave import given, then
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import json

import pymysql


# ---------- Paths & DB config ----------

ROOT_DIR = Path(__file__).resolve().parents[2]
PIPELINE_DIR = ROOT_DIR / "venv"
PIPELINE_FILE = PIPELINE_DIR / "run_pipeline.py"

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "Infy@1234",
    "database": "etl_demo",
}

REPORTS_DIR = ROOT_DIR / "reports"


# ---------- Helper functions ----------

def run_pipeline():
    """Run the full ETL pipeline (dbt + GE + DQ scorecard + performance)."""
    print(f"Running pipeline: {PIPELINE_FILE}")
    result = subprocess.run(
        [sys.executable, str(PIPELINE_FILE)],
        cwd=str(PIPELINE_DIR),
        shell=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"Pipeline failed with exit code {result.returncode}")


def _get_connection():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.Cursor,
    )


def query(sql, params=None):
    with _get_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def scalar(sql, params=None):
    return query(sql, params)[0][0]


def load_latest_scorecard():
    path = REPORTS_DIR / "dq_scorecard_latest.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_latest_performance():
    path = REPORTS_DIR / "performance_metrics_latest.json"
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ---------- Steps ----------

@given("the ETL pipeline has run")
def step_run_pipeline(context):
    run_pipeline()


# ----- Metadata check -----

@then('table "{table}" should have columns "{columns}"')
def step_metadata_check(context, table, columns):
    expected_cols = {c.strip() for c in columns.split(",")}
    cols = query(f"SHOW COLUMNS FROM {table};")
    actual_cols = {row[0] for row in cols}
    missing = expected_cols - actual_cols
    assert not missing, f"Table {table} is missing columns: {missing}"


# ----- Count check -----

@then('table "{table}" should have at least {min_rows:d} rows')
def step_min_rows(context, table, min_rows):
    count = scalar(f"SELECT COUNT(*) FROM {table};")
    assert count >= min_rows, f"{table} has only {count} rows (< {min_rows})."


# ----- Null check -----

@then('column "{column}" in table "{table}" should not contain nulls')
def step_not_null(context, column, table):
    nulls = scalar(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL;")
    assert nulls == 0, f"{table}.{column} has {nulls} NULL values."


# ----- Duplicate check -----

@then('table "{table}" should have no duplicate rows on column "{column}"')
def step_no_duplicates(context, table, column):
    dup = scalar(f"""
        SELECT COUNT(*) FROM (
            SELECT {column}, COUNT(*) AS c
            FROM {table}
            GROUP BY {column}
            HAVING c > 1
        ) t;
    """)
    assert dup == 0, f"Duplicates found in {table}.{column}: {dup}"


# ----- No negative amount check -----

@then('column "{column}" in table "{table}" should have no negative values')
def step_no_negative(context, column, table):
    neg = scalar(f"SELECT COUNT(*) FROM {table} WHERE {column} < 0;")
    assert neg == 0, f"Negative values found in {table}.{column}: {neg}"


# ----- Valid date columns check -----

@then('column "{column}" in table "{table}" should contain only valid dates')
def step_valid_dates(context, column, table):
    rows = query(f"SELECT {column} FROM {table};")
    for (value,) in rows:
        # If it's already a date/datetime object, it's fine
        if isinstance(value, (datetime,)):
            continue
        # Else assume string in YYYY-MM-DD (adjust if your format differs)
        try:
            datetime.strptime(str(value), "%Y-%m-%d")
        except Exception:
            raise AssertionError(f"Invalid date value in {table}.{column}: {value}")


# ----- DQ scorecard check -----

@then('DQ scorecard status for table "{table}" should be "{expected_status}"')
def step_dq_scorecard_status(context, table, expected_status):
    scorecard = load_latest_scorecard()
    assert table in scorecard, f"Table {table} not found in DQ scorecard."
    actual = scorecard[table]["status"]
    assert actual == expected_status, (
        f"DQ scorecard status for {table} is {actual}, expected {expected_status}"
    )


# ----- Performance metrics checks -----

@then('performance metric "{metric_name}" should be recorded')
def step_perf_metric_recorded(context, metric_name):
    perf = load_latest_performance()
    timings = perf.get("timings", {})
    assert metric_name in timings, f"Metric {metric_name} not found in performance metrics."
    assert timings[metric_name] >= 0, f"Metric {metric_name} has invalid value {timings[metric_name]}"