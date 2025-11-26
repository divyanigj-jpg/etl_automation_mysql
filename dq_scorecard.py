# dq_scorecard.py
import json
from pathlib import Path
from datetime import datetime

import pymysql


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "Infy@1234",
    "database": "etl_demo",
}

PROJECT_ROOT = Path(__file__).parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def _conn():
    return pymysql.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"],
        cursorclass=pymysql.cursors.Cursor,
    )


def _scalar(sql: str):
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def build_scorecard():
    """
    Build a simple DQ scorecard for src_orders, stg_orders, fct_orders_summary.

    Returns:
        (scorecard_dict, scorecard_json_path, scorecard_html_path)
    """
    tables = ["src_orders", "stg_orders", "fct_orders_summary"]
    score = {}

    for table in tables:
        row_count = _scalar(f"SELECT COUNT(*) FROM {table};")

        null_customer = None
        negative_amount = None

        if table in ("src_orders", "stg_orders"):
            null_customer = _scalar(
                f"SELECT COUNT(*) FROM {table} WHERE customer_id IS NULL;"
            )
            negative_amount = _scalar(
                f"SELECT COUNT(*) FROM {table} WHERE amount < 0;"
            )
        elif table == "fct_orders_summary":
            negative_amount = _scalar(
                f"SELECT COUNT(*) FROM {table} WHERE total_amount < 0;"
            )

        status = "PASS"
        if row_count == 0:
            status = "FAIL"
        if null_customer is not None and null_customer > 0:
            status = "FAIL"
        if negative_amount is not None and negative_amount > 0:
            status = "FAIL"

        score[table] = {
            "status": status,
            "row_count": int(row_count),
            "null_customer_id": int(null_customer) if null_customer is not None else None,
            "negative_amount_rows": int(negative_amount) if negative_amount is not None else None,
        }

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = REPORTS_DIR / f"dq_scorecard_{ts}.json"
    html_path = REPORTS_DIR / f"dq_scorecard_{ts}.html"

    # Save JSON
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(score, f, indent=2)

    # Save also as "latest" for Behave to read easily
    latest_json = REPORTS_DIR / "dq_scorecard_latest.json"
    with latest_json.open("w", encoding="utf-8") as f:
        json.dump(score, f, indent=2)

    # Simple HTML report
    html_lines = [
        "<html><body><h2>Data Quality Scorecard</h2>",
        "<table border='1' cellspacing='0' cellpadding='4'>",
        "<tr><th>Table</th><th>Status</th><th>Rows</th><th>Null customer_id</th><th>Negative amount rows</th></tr>",
    ]
    for table, info in score.items():
        html_lines.append(
            "<tr>"
            f"<td>{table}</td>"
            f"<td>{info['status']}</td>"
            f"<td>{info['row_count']}</td>"
            f"<td>{info['null_customer_id']}</td>"
            f"<td>{info['negative_amount_rows']}</td>"
            "</tr>"
        )
    html_lines.append("</table></body></html>")

    with html_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))

    latest_html = REPORTS_DIR / "dq_scorecard_latest.html"
    with latest_html.open("w", encoding="utf-8") as f:
        f.write("\n".join(html_lines))

    return score, str(json_path), str(html_path)