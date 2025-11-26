import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime

from dq_scorecard import build_scorecard


# ---- PATH CONFIG ----
DBT_PROJECT_DIR = Path(r"C:\Users\samhi\etl_automation_mysql\analytics")
GE_SCRIPTS_DIR = Path(r"C:\Users\samhi\etl_automation_mysql\venv")

PROJECT_ROOT = Path(__file__).parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# ---- LOGGING ----
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"pipeline_{run_id}.log"

logging.basicConfig(
    filename=str(log_file),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def run_cmd(cmd, cwd=None, description="command"):
    """
    Run a shell command, log it, and return its duration in seconds.
    Raises RuntimeError if the command fails.
    """
    logging.info("=== Running: %s ===", description)
    logging.info("Command: %s", " ".join(cmd))
    start = time.perf_counter()
    result = subprocess.run(cmd, cwd=cwd, shell=False)
    duration = time.perf_counter() - start

    if result.returncode != 0:
        logging.error(
            "❌ %s FAILED in %.2f seconds (exit code=%s)",
            description,
            duration,
            result.returncode,
        )
        raise RuntimeError(f"{description} failed with exit code {result.returncode}")

    logging.info("✅ %s completed in %.2f seconds", description, duration)
    return duration


def run_dbt(timings: dict):
    timings["dbt_run"] = run_cmd(
        ["dbt", "run"],
        cwd=str(DBT_PROJECT_DIR),
        description="dbt run",
    )
    timings["dbt_test"] = run_cmd(
        ["dbt", "test"],
        cwd=str(DBT_PROJECT_DIR),
        description="dbt test",
    )


def run_ge_validations(timings: dict):
    timings["ge_src_orders"] = run_cmd(
        [sys.executable, "run_ge_src_orders.py"],
        cwd=str(GE_SCRIPTS_DIR),
        description="GE validation: src_orders",
    )
    timings["ge_stg_orders"] = run_cmd(
        [sys.executable, "run_ge_stg_orders.py"],
        cwd=str(GE_SCRIPTS_DIR),
        description="GE validation: stg_orders",
    )
    timings["ge_fct_orders"] = run_cmd(
        [sys.executable, "run_ge_fct_orders_summary.py"],
        cwd=str(GE_SCRIPTS_DIR),
        description="GE validation: fct_orders_summary",
    )


def save_performance_metrics(timings: dict):
    """
    Save timings as JSON so Behave can read them for tests.
    """
    import json

    data = {
        "run_id": run_id,
        "timings": timings,
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    perf_path = REPORTS_DIR / f"performance_metrics_{ts}.json"
    latest = REPORTS_DIR / "performance_metrics_latest.json"

    with perf_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    with latest.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    logging.info("Performance metrics saved to %s and %s", perf_path, latest)


def main():
    logging.info("🚀 Starting ETL pipeline (run_id=%s)", run_id)
    timings = {}
    overall_start = time.perf_counter()
    success = False

    try:
        # 1) dbt transformations
        run_dbt(timings)

        # 2) GE validations
        run_ge_validations(timings)

        # 3) Data Quality Scorecard
        scorecard, _, _ = build_scorecard()
        logging.info("DQ Scorecard: %s", scorecard)

        success = all(info["status"] == "PASS" for info in scorecard.values())

    except Exception as e:
        logging.exception("Pipeline FAILED: %s", e)
        success = False
    finally:
        total = time.perf_counter() - overall_start
        timings["total_pipeline"] = total
        logging.info("Total pipeline time: %.2f seconds", total)

        save_performance_metrics(timings)

        logging.info("=== Performance summary ===")
        for step, t in timings.items():
            logging.info("  %-20s : %.2f s", step, t)

        if not success:
            logging.error("Pipeline marked as FAILED.")
            sys.exit(1)

        logging.info("🎉 Pipeline completed successfully (status=%s).", success)


if __name__ == "__main__":
    main()