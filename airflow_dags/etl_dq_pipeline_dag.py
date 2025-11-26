# etl_dq_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

# Update these paths for your environment (assuming Linux in real deployments)
PROJECT_DIR = "/opt/etl_automation_mysql"
VENV_BIN = f"{PROJECT_DIR}/venv/bin/python"
PIPELINE_SCRIPT = f"{PROJECT_DIR}/venv/run_pipeline.py"

default_args = {
    "owner": "etl_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_dq_pipeline",
    default_args=default_args,
    description="ETL + dbt + GE + DQ scorecard pipeline",
    schedule_interval="0 1 * * *",  # daily at 01:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    run_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=f"cd {PROJECT_DIR} && {VENV_BIN} {PIPELINE_SCRIPT}",
    )

    # You could split into tasks if you want:
    # - dbt_run
    # - dbt_test
    # - ge_validations
    # - dq_scorecard
    # For simplicity, here it's a single callable.

    run_pipeline