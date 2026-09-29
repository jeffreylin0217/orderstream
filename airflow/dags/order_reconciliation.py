"""One daily workflow. The stream runs independently of this scheduler."""

import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ["ORDERSTREAM_PROJECT_DIR"]
PYTHON = os.environ["ORDERSTREAM_PYTHON"]


def report_failure(context):
    logging.error(
        "Reconciliation failed: dag=%s task=%s run=%s",
        context["dag"].dag_id,
        context["task_instance"].task_id,
        context["run_id"],
    )


with DAG(
    dag_id="order_reconciliation",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": report_failure,
    },
    description="Validate the previous UTC event day and refresh daily deliveries",
    tags=["orders", "quality"],
) as dag:
    tasks = []
    for action in ("inspect", "validate", "refresh"):
        tasks.append(
            BashOperator(
                task_id=action,
                bash_command='cd "$PROJECT_DIR" && "$PIPELINE_PYTHON" -m reconciliation.daily '
                + action
                + ' --day "$RUN_DAY" --report "data/reconciliation/$RUN_DAY.json"',
                env={
                    "PROJECT_DIR": PROJECT_DIR,
                    "PIPELINE_PYTHON": PYTHON,
                    "RUN_DAY": "{{ data_interval_start | ds }}",
                },
                append_env=True,
            )
        )
    tasks[0] >> tasks[1] >> tasks[2]
