"""Airflow DAG for the StreamCommerce batch pipeline.

Each task calls ``src.batch_pipeline.run_step`` with the Airflow run_id, so every step is idempotent and
re-runnable (manifest checksums skip unchanged files; the processed layer upserts by business key; the warehouse
load is a transactional full refresh). Backfills therefore cannot duplicate data.

Configuration (environment variables, never credentials in code):
  STREAMCOMMERCE_SCHEDULE   cron expression or @daily (default), or "none" for manual runs only
  STREAMCOMMERCE_CATCHUP    "true" to enable catch-up/backfill runs (default false)
  STREAMCOMMERCE_ENGINE     pandas | spark (default spark inside the Airflow image)
"""
from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(os.environ.get("STREAMCOMMERCE_HOME", Path(__file__).resolve().parents[1]))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.batch_pipeline import RunContext, fail_run, run_step  # noqa: E402
from src.common.config import Settings  # noqa: E402
from src.metadata.repository import get_repository  # noqa: E402

log = logging.getLogger(__name__)
SCHEDULE = os.environ.get("STREAMCOMMERCE_SCHEDULE", "@daily")
CATCHUP = os.environ.get("STREAMCOMMERCE_CATCHUP", "false").lower() == "true"
ENGINE = os.environ.get("STREAMCOMMERCE_ENGINE", "spark")


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def notify_failure(context: dict) -> None:
    """Local failure notification: structured log line + append to data/processed/alerts.log + mark run failed."""
    ti = context["task_instance"]
    message = f"task {ti.task_id} failed (try {ti.try_number}) in run {context['run_id']}: {context.get('exception')}"
    log.error("pipeline failure", extra={"task": ti.task_id, "run_id": context["run_id"]})
    settings = Settings.from_env()
    settings.processed_dir.mkdir(parents=True, exist_ok=True)
    with (settings.processed_dir / "alerts.log").open("a", encoding="utf-8") as handle:
        handle.write(f"{datetime.utcnow().isoformat()}Z {message}\n")
    if ti.try_number >= (ti.max_tries or 0) + 1:  # final attempt only
        fail_run(settings, _safe_run_id(context["run_id"]), message, ti.task_id)


def run_pipeline_step(step: str, **context) -> None:
    """Airflow callable: run one pipeline step for this DAG run."""
    settings = Settings.from_env()
    settings.ensure_dirs()
    repo = get_repository(settings)
    run_id = _safe_run_id(context["run_id"])
    repo.start_run(run_id, "airflow_batch", ENGINE)
    params = context["params"]
    generate = None
    if step == "prepare" and params.get("generate"):
        generate = {"customers": params["customers"], "products": params["products"], "orders": params["orders"],
                    "seed": params["seed"], "faults": False}
    ctx = RunContext(settings, repo, run_id, engine=ENGINE, force=bool(params.get("force")), generate=generate)
    run_step(step, ctx)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="streamcommerce_batch_pipeline",
    description="Generate/ingest CSV -> validate -> PySpark clean -> DuckDB -> dbt -> quality -> summary",
    start_date=datetime(2025, 1, 1),
    schedule=None if SCHEDULE.lower() == "none" else SCHEDULE,
    catchup=CATCHUP,
    max_active_runs=1,
    default_args=default_args,
    params={"generate": True, "customers": 1000, "products": 200, "orders": 10000, "seed": 42, "force": False},
    tags=["streamcommerce", "batch"],
) as dag:
    steps = ["prepare", "ingest", "validate", "process", "load_warehouse", "dbt", "final_quality", "summary"]
    tasks = {
        s: PythonOperator(task_id=s, python_callable=run_pipeline_step, op_kwargs={"step": s}) for s in steps
    }
    previous = None
    for name in steps:
        if previous is not None:
            previous >> tasks[name]
        previous = tasks[name]
