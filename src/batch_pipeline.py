"""Batch pipeline runner (lightweight mode and the task implementations used by Airflow).

Steps (each is independently runnable and idempotent):
prepare -> ingest -> validate -> process -> load_warehouse -> dbt -> final_quality -> summary

Examples:
    python -m src.batch_pipeline --generate --orders 2000          # everything except dbt
    python -m src.batch_pipeline --generate --with-dbt             # including dbt build
    python -m src.batch_pipeline --steps process,final_quality --run-id my-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from src.common.config import PROJECT_ROOT, Settings
from src.common.exceptions import ConfigError, IngestionError, MissingDependencyError, StreamCommerceError
from src.common.logging_config import configure_logging
from src.generators.cli import generate_dataset
from src.ingestion.csv_ingestion import ingest_directory, validate_pending
from src.metadata.models import STATUS_FAILED, STATUS_SUCCESS
from src.metadata.repository import MetadataRepository, get_repository
from src.processing.batch_processor import process_pending
from src.quality.final_checks import run_final_checks
from src.warehouse.load_warehouse import load_warehouse

log = logging.getLogger(__name__)
STEPS = ["prepare", "ingest", "validate", "process", "load_warehouse", "dbt", "final_quality", "summary"]
DBT_DIR = PROJECT_ROOT / "dbt_project"


@dataclass
class RunContext:
    settings: Settings
    repo: MetadataRepository
    run_id: str
    engine: str = "pandas"
    input_dir: Path | None = None
    force: bool = False
    generate: dict | None = None
    output: dict = field(default_factory=dict)


def run_dbt(settings: Settings) -> str:
    """Run ``dbt build`` against the local DuckDB warehouse; raise with dbt's real output on failure."""
    dbt = os.environ.get("DBT_EXECUTABLE") or shutil.which("dbt")
    if not dbt:
        bindir = "Scripts" if sys.platform == "win32" else "bin"
        exe = "dbt.exe" if sys.platform == "win32" else "dbt"
        candidate = Path(sys.prefix) / bindir / exe
        if candidate.exists():
            dbt = str(candidate)
    if not dbt:
        raise MissingDependencyError("dbt is not installed (pip install dbt-core dbt-duckdb)")
    profiles = DBT_DIR / "profiles.yml"
    if not profiles.exists():
        shutil.copyfile(DBT_DIR / "profiles.yml.example", profiles)
    env = {**os.environ, "STREAMCOMMERCE_WAREHOUSE_PATH": str(settings.warehouse_path.resolve()),
           "DBT_PROFILES_DIR": str(DBT_DIR)}
    proc = subprocess.run([dbt, "build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
                          cwd=DBT_DIR, env=env, capture_output=True, text=True, check=False)
    tail = "\n".join((proc.stdout + proc.stderr).strip().splitlines()[-25:])
    if proc.returncode != 0:
        raise IngestionError(f"dbt build failed (exit {proc.returncode}):\n{tail}")
    return tail


def build_summary(ctx: RunContext) -> dict:
    """Assemble the execution summary from the metadata store (works across separate Airflow tasks)."""
    run = ctx.repo.get_run(ctx.run_id) or {}
    quality = ctx.repo.list_quality_results(ctx.run_id)
    failed_rules = [q for q in quality if q["status"] == "failed"]
    summary = {
        "run_id": ctx.run_id, "engine": run.get("engine"), "started_at": str(run.get("started_at")),
        "records_processed": run.get("records_processed", 0), "records_rejected": run.get("records_rejected", 0),
        "rules_evaluated": len(quality), "rules_failed": len(failed_rules),
        "failed_rules": [{k: q[k] for k in ("dataset_name", "rule_name", "severity", "failed_records",
                                            "total_records")} for q in failed_rules],
        "errors": ctx.repo.list_errors(ctx.run_id),
        "datasets": {m["dataset_name"]: m["row_count"] for m in ctx.repo.list_dataset_metadata()},
        "step_details": ctx.output,
    }
    ctx.settings.summaries_dir.mkdir(parents=True, exist_ok=True)
    path = ctx.settings.summaries_dir / f"{ctx.run_id}.json"
    path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    summary["summary_file"] = path.as_posix()
    return summary


def run_step(step: str, ctx: RunContext) -> None:
    """Execute one step, recording timing and any failure."""
    started = time.monotonic()
    log.info("step started", extra={"step": step, "run_id": ctx.run_id})
    s, repo = ctx.settings, ctx.repo
    if step == "prepare":
        in_dir = ctx.input_dir or s.incoming_dir
        if ctx.generate is not None:
            ctx.output["generated"] = generate_dataset(in_dir, **ctx.generate)
        elif not any(in_dir.glob("*.csv")):
            raise IngestionError(f"No CSV files in {in_dir}. Use --generate or copy files there.")
    elif step == "ingest":
        files = ingest_directory(s, repo, ctx.run_id, ctx.input_dir, ctx.force)
        ctx.output["ingest"] = [{"dataset": f.dataset, "skipped": f.skipped, "rows": f.record_count} for f in files]
    elif step == "validate":
        ctx.output["schema_warnings"] = validate_pending(repo, ctx.run_id)
    elif step == "process":
        ctx.output["process"] = [r.as_dict() for r in process_pending(s, repo, ctx.run_id, ctx.engine)]
    elif step == "load_warehouse":
        ctx.output["warehouse"] = load_warehouse(s, repo)
    elif step == "dbt":
        ctx.output["dbt"] = run_dbt(s)
    elif step == "final_quality":
        results = run_final_checks(s, repo, ctx.run_id)
        ctx.output["final_quality_failed"] = [r.rule_name for r in results if r.status == "failed"]
    elif step == "summary":
        ctx.output["duration_seconds"] = round(time.monotonic() - started, 3)
        repo.finish_run(ctx.run_id, STATUS_SUCCESS)
        ctx.output["summary_file"] = build_summary(ctx)["summary_file"]
    else:
        raise ConfigError(f"Unknown step {step!r}; valid steps: {STEPS}")
    log.info("step finished", extra={"step": step, "seconds": round(time.monotonic() - started, 3)})


def fail_run(settings: Settings, run_id: str, message: str, stage: str = "pipeline") -> None:
    """Mark a run failed (used by the Airflow failure callback). Safe to call more than once."""
    repo = get_repository(settings)
    repo.record_error(run_id, stage, None, "TaskFailure", message)
    repo.finish_run(run_id, STATUS_FAILED, message[:500])


def run_pipeline(settings: Settings, steps: list[str], run_id: str | None = None, engine: str = "pandas",
                 input_dir: Path | None = None, force: bool = False, generate: dict | None = None) -> dict:
    """Run ``steps`` in order under one run ID and return the execution summary."""
    settings.ensure_dirs()
    repo = get_repository(settings)
    run_id = run_id or f"run-{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    repo.start_run(run_id, "batch", engine)
    ctx = RunContext(settings, repo, run_id, engine, input_dir, force, generate)
    t0 = time.monotonic()
    try:
        for step in steps:
            run_step(step, ctx)
    except Exception as exc:
        repo.record_error(run_id, "pipeline", None, type(exc).__name__, str(exc))
        repo.finish_run(run_id, STATUS_FAILED, f"{type(exc).__name__}: {exc}"[:500])
        raise
    if "summary" not in steps:
        repo.finish_run(run_id, STATUS_SUCCESS)
    ctx.output["duration_seconds"] = round(time.monotonic() - t0, 3)
    return build_summary(ctx)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="StreamCommerce batch pipeline")
    p.add_argument("--steps", default="", help=f"comma-separated subset of {STEPS}")
    p.add_argument("--run-id", default=None)
    p.add_argument("--engine", choices=["pandas", "spark"], default="pandas")
    p.add_argument("--input-dir", type=Path, default=None)
    p.add_argument("--force", action="store_true", help="reprocess files even if the checksum is unchanged")
    p.add_argument("--with-dbt", action="store_true", help="include the dbt step in the default step list")
    p.add_argument("--generate", action="store_true", help="generate synthetic input first")
    p.add_argument("--customers", type=int, default=1000)
    p.add_argument("--products", type=int, default=200)
    p.add_argument("--orders", type=int, default=10000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--faults", action="store_true")
    a = p.parse_args(argv)

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    steps = [s.strip() for s in a.steps.split(",") if s.strip()] or [
        s for s in STEPS if a.with_dbt or s != "dbt"]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        print(f"Unknown steps: {unknown}. Valid: {STEPS}", file=sys.stderr)
        return 2
    generate = None
    if a.generate:
        generate = {"customers": a.customers, "products": a.products, "orders": a.orders,
                    "seed": a.seed, "faults": a.faults}
    try:
        summary = run_pipeline(settings, steps, a.run_id, a.engine, a.input_dir, a.force, generate)
    except StreamCommerceError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
