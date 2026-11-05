"""Final quality stage: cross-dataset consistency rules over the processed layer."""
from __future__ import annotations

from src.common.config import Settings
from src.common.schemas import DATASET_ORDER
from src.metadata.models import QualityResult, utc_now
from src.metadata.repository import MetadataRepository
from src.processing.storage import read_table
from src.quality.quality_rules import load_rules
from src.quality.quality_runner import run_rules


def run_final_checks(settings: Settings, repo: MetadataRepository, run_id: str) -> list[QualityResult]:
    """Evaluate 'final' stage rules plus a non-empty check per dataset; record all results."""
    rules = load_rules(settings.quality_rules_path or None)
    frames = {name: read_table(settings, name) for name in DATASET_ORDER}
    results: list[QualityResult] = []
    for name, df in frames.items():
        if df is None:
            results.append(QualityResult(name, f"{name}_processed_exists", "error", 0, 1, "failed",
                                         "processed dataset not found", utc_now()))
            continue
        results.append(QualityResult(name, f"{name}_not_empty", "error", len(df), int(df.empty),
                                     "failed" if df.empty else "passed", "dataset has no rows" if df.empty else "",
                                     utc_now()))
        _, _, rule_results = run_rules(name, df, rules.get(name, []), frames, stage="final")
        results.extend(rule_results)
    repo.record_quality_results(run_id, results)
    return results
