from __future__ import annotations

import math
import os
import statistics
import tempfile
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_mpl_cache_dir = Path(tempfile.gettempdir()) / "matplotlib"
_mpl_cache_dir.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_mpl_cache_dir))
warnings.filterwarnings(
    "ignore",
    message=".*'uem' was approximated by the union of 'reference' and 'hypothesis' extents.*",
)


@dataclass(slots=True)
class ItemMetrics:
    system_id: str
    system_version: str
    audio_id: str
    run_index: int
    audio_seconds: float
    processing_seconds: float
    rtf: float
    der: float | None
    gt_speakers: int | None
    pred_speakers: int | None
    abs_count_error: int | None
    bucket: str | None
    extra: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(raw: str) -> float:
    value = float(raw)
    if math.isnan(value) or math.isinf(value):
        raise ValueError(f"Invalid numeric value in RTTM: {raw}")
    return value


def load_rttm_annotation(path: Path) -> Any:
    from pyannote.core import Annotation, Segment

    annotation = Annotation(uri=path.stem)
    text = path.read_text(encoding="utf-8")
    for line in text.splitlines():
        row = line.strip()
        if not row or row.startswith("#"):
            continue
        parts = row.split()
        if len(parts) < 8 or parts[0] != "SPEAKER":
            continue

        start = _safe_float(parts[3])
        duration = _safe_float(parts[4])
        speaker = parts[7]
        if duration <= 0:
            continue

        end = start + duration
        annotation[Segment(start, end)] = speaker
    return annotation


def speaker_count(annotation: Any) -> int:
    return len(set(annotation.labels()))


def speaker_bucket(n_speakers: int) -> str:
    if n_speakers <= 0:
        raise ValueError(f"Expected positive speaker count, got {n_speakers}")
    if n_speakers <= 5:
        return str(n_speakers)
    if n_speakers <= 7:
        return "6-7"
    return "8+"


def compute_der(reference_rttm: Path, hypothesis_rttm: Path) -> float:
    from pyannote.metrics.diarization import DiarizationErrorRate

    ref = load_rttm_annotation(reference_rttm)
    hyp = load_rttm_annotation(hypothesis_rttm)
    # Keep this aligned with benchmark conventions used by this service.
    metric = DiarizationErrorRate(collar=0.25, skip_overlap=True)
    return float(metric(ref, hyp))


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    return float(statistics.median(values))


def _std(values: list[float]) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return float(statistics.pstdev(values))


def _percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None

    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])

    pos = ((len(ordered) - 1) * p) / 100.0
    left = int(math.floor(pos))
    right = int(math.ceil(pos))
    if left == right:
        return float(ordered[left])

    left_value = ordered[left]
    right_value = ordered[right]
    ratio = pos - left
    return float(left_value + (right_value - left_value) * ratio)


def _distribution(values: list[float]) -> dict[str, float | int | None]:
    return {
        "count": len(values),
        "mean": _mean(values),
        "median": _median(values),
        "p95": _percentile(values, 95.0),
        "std": _std(values),
        "min": min(values) if values else None,
        "max": max(values) if values else None,
    }


def _weighted_mean(values: list[tuple[float, float]]) -> float | None:
    weighted_sum = 0.0
    total_weight = 0.0
    for value, weight in values:
        weighted_sum += value * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return float(weighted_sum / total_weight)


def summarize(
    items: list[ItemMetrics],
    systems: list[dict[str, Any]],
    *,
    n_runs: int = 1,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    grouped: dict[str, list[ItemMetrics]] = {}
    for item in items:
        system_ref = f"{item.system_id}@{item.system_version}"
        grouped.setdefault(system_ref, []).append(item)

    der_summary: dict[str, Any] = {}
    rtf_summary: dict[str, Any] = {}
    systems_summary: list[dict[str, Any]] = []

    speaker_table: list[dict[str, Any]] = []

    for system in systems:
        system_ref = f"{system['system_id']}@{system['version']}"
        rows = grouped.get(system_ref, [])

        runs: dict[int, list[ItemMetrics]] = {}
        for row in rows:
            runs.setdefault(row.run_index, []).append(row)
        run_indices = sorted(runs)

        primary_rows = runs.get(1, rows)

        ders = [row.der for row in primary_rows if row.der is not None]
        der_weighted = _weighted_mean(
            [(row.der, row.audio_seconds) for row in primary_rows if row.der is not None]
        )
        der_median = _median([value for value in ders if value is not None])

        rtfs = [row.rtf for row in primary_rows]
        rtf_mean = _mean(rtfs)
        rtf_median = _median(rtfs)

        per_run_der_weighted: list[float] = []
        per_run_rtf_mean: list[float] = []
        for run_index in run_indices:
            run_rows = runs[run_index]
            run_der = _weighted_mean(
                [(row.der, row.audio_seconds) for row in run_rows if row.der is not None]
            )
            run_rtf = _mean([row.rtf for row in run_rows])
            if run_der is not None:
                per_run_der_weighted.append(run_der)
            if run_rtf is not None:
                per_run_rtf_mean.append(run_rtf)

        der_run_stats = _distribution(per_run_der_weighted)
        rtf_run_stats = _distribution(per_run_rtf_mean)

        count_errors = [
            row.abs_count_error for row in primary_rows if row.abs_count_error is not None
        ]
        exact = [value for value in count_errors if value == 0]
        within_1 = [value for value in count_errors if value <= 1]

        speaker_exact = (len(exact) / len(count_errors)) if count_errors else None
        speaker_within_1 = (len(within_1) / len(count_errors)) if count_errors else None
        speaker_mae = _mean([float(value) for value in count_errors])

        der_weighted_rollup = (
            float(der_run_stats["mean"]) if der_run_stats["mean"] is not None else der_weighted
        )
        rtf_mean_rollup = (
            float(rtf_run_stats["mean"]) if rtf_run_stats["mean"] is not None else rtf_mean
        )

        systems_summary.append(
            {
                "system": system_ref,
                "runs": n_runs,
                "runs_completed": len(run_indices),
                "files_per_run": len(primary_rows),
                "der_weighted": der_weighted_rollup,
                "der_median": der_median,
                "rtf_mean": rtf_mean_rollup,
                "rtf_median": rtf_median,
                "der_weighted_run_values": per_run_der_weighted,
                "rtf_mean_run_values": per_run_rtf_mean,
                "der_weighted_run_stats": der_run_stats,
                "rtf_mean_run_stats": rtf_run_stats,
                "speaker_count_exact": speaker_exact,
                "speaker_count_within_1": speaker_within_1,
                "speaker_count_mae": speaker_mae,
            }
        )

        der_summary[system_ref] = {
            "runs": n_runs,
            "runs_completed": len(run_indices),
            "files": len(ders),
            "files_per_run": len(ders),
            "weighted": der_weighted_rollup,
            "median": der_median,
            "mean": _mean([float(value) for value in ders]),
            "run_values": per_run_der_weighted,
            "run_stats": der_run_stats,
        }
        rtf_summary[system_ref] = {
            "runs": n_runs,
            "runs_completed": len(run_indices),
            "files": len(primary_rows),
            "files_per_run": len(primary_rows),
            "mean": rtf_mean_rollup,
            "median": rtf_median,
            "min": min(rtfs) if rtfs else None,
            "max": max(rtfs) if rtfs else None,
            "run_values": per_run_rtf_mean,
            "run_stats": rtf_run_stats,
        }

        bucket_groups: dict[str, list[ItemMetrics]] = {}
        for row in primary_rows:
            if row.bucket is None:
                continue
            bucket_groups.setdefault(row.bucket, []).append(row)

        for bucket in ["1", "2", "3", "4", "5", "6-7", "8+"]:
            bucket_rows = bucket_groups.get(bucket, [])
            if not bucket_rows:
                continue
            errors = [x.abs_count_error for x in bucket_rows if x.abs_count_error is not None]
            exact_count = len([x for x in errors if x == 0])
            within_1_count = len([x for x in errors if x <= 1])

            speaker_table.append(
                {
                    "system": system_ref,
                    "bucket": bucket,
                    "files": len(bucket_rows),
                    "exact_rate": (exact_count / len(errors)) if errors else None,
                    "within_1_rate": (within_1_count / len(errors)) if errors else None,
                    "mae": _mean([float(x) for x in errors]) if errors else None,
                }
            )

    winner_by_der = None
    winner_by_rtf = None

    candidates_der = [s for s in systems_summary if s["der_weighted"] is not None]
    if candidates_der:
        winner_by_der = min(candidates_der, key=lambda row: row["der_weighted"])["system"]

    candidates_rtf = [s for s in systems_summary if s["rtf_mean"] is not None]
    if candidates_rtf:
        winner_by_rtf = min(candidates_rtf, key=lambda row: row["rtf_mean"])["system"]

    metrics_summary = {
        "n_runs": n_runs,
        "winner_by_der": winner_by_der,
        "winner_by_rtf": winner_by_rtf,
        "systems": systems_summary,
    }

    return metrics_summary, speaker_table, der_summary, rtf_summary
