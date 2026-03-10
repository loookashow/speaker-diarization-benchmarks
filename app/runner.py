from __future__ import annotations

import hashlib
import os
import platform
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any, Callable

import soundfile as sf

from app.config import Settings
from app.datasets import DatasetCatalog, DatasetItem, DatasetSpec
from app.metrics import (
    ItemMetrics,
    compute_der,
    load_rttm_annotation,
    speaker_bucket,
    speaker_count,
    summarize,
)
from app.schemas import BenchmarkJobCreateRequest
from app.systems import SystemExecutor, SystemSpec


@dataclass(frozen=True)
class RunResult:
    report_payload: dict
    report_items: list[dict]


def _safe_slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value)


class BenchmarkRunner:
    def __init__(self, datasets: DatasetCatalog, work_dir: Path) -> None:
        self._datasets = datasets
        self._work_dir = work_dir

    def run_job(
        self,
        *,
        job_key: str,
        request: BenchmarkJobCreateRequest,
        executors: list[SystemExecutor],
        progress: Callable[[float, int | None, str], None],
    ) -> RunResult:
        dataset_spec = self._datasets.get(request.dataset_id)
        dataset_items = self._datasets.collect_items(request.dataset_id, request.limit_files)
        cpu_threads = self._resolve_cpu_threads(request)
        n_runs = request.n_runs
        warmup_count = min(request.warmup_files, len(dataset_items))
        warmup_items = dataset_items[:warmup_count]
        bench_items = dataset_items[warmup_count:]

        if not bench_items:
            raise RuntimeError(
                "No benchmark files left after warmup. "
                "Decrease warmup_files or increase dataset size."
            )

        total_steps = len(executors) * (len(warmup_items) + (len(bench_items) * n_runs))
        completed_steps = 0
        started_at = time.perf_counter()

        metrics: list[ItemMetrics] = []
        run_dir = self._work_dir / job_key
        run_dir.mkdir(parents=True, exist_ok=True)

        for executor in executors:
            executor.prepare()

            for item in warmup_items:
                step = f"warmup {executor.system_ref} on {item.audio_id}"
                output_rttm = self._prediction_path(run_dir, executor, item.audio_id, warmup=True)
                executor.run(
                    audio_path=item.audio_path,
                    output_rttm=output_rttm,
                    work_dir=run_dir,
                    reference_rttm=item.reference_rttm_path,
                )
                completed_steps += 1
                progress(*self._progress_values(started_at, completed_steps, total_steps, step))

            for run_index in range(1, n_runs + 1):
                for item in bench_items:
                    step = (
                        f"bench run {run_index}/{n_runs} {executor.system_ref} on {item.audio_id}"
                    )
                    output_rttm = self._prediction_path(
                        run_dir,
                        executor,
                        item.audio_id,
                        run_index=run_index,
                    )
                    processing_seconds = executor.run(
                        audio_path=item.audio_path,
                        output_rttm=output_rttm,
                        work_dir=run_dir,
                        reference_rttm=item.reference_rttm_path,
                    )

                    audio_seconds = self._audio_duration_seconds(item.audio_path)
                    rtf = processing_seconds / max(audio_seconds, 1e-9)

                    der = compute_der(item.reference_rttm_path, output_rttm)
                    ref_annotation = load_rttm_annotation(item.reference_rttm_path)
                    pred_annotation = load_rttm_annotation(output_rttm)

                    gt_speakers = speaker_count(ref_annotation)
                    pred_speakers = speaker_count(pred_annotation)
                    abs_count_error = abs(gt_speakers - pred_speakers)
                    bucket = speaker_bucket(gt_speakers) if gt_speakers > 0 else None

                    metrics.append(
                        ItemMetrics(
                            system_id=executor.spec.system_id,
                            system_version=executor.spec.version,
                            audio_id=item.audio_id,
                            run_index=run_index,
                            audio_seconds=float(audio_seconds),
                            processing_seconds=float(processing_seconds),
                            rtf=float(rtf),
                            der=float(der),
                            gt_speakers=gt_speakers,
                            pred_speakers=pred_speakers,
                            abs_count_error=abs_count_error,
                            bucket=bucket,
                            extra={
                                "audio_path": str(item.audio_path),
                                "reference_rttm": str(item.reference_rttm_path),
                                "predicted_rttm": str(output_rttm),
                            },
                        )
                    )

                    completed_steps += 1
                    progress(*self._progress_values(started_at, completed_steps, total_steps, step))

        systems = [
            {
                "system_id": executor.spec.system_id,
                "version": executor.spec.version,
                "params": executor.spec.params,
            }
            for executor in executors
        ]
        metrics_summary, speaker_table, der_summary, rtf_summary = summarize(
            metrics,
            systems,
            n_runs=n_runs,
        )
        dataset_manifest = self._dataset_manifest(
            spec=dataset_spec,
            items=dataset_items,
            warmup_count=warmup_count,
            limit_files=request.limit_files,
            n_runs=n_runs,
            metadata=request.metadata,
        )
        machine_fingerprint = self._machine_fingerprint(cpu_threads=cpu_threads)

        report_payload = {
            "job_key": job_key,
            "status": "completed",
            "dataset_id": request.dataset_id,
            "systems": {"systems": systems},
            "metrics_summary": metrics_summary,
            "speaker_count_table": {"rows": speaker_table},
            "der_summary": der_summary,
            "rtf_summary": rtf_summary,
            "run_config": {
                "n_runs": n_runs,
                "cpu_threads": cpu_threads,
                "warmup_files": warmup_count,
                "bench_files": len(bench_items),
                "limit_files": request.limit_files,
            },
            "dataset_manifest": dataset_manifest,
            "machine_fingerprint": machine_fingerprint,
            "items_count": len(metrics),
            "artifact_paths": {
                "run_dir": str(run_dir),
            },
            "error": None,
        }

        report_items = [
            {
                "job_key": job_key,
                "dataset_id": request.dataset_id,
                "system_id": item.system_id,
                "system_version": item.system_version,
                "audio_id": item.audio_id,
                "run_index": item.run_index,
                "audio_seconds": item.audio_seconds,
                "processing_seconds": item.processing_seconds,
                "rtf": item.rtf,
                "der": item.der,
                "gt_speakers": item.gt_speakers,
                "pred_speakers": item.pred_speakers,
                "abs_count_error": item.abs_count_error,
                "bucket": item.bucket,
                "extra": item.extra,
            }
            for item in metrics
        ]

        return RunResult(report_payload=report_payload, report_items=report_items)

    @staticmethod
    def _audio_duration_seconds(path: Path) -> float:
        info = sf.info(str(path))
        return float(info.duration)

    @staticmethod
    def _prediction_path(
        run_dir: Path,
        executor: SystemExecutor,
        audio_id: str,
        *,
        run_index: int | None = None,
        warmup: bool = False,
    ) -> Path:
        system = _safe_slug(executor.spec.system_id)
        version = _safe_slug(executor.spec.version)
        root = run_dir / "predictions" / f"{system}__{version}"
        if warmup:
            return root / "warmup" / f"{audio_id}.rttm"
        if run_index is not None:
            return root / f"run_{run_index:03d}" / f"{audio_id}.rttm"
        return root / f"{audio_id}.rttm"

    @staticmethod
    def _progress_values(
        started_at: float,
        completed_steps: int,
        total_steps: int,
        current_step: str,
    ) -> tuple[float, int | None, str]:
        percent = (completed_steps / total_steps) * 100 if total_steps > 0 else 100.0
        elapsed = time.perf_counter() - started_at
        if completed_steps <= 0:
            eta = None
        else:
            remaining_steps = max(total_steps - completed_steps, 0)
            eta = int((elapsed / completed_steps) * remaining_steps)
        return round(percent, 2), eta, current_step

    @staticmethod
    def _resolve_cpu_threads(request: BenchmarkJobCreateRequest) -> int:
        return int(request.cpu_threads or (os.cpu_count() or 1))

    @staticmethod
    def _safe_relative(path: Path, root: Path) -> str:
        try:
            return str(path.resolve().relative_to(root.resolve()))
        except Exception:
            return str(path)

    @staticmethod
    def _dataset_manifest(
        *,
        spec: DatasetSpec,
        items: list[DatasetItem],
        warmup_count: int,
        limit_files: int | None,
        n_runs: int,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        hasher = hashlib.sha256()
        total_audio_bytes = 0
        total_rttm_bytes = 0

        for item in items:
            audio_stat = item.audio_path.stat()
            ref_stat = item.reference_rttm_path.stat()
            total_audio_bytes += audio_stat.st_size
            total_rttm_bytes += ref_stat.st_size

            row = (
                f"{item.audio_id}|"
                f"{BenchmarkRunner._safe_relative(item.audio_path, spec.root)}|"
                f"{audio_stat.st_size}|{audio_stat.st_mtime_ns}|"
                f"{BenchmarkRunner._safe_relative(item.reference_rttm_path, spec.root)}|"
                f"{ref_stat.st_size}|{ref_stat.st_mtime_ns}"
            )
            hasher.update(row.encode("utf-8"))

        bench_items = max(len(items) - warmup_count, 0)
        return {
            "dataset_id": spec.dataset_id,
            "root": str(spec.root),
            "audio_dir": spec.audio_dir,
            "reference_rttm_dir": spec.reference_rttm_dir,
            "audio_glob": spec.audio_glob,
            "selection_count": len(items),
            "warmup_count": warmup_count,
            "bench_count": bench_items,
            "n_runs": n_runs,
            "effective_scored_items": bench_items * n_runs,
            "limit_files": limit_files,
            "dataset_version": metadata.get("dataset_version"),
            "selection_hash_sha256": hasher.hexdigest(),
            "hash_strategy": (
                "sha256(audio_id, relative_paths, size_bytes, mtime_ns for audio + reference RTTM)"
            ),
            "total_audio_bytes": total_audio_bytes,
            "total_reference_rttm_bytes": total_rttm_bytes,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }

    @staticmethod
    def _machine_fingerprint(*, cpu_threads: int) -> dict[str, Any]:
        pinned_env = {
            key: str(cpu_threads)
            for key in (
                "OMP_NUM_THREADS",
                "MKL_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "NUMEXPR_NUM_THREADS",
                "VECLIB_MAXIMUM_THREADS",
                "BLIS_NUM_THREADS",
                "TORCH_NUM_THREADS",
            )
        }
        pinned_env["TOKENIZERS_PARALLELISM"] = "false"

        return {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python_version": platform.python_version(),
            "cpu_model": BenchmarkRunner._cpu_model(),
            "logical_cpus": os.cpu_count(),
            "memory_total_bytes": BenchmarkRunner._memory_total_bytes(),
            "thread_pinning": {
                "cpu_threads": cpu_threads,
                "env": pinned_env,
            },
            "package_versions": BenchmarkRunner._package_versions(
                [
                    "diarize",
                    "torch",
                    "torchaudio",
                    "pyannote.audio",
                    "pyannote.metrics",
                    "numpy",
                    "soundfile",
                    "foxnose-sdk",
                ]
            ),
        }

    @staticmethod
    def _cpu_model() -> str | None:
        cpuinfo = Path("/proc/cpuinfo")
        if cpuinfo.exists():
            try:
                for line in cpuinfo.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if line.lower().startswith("model name"):
                        parts = line.split(":", 1)
                        if len(parts) == 2:
                            value = parts[1].strip()
                            if value:
                                return value
            except Exception:
                pass

        value = platform.processor()
        return value or None

    @staticmethod
    def _memory_total_bytes() -> int | None:
        if not hasattr(os, "sysconf"):
            return None
        try:
            pages = int(os.sysconf("SC_PHYS_PAGES"))
            page_size = int(os.sysconf("SC_PAGE_SIZE"))
        except (ValueError, OSError, TypeError):
            return None

        if pages <= 0 or page_size <= 0:
            return None
        return pages * page_size

    @staticmethod
    def _package_versions(names: list[str]) -> dict[str, str | None]:
        versions: dict[str, str | None] = {}
        for name in names:
            try:
                versions[name] = importlib_metadata.version(name)
            except importlib_metadata.PackageNotFoundError:
                versions[name] = None
        return versions

    @staticmethod
    def build_failed_report_payload(
        *,
        job_key: str,
        request: BenchmarkJobCreateRequest,
        error: str,
    ) -> dict:
        cpu_threads = BenchmarkRunner._resolve_cpu_threads(request)
        return {
            "job_key": job_key,
            "status": "failed",
            "dataset_id": request.dataset_id,
            "systems": {"systems": [system.model_dump() for system in request.systems]},
            "metrics_summary": {},
            "speaker_count_table": {"rows": []},
            "der_summary": {},
            "rtf_summary": {},
            "run_config": {
                "n_runs": request.n_runs,
                "cpu_threads": cpu_threads,
                "warmup_files": request.warmup_files,
                "bench_files": 0,
                "limit_files": request.limit_files,
            },
            "dataset_manifest": None,
            "machine_fingerprint": BenchmarkRunner._machine_fingerprint(cpu_threads=cpu_threads),
            "items_count": 0,
            "artifact_paths": None,
            "error": error,
        }


def build_executors(request: BenchmarkJobCreateRequest, settings: Settings) -> list[SystemExecutor]:
    executors: list[SystemExecutor] = []
    cpu_threads = int(request.cpu_threads or (os.cpu_count() or 1))
    for system in request.systems:
        spec = SystemSpec(
            system_id=system.system_id,
            version=system.version,
            params=system.params,
        )
        executors.append(SystemExecutor(spec, settings, cpu_threads=cpu_threads))
    return executors
