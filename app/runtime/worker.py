from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from app.config import Settings
from app.foxnose_store import FoxnoseStore
from app.runner import BenchmarkRunner, build_executors
from app.schemas import BenchmarkJobCreateRequest

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


@dataclass(slots=True)
class JobTask:
    job_key: str
    job_payload: dict
    request: BenchmarkJobCreateRequest


class BenchmarkWorker:
    def __init__(
        self,
        *,
        settings: Settings,
        store: FoxnoseStore,
        runner: BenchmarkRunner,
    ) -> None:
        self._settings = settings
        self._store = store
        self._runner = runner
        self._stop_event = threading.Event()
        self._threads: set[threading.Thread] = set()
        self._lock = threading.Lock()

    def start(self) -> None:
        logger.info("Benchmark worker initialized")

    def stop(self, timeout: float = 10.0) -> None:
        self._stop_event.set()
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(timeout=timeout)
        logger.info("Benchmark worker stopped")

    def enqueue(self, task: JobTask) -> None:
        if self._stop_event.is_set():
            logger.error("Worker already stopped; job %s was not scheduled", task.job_key)
            return

        thread = threading.Thread(
            target=self._process_task,
            args=(task,),
            daemon=True,
            name=f"bench-job-{task.job_key}",
        )
        with self._lock:
            self._threads.add(thread)
        logger.info("Benchmark job enqueued: %s", task.job_key)
        thread.start()

    def _process_task(self, task: JobTask) -> None:
        try:
            logger.info("Benchmark job started: %s", task.job_key)
            self._run_task(task)
        except Exception:  # noqa: BLE001
            logger.exception("Unhandled error while processing benchmark job %s", task.job_key)
        finally:
            with self._lock:
                current = threading.current_thread()
                if current in self._threads:
                    self._threads.remove(current)

    def _run_task(self, task: JobTask) -> None:
        payload = dict(task.job_payload)
        logger.info("Job %s: updating status to running", task.job_key)

        payload.update(
            {
                "status": "running",
                "started_at": _now_iso(),
                "current_step": "preparing",
                "progress_percent": 0,
                "eta_seconds": None,
                "error": None,
            }
        )
        self._store.update_job_resource(task.job_key, payload)
        logger.info("Job %s: status updated to running", task.job_key)

        try:
            logger.info("Job %s: building executors", task.job_key)
            executors = build_executors(task.request, self._settings)
            logger.info("Job %s: built %d executors", task.job_key, len(executors))

            def on_progress(
                progress_percent: float, eta_seconds: int | None, current_step: str
            ) -> None:
                payload.update(
                    {
                        "progress_percent": progress_percent,
                        "eta_seconds": eta_seconds,
                        "current_step": current_step,
                    }
                )
                self._store.update_job_resource(task.job_key, payload)

            logger.info("Job %s: running benchmark job", task.job_key)
            run_result = self._runner.run_job(
                job_key=task.job_key,
                request=task.request,
                executors=executors,
                progress=on_progress,
            )
            logger.info("Job %s: benchmark run finished", task.job_key)

            report_payload = dict(run_result.report_payload)
            report_payload["created_at"] = _now_iso()
            logger.info("Job %s: creating report resource", task.job_key)
            report_key = self._store.create_report_resource(report_payload)
            logger.info("Job %s: report resource created (%s)", task.job_key, report_key)

            report_items = []
            for item in run_result.report_items:
                row = dict(item)
                row["report_key"] = report_key
                report_items.append(row)
            logger.info("Job %s: creating %d report items", task.job_key, len(report_items))
            self._store.create_report_items(report_items)
            logger.info("Job %s: report items created", task.job_key)

            payload.update(
                {
                    "status": "completed",
                    "progress_percent": 100,
                    "eta_seconds": 0,
                    "current_step": "completed",
                    "finished_at": _now_iso(),
                    "report_key": report_key,
                    "error": None,
                }
            )
            logger.info("Job %s: updating status to completed", task.job_key)
            self._store.update_job_resource(task.job_key, payload)
            logger.info("Job %s: status updated to completed", task.job_key)
            return

        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            logger.exception("Job %s: failed with error", task.job_key)
            report_key: str | None = None
            try:
                failed_report = self._runner.build_failed_report_payload(
                    job_key=task.job_key,
                    request=task.request,
                    error=error_text,
                )
                failed_report["created_at"] = _now_iso()
                report_key = self._store.create_report_resource(failed_report)
            except Exception:
                report_key = None

            payload.update(
                {
                    "status": "failed",
                    "current_step": "failed",
                    "finished_at": _now_iso(),
                    "error": error_text,
                    "report_key": report_key,
                    "eta_seconds": None,
                }
            )
            self._store.update_job_resource(task.job_key, payload)
