from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query, status
from foxnose_sdk.errors import FoxnoseAPIError

from app.auth import require_api_key
from app.config import get_settings
from app.datasets import DatasetCatalog
from app.foxnose_store import FoxnoseStore
from app.runner import BenchmarkRunner
from app.runtime import BenchmarkWorker, JobTask
from app.schemas import (
    BenchmarkJobCreateRequest,
    BenchmarkJobCreateResponse,
    BenchmarkJobStatusResponse,
    DatasetInfo,
    JobReportResponse,
    ReportListResponse,
    ReportSummary,
    SystemRequest,
)


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _to_systems(raw: Any) -> list[SystemRequest]:
    if isinstance(raw, dict) and isinstance(raw.get("systems"), list):
        raw = raw["systems"]
    if not isinstance(raw, list):
        return []
    out: list[SystemRequest] = []
    for item in raw:
        try:
            out.append(SystemRequest.model_validate(item))
        except Exception:
            continue
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    datasets = DatasetCatalog.from_settings(settings)
    store = FoxnoseStore(settings)
    store.ensure_ready()

    runner = BenchmarkRunner(datasets=datasets, work_dir=settings.work_dir)
    worker = BenchmarkWorker(settings=settings, store=store, runner=runner)
    worker.start()

    app.state.settings = settings
    app.state.datasets = datasets
    app.state.store = store
    app.state.worker = worker
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(title="Diarization Benchmarks Service", version="0.1.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/datasets", response_model=list[DatasetInfo])
def list_datasets(_: str = Depends(require_api_key)) -> list[DatasetInfo]:
    datasets: DatasetCatalog = app.state.datasets
    return datasets.list_infos()


@app.post(
    "/v1/jobs", response_model=BenchmarkJobCreateResponse, status_code=status.HTTP_202_ACCEPTED
)
def create_job(
    request: BenchmarkJobCreateRequest,
    _: str = Depends(require_api_key),
) -> BenchmarkJobCreateResponse:
    datasets: DatasetCatalog = app.state.datasets
    store: FoxnoseStore = app.state.store
    worker: BenchmarkWorker = app.state.worker

    try:
        datasets.get(request.dataset_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    request_payload = request.model_dump(mode="json")
    job_payload = store.build_new_job_payload(request_payload)

    try:
        job_key = store.create_job_resource(job_payload)
    except FoxnoseAPIError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to create job resource in FoxNose: {exc.message}",
        ) from exc

    worker.enqueue(JobTask(job_key=job_key, job_payload=job_payload, request=request))
    return BenchmarkJobCreateResponse(job_id=job_key, status="queued")


@app.get("/v1/jobs/{job_id}", response_model=BenchmarkJobStatusResponse)
def get_job_status(job_id: str, _: str = Depends(require_api_key)) -> BenchmarkJobStatusResponse:
    store: FoxnoseStore = app.state.store

    try:
        data = store.get_job_resource(job_id)
    except FoxnoseAPIError as exc:
        if exc.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch job from FoxNose: {exc.message}",
        ) from exc

    request_data = data.get("request") or {}
    systems = _to_systems(request_data.get("systems"))

    created_at = _dt(data.get("submitted_at"))
    if created_at is None:
        raise HTTPException(status_code=500, detail="Job missing submitted_at")

    return BenchmarkJobStatusResponse(
        job_id=job_id,
        status=str(data.get("status", "unknown")),
        progress_percent=float(data.get("progress_percent") or 0),
        eta_seconds=(float(data["eta_seconds"]) if data.get("eta_seconds") is not None else None),
        current_step=data.get("current_step"),
        created_at=created_at,
        started_at=_dt(data.get("started_at")),
        finished_at=_dt(data.get("finished_at")),
        error=data.get("error"),
        dataset_id=str(data.get("dataset_id") or request_data.get("dataset_id") or ""),
        systems=systems,
        report_key=data.get("report_key"),
    )


@app.get("/v1/jobs/{job_id}/report", response_model=JobReportResponse)
def get_job_report(job_id: str, _: str = Depends(require_api_key)) -> JobReportResponse:
    store: FoxnoseStore = app.state.store

    try:
        job_data = store.get_job_resource(job_id)
    except FoxnoseAPIError as exc:
        if exc.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Job not found"
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch job from FoxNose: {exc.message}",
        ) from exc

    report_key = job_data.get("report_key")
    if not report_key:
        if job_data.get("status") in {"queued", "running"}:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Job is not finished yet",
            )
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found")

    try:
        report = store.get_report_resource(str(report_key))
    except FoxnoseAPIError as exc:
        if exc.status_code == 404:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Report not found"
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to fetch report from FoxNose: {exc.message}",
        ) from exc

    return JobReportResponse(job_id=job_id, report_key=str(report_key), report=report)


@app.get("/v1/reports", response_model=ReportListResponse)
def list_reports(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    _: str = Depends(require_api_key),
) -> ReportListResponse:
    store: FoxnoseStore = app.state.store

    try:
        count, rows = store.list_reports(limit=limit, offset=offset)
    except FoxnoseAPIError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to list reports from FoxNose: {exc.message}",
        ) from exc

    summaries: list[ReportSummary] = []
    for row in rows:
        created_at = _dt(row.get("created_at"))
        if created_at is None:
            continue

        summaries.append(
            ReportSummary(
                report_key=str(row["report_key"]),
                job_id=str(row.get("job_key") or ""),
                created_at=created_at,
                status=str(row.get("status") or "unknown"),
                dataset_id=str(row.get("dataset_id") or ""),
                systems=_to_systems(row.get("systems")),
            )
        )

    return ReportListResponse(count=count, results=summaries)
