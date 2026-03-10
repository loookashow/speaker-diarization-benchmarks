from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SystemRequest(BaseModel):
    system_id: str = Field(..., description="System id, e.g. diarize or pyannote_community_1")
    version: str = Field(..., description="Package version to benchmark")
    params: dict[str, Any] = Field(default_factory=dict)


class BenchmarkJobCreateRequest(BaseModel):
    dataset_id: str
    systems: list[SystemRequest] = Field(min_length=1)
    limit_files: int | None = Field(default=None, ge=1)
    warmup_files: int = Field(default=1, ge=0)
    n_runs: int = Field(default=1, ge=1, le=20)
    cpu_threads: int | None = Field(default=None, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BenchmarkJobCreateResponse(BaseModel):
    job_id: str
    status: str


class BenchmarkJobStatusResponse(BaseModel):
    job_id: str
    status: str
    progress_percent: float
    eta_seconds: float | None
    current_step: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error: str | None
    dataset_id: str
    systems: list[SystemRequest]
    report_key: str | None = None


class ReportSummary(BaseModel):
    report_key: str
    job_id: str
    created_at: datetime
    status: str
    dataset_id: str
    systems: list[SystemRequest]


class DatasetInfo(BaseModel):
    dataset_id: str
    root: str
    audio_dir: str
    reference_rttm_dir: str
    audio_glob: str


class ReportListResponse(BaseModel):
    count: int
    results: list[ReportSummary]


class JobReportResponse(BaseModel):
    job_id: str
    report_key: str
    report: dict[str, Any]
