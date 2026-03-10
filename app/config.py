from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _split_csv(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class Settings:
    api_keys: tuple[str, ...]
    api_key_header: str
    reports_dir: Path
    work_dir: Path
    venv_root: Path
    datasets_root: Path
    datasets_file: Path | None
    python_bin: str

    foxnose_base_url: str
    foxnose_environment_key: str
    foxnose_auth_mode: str
    foxnose_access_token: str | None
    foxnose_public_key: str | None
    foxnose_secret_key: str | None
    foxnose_private_key: str | None
    foxnose_jobs_folder: str
    foxnose_reports_folder: str
    foxnose_report_items_folder: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    raw_keys = os.getenv("BENCH_API_KEYS", "")
    settings = Settings(
        api_keys=tuple(_split_csv(raw_keys)),
        api_key_header=os.getenv("BENCH_API_KEY_HEADER", "X-API-Key"),
        reports_dir=Path(os.getenv("BENCH_REPORTS_DIR", "./data/reports")).expanduser(),
        work_dir=Path(os.getenv("BENCH_WORK_DIR", "./data/work")).expanduser(),
        venv_root=Path(os.getenv("BENCH_VENV_ROOT", "./data/venvs")).expanduser(),
        datasets_root=Path(os.getenv("BENCH_DATASETS_ROOT", "/data/datasets")).expanduser(),
        datasets_file=(
            Path(os.environ["BENCH_DATASETS_FILE"]).expanduser()
            if os.getenv("BENCH_DATASETS_FILE")
            else None
        ),
        python_bin=os.getenv("BENCH_PYTHON_BIN", sys.executable),
        foxnose_base_url=os.getenv("FOXNOSE_BASE_URL", "https://api.foxnose.net"),
        foxnose_environment_key=os.getenv("FOXNOSE_ENV_KEY", ""),
        foxnose_auth_mode=os.getenv("FOXNOSE_AUTH_MODE", "secure"),
        foxnose_access_token=os.getenv("FOXNOSE_ACCESS_TOKEN"),
        foxnose_public_key=os.getenv("FOXNOSE_PUBLIC_KEY"),
        foxnose_secret_key=os.getenv("FOXNOSE_SECRET_KEY"),
        foxnose_private_key=os.getenv("FOXNOSE_PRIVATE_KEY"),
        foxnose_jobs_folder=os.getenv("FOXNOSE_BENCH_JOBS_FOLDER", "benchmark_jobs"),
        foxnose_reports_folder=os.getenv("FOXNOSE_BENCH_REPORTS_FOLDER", "benchmark_reports"),
        foxnose_report_items_folder=os.getenv(
            "FOXNOSE_BENCH_REPORT_ITEMS_FOLDER",
            "benchmark_report_items",
        ),
    )

    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    settings.work_dir.mkdir(parents=True, exist_ok=True)
    settings.venv_root.mkdir(parents=True, exist_ok=True)

    return settings
