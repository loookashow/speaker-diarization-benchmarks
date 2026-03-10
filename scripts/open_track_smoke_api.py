#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, parse, request


@dataclass
class SmokeResult:
    dataset_id: str
    job_id: str | None
    final_status: str
    error: str | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit small Open Track benchmark jobs and wait for completion."
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("BENCH_BASE_URL", "http://127.0.0.1:8080"),
        help="Benchmark API base URL.",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("BENCH_API_KEY", ""),
        help="Benchmark API key (or BENCH_API_KEY env).",
    )
    parser.add_argument(
        "--datasets-file",
        default="config/datasets.open_track.json",
        help="Path to Open Track datasets config.",
    )
    parser.add_argument(
        "--dataset-id",
        action="append",
        default=[],
        help="Run smoke only for this dataset_id (repeatable).",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval for job status.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=600,
        help="Timeout for each job.",
    )
    parser.add_argument(
        "--cpu-threads",
        type=int,
        default=1,
        help="cpu_threads for smoke jobs.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=1,
        help="limit_files for smoke jobs.",
    )
    return parser.parse_args()


def http_json(
    *,
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    headers = {"accept": "application/json", "x-api-key": api_key}
    body: bytes | None = None
    if payload is not None:
        headers["content-type"] = "application/json"
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

    req = request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {url}: {detail[:400]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Connection error for {url}: {exc}") from exc


def load_dataset_ids(path: Path) -> list[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("datasets", raw) if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        raise RuntimeError("datasets file must be a list or {'datasets': [...]} format")

    out: list[str] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        dataset_id = str(item.get("dataset_id") or "").strip()
        if dataset_id:
            out.append(dataset_id)
    return out


def create_smoke_job(
    *,
    base_url: str,
    api_key: str,
    dataset_id: str,
    cpu_threads: int,
    limit_files: int,
) -> str:
    payload = {
        "dataset_id": dataset_id,
        "systems": [{"system_id": "oracle_reference", "version": "1.0.0", "params": {}}],
        "limit_files": limit_files,
        "warmup_files": 0,
        "n_runs": 1,
        "cpu_threads": cpu_threads,
        "metadata": {"smoke": True, "profile": "open_track"},
    }
    data = http_json(
        method="POST",
        base_url=base_url,
        path="/v1/jobs",
        api_key=api_key,
        payload=payload,
    )
    job_id = str(data.get("job_id") or "")
    if not job_id:
        raise RuntimeError(f"create job returned invalid response for dataset {dataset_id}: {data}")
    return job_id


def wait_job(
    *,
    base_url: str,
    api_key: str,
    job_id: str,
    timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    start = time.monotonic()
    while True:
        data = http_json(
            method="GET",
            base_url=base_url,
            path=f"/v1/jobs/{job_id}",
            api_key=api_key,
        )
        status = str(data.get("status") or "unknown")
        if status in {"completed", "failed"}:
            return data
        if (time.monotonic() - start) > timeout_seconds:
            raise RuntimeError(f"timeout while waiting job {job_id}")
        time.sleep(poll_interval_seconds)


def print_results(results: list[SmokeResult]) -> None:
    headers = ["dataset_id", "job_id", "final_status", "error"]
    rows = [
        [
            row.dataset_id,
            row.job_id or "-",
            row.final_status,
            row.error or "-",
        ]
        for row in results
    ]

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt(cells: list[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    print(fmt(headers))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(fmt(row))


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("api key is required (--api-key or BENCH_API_KEY env)", file=sys.stderr)
        return 2

    datasets_file = Path(args.datasets_file).expanduser()
    if not datasets_file.exists():
        print(f"datasets file not found: {datasets_file}", file=sys.stderr)
        return 2
    dataset_ids = load_dataset_ids(datasets_file)
    if args.dataset_id:
        requested = [value.strip() for value in args.dataset_id if value.strip()]
        requested_set = set(requested)
        dataset_ids = [dataset_id for dataset_id in dataset_ids if dataset_id in requested_set]
        if not dataset_ids:
            print(
                "no matching dataset_id values were found in datasets file",
                file=sys.stderr,
            )
            return 2
    if not dataset_ids:
        print("no dataset_id entries in datasets file", file=sys.stderr)
        return 2

    results: list[SmokeResult] = []
    for dataset_id in dataset_ids:
        try:
            job_id = create_smoke_job(
                base_url=args.base_url,
                api_key=args.api_key,
                dataset_id=dataset_id,
                cpu_threads=args.cpu_threads,
                limit_files=args.limit_files,
            )
            final = wait_job(
                base_url=args.base_url,
                api_key=args.api_key,
                job_id=job_id,
                timeout_seconds=args.timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
            status = str(final.get("status") or "unknown")
            err = str(final.get("error")) if final.get("error") else None
            results.append(
                SmokeResult(
                    dataset_id=dataset_id,
                    job_id=job_id,
                    final_status=status,
                    error=err,
                )
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                SmokeResult(
                    dataset_id=dataset_id,
                    job_id=None,
                    final_status="error",
                    error=str(exc),
                )
            )

    print_results(results)
    has_failures = any(row.final_status != "completed" for row in results)
    return 1 if has_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
