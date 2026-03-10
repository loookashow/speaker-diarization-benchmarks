#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

SCHEMA_VERSION = "v1"
DEFAULT_TIMEOUT_SECONDS = 60

CSV_FIELDS = [
    "job_id",
    "report_key",
    "status",
    "dataset_id",
    "system_ref",
    "system_id",
    "system_version",
    "der_weighted",
    "der_median",
    "rtf_mean",
    "rtf_median",
    "speaker_count_exact",
    "speaker_count_within_1",
    "speaker_count_mae",
    "items_count",
    "job_created_at",
    "job_started_at",
    "job_finished_at",
    "run_published_at_utc",
    "source_base_url",
    "run_file",
]


class HttpJsonError(RuntimeError):
    def __init__(self, status_code: int, url: str, body: str) -> None:
        super().__init__(f"HTTP {status_code} for {url}: {body[:500]}")
        self.status_code = status_code
        self.url = url
        self.body = body


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_utc(raw: Any) -> datetime | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        value = datetime.fromisoformat(text)
    except ValueError:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def sort_key_for_dt(raw: Any) -> tuple[int, str]:
    dt = parse_utc(raw)
    if dt is None:
        return (0, "")
    return (1, dt.isoformat())


def json_request(
    *,
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    payload: dict[str, Any] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    url = parse.urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    headers = {
        "accept": "application/json",
        "x-api-key": api_key,
    }
    body: bytes | None = None
    if payload is not None:
        headers["content-type"] = "application/json"
        body = json.dumps(payload, ensure_ascii=True).encode("utf-8")

    req = request.Request(url=url, data=body, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8")
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise HttpJsonError(exc.code, url, error_body) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to connect to {url}: {exc}") from exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON response from {url}: {raw[:500]}") from exc


def get_git_sha() -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None
    return out or None


def normalize_system_ref(system_id: str, system_version: str) -> str:
    return f"{system_id}@{system_version}"


def split_system_ref(system_ref: str) -> tuple[str, str]:
    if "@" not in system_ref:
        return system_ref, ""
    system_id, version = system_ref.rsplit("@", 1)
    return system_id, version


def fetch_job_status(base_url: str, api_key: str, job_id: str) -> dict[str, Any]:
    return json_request(
        method="GET",
        base_url=base_url,
        path=f"/v1/jobs/{job_id}",
        api_key=api_key,
    )


def fetch_job_report(base_url: str, api_key: str, job_id: str) -> dict[str, Any]:
    return json_request(
        method="GET",
        base_url=base_url,
        path=f"/v1/jobs/{job_id}/report",
        api_key=api_key,
    )


def collect_run_from_api(
    *,
    base_url: str,
    api_key: str,
    job_id: str,
    allow_failed: bool,
) -> dict[str, Any]:
    job_status = fetch_job_status(base_url, api_key, job_id)
    status = str(job_status.get("status", "unknown"))
    if status in {"queued", "running"}:
        raise RuntimeError(
            f"Job {job_id} is not finished yet (status={status}). "
            "Wait for completion and run publish again."
        )
    if status == "failed" and not allow_failed:
        raise RuntimeError(
            f"Job {job_id} failed. Re-run with --allow-failed to publish failed runs too."
        )

    report: dict[str, Any] | None = None
    try:
        report = fetch_job_report(base_url, api_key, job_id)
    except HttpJsonError as exc:
        if status == "failed" and exc.status_code in {404, 409}:
            report = None
        else:
            raise

    return {
        "schema_version": SCHEMA_VERSION,
        "published_at_utc": utc_now_iso(),
        "source": {
            "base_url": base_url,
            "generator": "scripts/publish_benchmarks.py",
            "git_sha": get_git_sha(),
        },
        "job": job_status,
        "report_envelope": report,
    }


def scrub_run_record(
    payload: dict[str, Any],
    *,
    redact_internal_fields: bool,
) -> dict[str, Any]:
    if not redact_internal_fields:
        return payload

    # Deep-copy via json to keep payload JSON-serializable and avoid in-place mutation surprises.
    cleaned = json.loads(json.dumps(payload))

    source = cleaned.get("source")
    if isinstance(source, dict):
        source.pop("base_url", None)

    report_envelope = cleaned.get("report_envelope")
    if isinstance(report_envelope, dict):
        report_payload = report_envelope.get("report")
        if isinstance(report_payload, dict):
            artifact_paths = report_payload.get("artifact_paths")
            if isinstance(artifact_paths, dict):
                artifact_paths.pop("run_dir", None)
                if not artifact_paths:
                    report_payload["artifact_paths"] = None

            machine_fingerprint = report_payload.get("machine_fingerprint")
            if isinstance(machine_fingerprint, dict):
                machine_fingerprint.pop("hostname", None)

    return cleaned


def extract_job_systems(job: dict[str, Any]) -> list[dict[str, Any]]:
    systems = job.get("systems")
    if isinstance(systems, list):
        return [item for item in systems if isinstance(item, dict)]
    return []


def extract_metrics_system_rows(report_payload: dict[str, Any]) -> list[dict[str, Any]]:
    metrics_summary = report_payload.get("metrics_summary")
    if not isinstance(metrics_summary, dict):
        return []
    systems = metrics_summary.get("systems")
    if not isinstance(systems, list):
        return []
    return [row for row in systems if isinstance(row, dict)]


def save_run_record(output_dir: Path, run_record: dict[str, Any]) -> Path:
    job = run_record.get("job") if isinstance(run_record.get("job"), dict) else {}
    job_id = str(job.get("job_id") or "")
    if not job_id:
        raise RuntimeError("Run record has no job.job_id")

    created = parse_utc(job.get("created_at")) or datetime.now(timezone.utc)
    date_dir = created.strftime("%Y-%m-%d")
    out_dir = output_dir / "runs" / date_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{job_id}.json"
    write_json_if_changed(out_path, run_record)
    return out_path


def write_json_if_changed(path: Path, payload: dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_text_if_changed(path: Path, text: str) -> None:
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def load_all_runs(output_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    runs_root = output_dir / "runs"
    if not runs_root.exists():
        return []

    loaded: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(runs_root.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        loaded.append((path, payload))
    return loaded


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rows_from_run(path: Path, payload: dict[str, Any], output_dir: Path) -> list[dict[str, Any]]:
    job = payload.get("job")
    if not isinstance(job, dict):
        return []

    job_id = str(job.get("job_id") or "")
    if not job_id:
        return []

    report_envelope = payload.get("report_envelope")
    report_payload: dict[str, Any] | None = None
    if isinstance(report_envelope, dict):
        maybe_report = report_envelope.get("report")
        if isinstance(maybe_report, dict):
            report_payload = maybe_report

    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    source_base_url = str(source.get("base_url") or "")
    run_published_at = str(payload.get("published_at_utc") or "")

    dataset_id = str(job.get("dataset_id") or "")
    status = str(job.get("status") or "")
    report_key = str(job.get("report_key") or "")
    items_count = (
        int(report_payload.get("items_count")) if isinstance(report_payload, dict) else None
    )

    rel_path = path.relative_to(output_dir).as_posix()
    job_created_at = str(job.get("created_at") or "")
    job_started_at = str(job.get("started_at") or "")
    job_finished_at = str(job.get("finished_at") or "")

    metric_rows = extract_metrics_system_rows(report_payload or {})
    out: list[dict[str, Any]] = []

    if metric_rows:
        for metric in metric_rows:
            system_ref = str(metric.get("system") or "")
            system_id, system_version = split_system_ref(system_ref)
            out.append(
                {
                    "job_id": job_id,
                    "report_key": report_key,
                    "status": status,
                    "dataset_id": dataset_id,
                    "system_ref": system_ref,
                    "system_id": system_id,
                    "system_version": system_version,
                    "der_weighted": safe_float(metric.get("der_weighted")),
                    "der_median": safe_float(metric.get("der_median")),
                    "rtf_mean": safe_float(metric.get("rtf_mean")),
                    "rtf_median": safe_float(metric.get("rtf_median")),
                    "speaker_count_exact": safe_float(metric.get("speaker_count_exact")),
                    "speaker_count_within_1": safe_float(metric.get("speaker_count_within_1")),
                    "speaker_count_mae": safe_float(metric.get("speaker_count_mae")),
                    "items_count": items_count,
                    "job_created_at": job_created_at,
                    "job_started_at": job_started_at,
                    "job_finished_at": job_finished_at,
                    "run_published_at_utc": run_published_at,
                    "source_base_url": source_base_url,
                    "run_file": rel_path,
                }
            )
        return out

    for system in extract_job_systems(job):
        system_id = str(system.get("system_id") or "")
        system_version = str(system.get("version") or "")
        if not system_id:
            continue
        system_ref = normalize_system_ref(system_id, system_version)
        out.append(
            {
                "job_id": job_id,
                "report_key": report_key,
                "status": status,
                "dataset_id": dataset_id,
                "system_ref": system_ref,
                "system_id": system_id,
                "system_version": system_version,
                "der_weighted": None,
                "der_median": None,
                "rtf_mean": None,
                "rtf_median": None,
                "speaker_count_exact": None,
                "speaker_count_within_1": None,
                "speaker_count_mae": None,
                "items_count": items_count,
                "job_created_at": job_created_at,
                "job_started_at": job_started_at,
                "job_finished_at": job_finished_at,
                "run_published_at_utc": run_published_at,
                "source_base_url": source_base_url,
                "run_file": rel_path,
            }
        )
    return out


def format_cell(value: Any, digits: int = 6) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def build_latest_snapshot(
    runs: list[tuple[Path, dict[str, Any]]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    by_job: dict[str, dict[str, Any]] = {}
    for _path, run in runs:
        job = run.get("job")
        if not isinstance(job, dict):
            continue
        job_id = str(job.get("job_id") or "")
        if not job_id:
            continue
        if job_id in by_job:
            continue
        systems = extract_job_systems(job)
        by_job[job_id] = {
            "job_id": job_id,
            "dataset_id": job.get("dataset_id"),
            "status": job.get("status"),
            "created_at": job.get("created_at"),
            "started_at": job.get("started_at"),
            "finished_at": job.get("finished_at"),
            "report_key": job.get("report_key"),
            "systems": [
                normalize_system_ref(str(s.get("system_id") or ""), str(s.get("version") or ""))
                for s in systems
                if s.get("system_id")
            ],
        }

    latest_runs = sorted(
        by_job.values(),
        key=lambda x: sort_key_for_dt(x.get("created_at")),
        reverse=True,
    )

    completed_rows = [row for row in rows if row.get("status") == "completed"]
    completed_rows = sorted(
        completed_rows,
        key=lambda x: sort_key_for_dt(x.get("job_created_at")),
        reverse=True,
    )

    latest_per_system_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in completed_rows:
        key = (str(row.get("dataset_id") or ""), str(row.get("system_ref") or ""))
        if key not in latest_per_system_key:
            latest_per_system_key[key] = row
    latest_per_system = list(latest_per_system_key.values())

    leaderboards: dict[str, dict[str, list[dict[str, Any]]]] = {}
    by_dataset: dict[str, list[dict[str, Any]]] = {}
    for row in latest_per_system:
        dataset_id = str(row.get("dataset_id") or "")
        by_dataset.setdefault(dataset_id, []).append(row)

    for dataset_id, dataset_rows in by_dataset.items():
        by_der = sorted(
            dataset_rows,
            key=lambda r: (
                r.get("der_weighted") is None,
                float(r.get("der_weighted") or 0.0),
                float(r.get("rtf_mean") or 0.0),
            ),
        )
        by_rtf = sorted(
            dataset_rows,
            key=lambda r: (
                r.get("rtf_mean") is None,
                float(r.get("rtf_mean") or 0.0),
                float(r.get("der_weighted") or 0.0),
            ),
        )
        leaderboards[dataset_id] = {
            "by_der": by_der,
            "by_rtf": by_rtf,
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": utc_now_iso(),
        "runs_count": len(runs),
        "rows_count": len(rows),
        "latest_runs": latest_runs[:50],
        "latest_per_system": latest_per_system,
        "leaderboards": leaderboards,
    }


def render_markdown(snapshot: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Benchmark Results")
    lines.append("")
    lines.append(f"- Generated (UTC): `{snapshot.get('generated_at_utc')}`")
    lines.append(f"- Runs indexed: `{snapshot.get('runs_count')}`")
    lines.append(f"- Rows indexed: `{snapshot.get('rows_count')}`")
    lines.append("")

    lines.append("## Latest Runs")
    lines.append("")
    lines.append("| Created (UTC) | Job | Status | Dataset | Systems | Report |")
    lines.append("|---|---|---|---|---|---|")
    latest_runs = snapshot.get("latest_runs")
    if isinstance(latest_runs, list) and latest_runs:
        for run in latest_runs:
            if not isinstance(run, dict):
                continue
            systems = run.get("systems")
            systems_text = ", ".join(systems) if isinstance(systems, list) else "-"
            lines.append(
                "| "
                f"{run.get('created_at') or '-'} | "
                f"`{run.get('job_id') or '-'}` | "
                f"{run.get('status') or '-'} | "
                f"{run.get('dataset_id') or '-'} | "
                f"{systems_text or '-'} | "
                f"`{run.get('report_key') or '-'}` |"
            )
    else:
        lines.append("| - | - | - | - | - | - |")

    lines.append("")
    lines.append("## Latest Per System (Completed Runs)")
    lines.append("")

    leaderboards = snapshot.get("leaderboards")
    if not isinstance(leaderboards, dict) or not leaderboards:
        lines.append("No completed benchmark runs indexed yet.")
    else:
        for dataset_id in sorted(leaderboards.keys()):
            board = leaderboards.get(dataset_id)
            if not isinstance(board, dict):
                continue
            rows = board.get("by_der")
            if not isinstance(rows, list) or not rows:
                continue

            lines.append(f"### {dataset_id}")
            lines.append("")
            lines.append(
                "| System | DER (weighted) | RTF (mean) | Exact | Within ±1 | MAE | Files | Job |"
            )
            lines.append("|---|---|---|---|---|---|---|---|")
            for row in rows:
                if not isinstance(row, dict):
                    continue
                lines.append(
                    "| "
                    f"{row.get('system_ref') or '-'} | "
                    f"{format_cell(row.get('der_weighted'))} | "
                    f"{format_cell(row.get('rtf_mean'))} | "
                    f"{format_cell(row.get('speaker_count_exact'))} | "
                    f"{format_cell(row.get('speaker_count_within_1'))} | "
                    f"{format_cell(row.get('speaker_count_mae'))} | "
                    f"{row.get('items_count') if row.get('items_count') is not None else '-'} | "
                    f"`{row.get('job_id') or '-'}` |"
                )
            lines.append("")

    lines.append("## Artifacts")
    lines.append("")
    lines.append("- Machine-readable snapshot: `benchmarks/latest.json`")
    lines.append("- Historical table: `benchmarks/history.csv`")
    lines.append("- Raw runs: `benchmarks/runs/YYYY-MM-DD/<job_id>.json`")
    lines.append("")
    return "\n".join(lines)


def write_history_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    sorted_rows = sorted(
        rows,
        key=lambda x: (
            sort_key_for_dt(x.get("job_created_at")),
            str(x.get("job_id") or ""),
            str(x.get("system_ref") or ""),
        ),
        reverse=True,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow({field: row.get(field) for field in CSV_FIELDS})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Publish benchmark run artifacts (runs JSON + latest.json + latest.md + history.csv)."
        )
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="Benchmark API base URL (example: http://127.0.0.1:8080).",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key for benchmark service (can use BENCH_API_KEY env).",
    )
    parser.add_argument(
        "--job-id",
        action="append",
        default=[],
        help="Job ID to publish. Repeat this argument for multiple jobs.",
    )
    parser.add_argument(
        "--output-dir",
        default="benchmarks",
        help="Output directory for generated artifacts.",
    )
    parser.add_argument(
        "--allow-failed",
        action="store_true",
        help="Allow publishing failed jobs (without metrics).",
    )
    parser.add_argument(
        "--include-internal-fields",
        action="store_true",
        help=(
            "Keep internal fields in published artifacts "
            "(source base URL, run_dir, hostname). Default is redacted."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = args.base_url or ""
    api_key = args.api_key or ""
    if args.job_id:
        if not base_url:
            base_url = os.environ.get("BENCH_BASE_URL", "")
        if not api_key:
            api_key = os.environ.get("BENCH_API_KEY", "")
        if not base_url or not api_key:
            print(
                "base_url and api_key are required when --job-id is provided "
                "(or set BENCH_BASE_URL and BENCH_API_KEY).",
                file=sys.stderr,
            )
            return 2

    for job_id in args.job_id:
        raw_run = collect_run_from_api(
            base_url=base_url,
            api_key=api_key,
            job_id=job_id,
            allow_failed=args.allow_failed,
        )
        run = scrub_run_record(
            raw_run,
            redact_internal_fields=not args.include_internal_fields,
        )
        out_path = save_run_record(output_dir, run)
        print(f"saved run: {out_path}")

    loaded_runs = load_all_runs(output_dir)
    runs: list[tuple[Path, dict[str, Any]]] = []
    for run_path, payload in loaded_runs:
        cleaned = scrub_run_record(
            payload,
            redact_internal_fields=not args.include_internal_fields,
        )
        write_json_if_changed(run_path, cleaned)
        runs.append((run_path, cleaned))
    all_rows: list[dict[str, Any]] = []
    for run_path, payload in runs:
        all_rows.extend(rows_from_run(run_path, payload, output_dir))

    snapshot = build_latest_snapshot(runs, all_rows)
    write_json_if_changed(output_dir / "latest.json", snapshot)
    write_history_csv(output_dir / "history.csv", all_rows)
    write_text_if_changed(output_dir / "latest.md", render_markdown(snapshot))

    print(f"indexed runs: {len(runs)}")
    print(f"indexed rows: {len(all_rows)}")
    print(f"updated: {output_dir / 'latest.json'}")
    print(f"updated: {output_dir / 'latest.md'}")
    print(f"updated: {output_dir / 'history.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
