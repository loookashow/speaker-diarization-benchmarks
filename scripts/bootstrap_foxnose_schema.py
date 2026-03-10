#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from foxnose_sdk.auth import JWTAuth, SecureKeyAuth, SimpleKeyAuth
from foxnose_sdk.errors import FoxnoseAPIError
from foxnose_sdk.management import ManagementClient


@dataclass(frozen=True)
class FieldSpec:
    key: str
    name: str
    type: str
    description: str = ""
    meta: dict[str, Any] = field(default_factory=dict)
    required: bool = False
    nullable: bool = False
    multiple: bool = False
    localizable: bool = False
    searchable: bool = False
    private: bool = False
    vectorizable: bool = False
    parent: str | None = None

    def payload(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "key": self.key,
            "name": self.name,
            "description": self.description,
            "type": self.type,
        }
        if self.meta:
            body["meta"] = dict(self.meta)
        if self.parent:
            body["parent"] = self.parent
        if self.required:
            body["required"] = True
        if self.nullable:
            body["nullable"] = True
        if self.multiple:
            body["multiple"] = True
        if self.localizable:
            body["localizable"] = True
        if self.searchable:
            body["searchable"] = True
        if self.private:
            body["private"] = True
        if self.vectorizable:
            body["vectorizable"] = True
        return body


@dataclass(frozen=True)
class FolderSpec:
    alias: str
    name: str
    fields: tuple[FieldSpec, ...]


def _dt(ts: Any) -> datetime:
    if isinstance(ts, datetime):
        return ts
    if not isinstance(ts, str):
        return datetime.min
    iso = ts.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso)
    except ValueError:
        return datetime.min


def _latest_published(versions: list[Any]) -> Any | None:
    published = [
        v
        for v in versions
        if getattr(v, "published_at", None) and not getattr(v, "archived_at", None)
    ]
    if not published:
        return None
    return max(published, key=lambda v: getattr(v, "version_number", 0) or 0)


def _matching_draft(versions: list[Any], version_name: str) -> Any | None:
    drafts = [
        v
        for v in versions
        if not getattr(v, "published_at", None)
        and not getattr(v, "archived_at", None)
        and getattr(v, "name", "") == version_name
    ]
    if not drafts:
        return None
    return max(drafts, key=lambda v: _dt(getattr(v, "created_at", None)))


def _normalize(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in sorted(value.items(), key=lambda item: item[0])}
    if isinstance(value, list):
        return [_normalize(v) for v in value]
    return value


def _meta_matches(expected: dict[str, Any], actual: dict[str, Any] | None) -> bool:
    actual = actual or {}
    for key, exp in expected.items():
        if _normalize(actual.get(key)) != _normalize(exp):
            return False
    return True


def _field_matches(spec: FieldSpec, current: Any) -> bool:
    checks = [
        ("key", spec.key),
        ("name", spec.name),
        ("description", spec.description),
        ("type", spec.type),
        ("required", spec.required),
        ("nullable", spec.nullable),
        ("multiple", spec.multiple),
        ("localizable", spec.localizable),
        ("searchable", spec.searchable),
        ("private", spec.private),
        ("vectorizable", spec.vectorizable),
        ("parent", spec.parent),
    ]
    for attr, expected in checks:
        if getattr(current, attr, None) != expected:
            return False
    return _meta_matches(spec.meta, getattr(current, "meta", {}))


def _find_folder_by_alias(client: ManagementClient, alias: str) -> Any | None:
    """Find folder without query params to keep Secure auth deterministic."""
    items = client.list_folders().results
    for folder in items:
        if getattr(folder, "alias", None) == alias or getattr(folder, "path", None) == alias:
            return folder
    return None


def _ensure_folder(client: ManagementClient, spec: FolderSpec) -> Any:
    try:
        folder = _find_folder_by_alias(client, spec.alias)
        if folder is None:
            raise FoxnoseAPIError(
                message="Folder not found",
                status_code=404,
                error_code="folder_not_found",
                response_body={"message": "Folder not found", "error_code": "folder_not_found"},
            )
        print(
            "[folder] exists "
            f"alias={spec.alias} key={folder.key} "
            f"folder_type={getattr(folder, 'folder_type', None)} "
            f"content_type={getattr(folder, 'content_type', None)} "
            f"mode={getattr(folder, 'mode', None)}"
        )
    except FoxnoseAPIError as exc:
        if exc.status_code != 404:
            raise
        folder = client.create_folder(
            {
                "name": spec.name,
                "alias": spec.alias,
                "folder_type": "collection",
                "content_type": "document",
                "strict_reference": False,
            }
        )
        print(f"[folder] created alias={spec.alias} key={folder.key}")

    folder_type = getattr(folder, "folder_type", None)
    content_type = getattr(folder, "content_type", None)
    if folder_type != "collection" or content_type != "document":
        raise RuntimeError(
            "Folder alias exists but has incompatible type. "
            f"alias={spec.alias}, key={folder.key}, "
            f"folder_type={folder_type}, content_type={content_type}. "
            "Expected folder_type=collection and content_type=document."
        )

    # Fail fast with a clear error when credentials cannot access schema-model routes.
    try:
        _call_with_folder_fallback(
            op_name="list_folder_versions(preflight)",
            folder_key=folder.key,
            folder_alias=spec.alias,
            fn=lambda candidate: client.list_folder_versions(candidate),
        )
    except FoxnoseAPIError as exc:
        if exc.status_code == 404:
            raise RuntimeError(
                "Folder exists but schema-model endpoint is not accessible. "
                f"alias={spec.alias}, key={folder.key}. "
                "This usually means insufficient credential permissions for schema operations."
            ) from exc
        raise

    return folder


def _folder_candidates(folder_key: str, folder_alias: str | None) -> list[str]:
    out = [folder_key]
    if folder_alias and folder_alias not in out:
        out.append(folder_alias)
    return out


def _call_with_folder_fallback(
    *,
    op_name: str,
    folder_key: str,
    folder_alias: str | None,
    fn: Any,
) -> Any:
    candidates = _folder_candidates(folder_key, folder_alias)
    for idx, candidate in enumerate(candidates):
        try:
            return fn(candidate)
        except FoxnoseAPIError as exc:
            is_last = idx >= (len(candidates) - 1)
            if exc.status_code != 404 or is_last:
                raise
            next_candidate = candidates[idx + 1]
            print(
                f"[warn] {op_name}: folder '{candidate}' returned 404; "
                f"retrying with '{next_candidate}'"
            )
    raise RuntimeError(f"{op_name}: no folder candidates available")


def _list_fields_by_path(
    client: ManagementClient,
    folder_key: str,
    folder_alias: str | None,
    version_key: str,
) -> dict[str, Any]:
    items = _call_with_folder_fallback(
        op_name="list_folder_fields",
        folder_key=folder_key,
        folder_alias=folder_alias,
        fn=lambda candidate: client.list_folder_fields(candidate, version_key).results,
    )
    return {field.path: field for field in items}


def _version_is_up_to_date(
    client: ManagementClient,
    folder_key: str,
    folder_alias: str | None,
    version_key: str,
    fields: tuple[FieldSpec, ...],
) -> bool:
    actual_by_path = _list_fields_by_path(client, folder_key, folder_alias, version_key)
    for spec in fields:
        current = actual_by_path.get(spec.key)
        if current is None or not _field_matches(spec, current):
            return False
    return True


def _ensure_draft_version(
    client: ManagementClient,
    folder_key: str,
    folder_alias: str | None,
    version_name: str,
    fields: tuple[FieldSpec, ...],
) -> tuple[Any | None, bool]:
    versions = _call_with_folder_fallback(
        op_name="list_folder_versions",
        folder_key=folder_key,
        folder_alias=folder_alias,
        fn=lambda candidate: client.list_folder_versions(candidate).results,
    )
    draft = _matching_draft(versions, version_name)

    if draft is not None:
        print(f"[version] using existing draft key={draft.key} name={draft.name}")
        return draft, True

    latest_published = _latest_published(versions)
    if latest_published and _version_is_up_to_date(
        client,
        folder_key,
        folder_alias,
        latest_published.key,
        fields,
    ):
        print(f"[version] already up-to-date published={latest_published.key}; skipping")
        return None, False

    payload = {
        "name": version_name,
        "description": "Benchmark schema bootstrap",
    }
    if latest_published is not None:
        draft = _call_with_folder_fallback(
            op_name="create_folder_version(copy_from)",
            folder_key=folder_key,
            folder_alias=folder_alias,
            fn=lambda candidate: client.create_folder_version(
                candidate,
                payload,
                copy_from=latest_published.key,
            ),
        )
        print(f"[version] created draft key={draft.key} copy_from={latest_published.key}")
    else:
        draft = _call_with_folder_fallback(
            op_name="create_folder_version",
            folder_key=folder_key,
            folder_alias=folder_alias,
            fn=lambda candidate: client.create_folder_version(candidate, payload),
        )
        print(f"[version] created first draft key={draft.key}")
    return draft, True


def _sync_fields(
    client: ManagementClient,
    folder_key: str,
    folder_alias: str | None,
    draft_key: str,
    fields: tuple[FieldSpec, ...],
) -> None:
    existing = _list_fields_by_path(client, folder_key, folder_alias, draft_key)
    for spec in fields:
        current = existing.get(spec.key)
        payload = spec.payload()
        if current is None:
            _call_with_folder_fallback(
                op_name=f"create_folder_field({spec.key})",
                folder_key=folder_key,
                folder_alias=folder_alias,
                fn=lambda candidate: client.create_folder_field(candidate, draft_key, payload),
            )
            print(f"[field] created {spec.key}")
            continue
        if _field_matches(spec, current):
            print(f"[field] ok {spec.key}")
            continue
        _call_with_folder_fallback(
            op_name=f"update_folder_field({spec.key})",
            folder_key=folder_key,
            folder_alias=folder_alias,
            fn=lambda candidate: client.update_folder_field(
                candidate,
                draft_key,
                spec.key,
                payload,
            ),
        )
        print(f"[field] updated {spec.key}")


def _publish(
    client: ManagementClient,
    folder_key: str,
    folder_alias: str | None,
    version_key: str,
) -> Any:
    published = _call_with_folder_fallback(
        op_name="publish_folder_version",
        folder_key=folder_key,
        folder_alias=folder_alias,
        fn=lambda candidate: client.publish_folder_version(candidate, version_key),
    )
    print(f"[publish] folder={folder_key} version={version_key}")
    return published


def _build_specs() -> tuple[FolderSpec, ...]:
    jobs_fields = (
        FieldSpec(
            key="status",
            name="Status",
            description="Job lifecycle status",
            type="string",
            required=True,
            searchable=True,
            meta={
                "enum": ["queued", "running", "completed", "failed", "canceled"],
                "max_length": 16,
            },
        ),
        FieldSpec(
            key="progress_percent",
            name="Progress Percent",
            description="Execution progress in percent",
            type="number",
            required=True,
            meta={"minimum": 0, "maximum": 100},
        ),
        FieldSpec(
            key="eta_seconds",
            name="ETA Seconds",
            description="Estimated remaining seconds",
            type="integer",
            nullable=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="current_step",
            name="Current Step",
            description="Current execution stage",
            type="string",
            nullable=True,
            meta={"max_length": 255},
        ),
        FieldSpec(
            key="submitted_at",
            name="Submitted At",
            description="Submission timestamp",
            type="datetime",
            required=True,
            searchable=True,
        ),
        FieldSpec(
            key="started_at",
            name="Started At",
            description="Execution start timestamp",
            type="datetime",
            nullable=True,
            searchable=True,
        ),
        FieldSpec(
            key="finished_at",
            name="Finished At",
            description="Execution finish timestamp",
            type="datetime",
            nullable=True,
            searchable=True,
        ),
        FieldSpec(
            key="dataset_id",
            name="Dataset ID",
            description="Dataset identifier",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 64, "pattern": "^[a-z0-9._-]+$"},
        ),
        FieldSpec(
            key="request",
            name="Request",
            description="Original run request JSON",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="system_matrix",
            name="System Matrix",
            description="Normalized systems and versions",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="report_key",
            name="Report Key",
            description="Reference to benchmark report",
            type="reference",
            nullable=True,
            searchable=True,
        ),
        FieldSpec(
            key="error",
            name="Error",
            description="Error text for failed jobs",
            type="text",
            nullable=True,
        ),
        FieldSpec(
            key="worker_info",
            name="Worker Info",
            description="Worker runtime metadata",
            type="json",
            nullable=True,
        ),
    )

    reports_fields = (
        FieldSpec(
            key="job_key",
            name="Job Key",
            description="Reference to benchmark job",
            type="reference",
            required=True,
            searchable=True,
        ),
        FieldSpec(
            key="status",
            name="Status",
            description="Report status",
            type="string",
            required=True,
            searchable=True,
            meta={"enum": ["completed", "failed"], "max_length": 16},
        ),
        FieldSpec(
            key="created_at",
            name="Created At",
            description="Report creation timestamp",
            type="datetime",
            required=True,
            searchable=True,
        ),
        FieldSpec(
            key="dataset_id",
            name="Dataset ID",
            description="Dataset identifier",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 64, "pattern": "^[a-z0-9._-]+$"},
        ),
        FieldSpec(
            key="systems",
            name="Systems",
            description="Benchmarked systems list",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="metrics_summary",
            name="Metrics Summary",
            description="Top-level metrics summary",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="speaker_count_table",
            name="Speaker Count Table",
            description="Speaker count benchmark table",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="der_summary",
            name="DER Summary",
            description="DER aggregate metrics",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="rtf_summary",
            name="RTF Summary",
            description="RTF aggregate metrics",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="run_config",
            name="Run Config",
            description="Resolved benchmark run settings",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="dataset_manifest",
            name="Dataset Manifest",
            description="Dataset selection and manifest metadata",
            type="json",
            nullable=True,
        ),
        FieldSpec(
            key="machine_fingerprint",
            name="Machine Fingerprint",
            description="CPU/OS/runtime fingerprint for reproducibility",
            type="json",
            required=True,
        ),
        FieldSpec(
            key="items_count",
            name="Items Count",
            description="Number of per-file rows",
            type="integer",
            required=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="artifact_paths",
            name="Artifact Paths",
            description="Paths to large local artifacts",
            type="json",
            nullable=True,
        ),
        FieldSpec(
            key="error",
            name="Error",
            description="Failure details for failed reports",
            type="text",
            nullable=True,
        ),
    )

    report_items_fields = (
        FieldSpec(
            key="report_key",
            name="Report Key",
            description="Reference to parent report",
            type="reference",
            required=True,
            searchable=True,
        ),
        FieldSpec(
            key="job_key",
            name="Job Key",
            description="Reference to job",
            type="reference",
            required=True,
            searchable=True,
        ),
        FieldSpec(
            key="dataset_id",
            name="Dataset ID",
            description="Dataset identifier",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 64, "pattern": "^[a-z0-9._-]+$"},
        ),
        FieldSpec(
            key="system_id",
            name="System ID",
            description="Diarization system identifier",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 64, "pattern": "^[a-z0-9._-]+$"},
        ),
        FieldSpec(
            key="system_version",
            name="System Version",
            description="Version of benchmarked system",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 64},
        ),
        FieldSpec(
            key="audio_id",
            name="Audio ID",
            description="Per-file audio identifier",
            type="string",
            required=True,
            searchable=True,
            meta={"max_length": 255},
        ),
        FieldSpec(
            key="run_index",
            name="Run Index",
            description="Repeat run index (1-based)",
            type="integer",
            required=True,
            meta={"minimum": 1},
        ),
        FieldSpec(
            key="audio_seconds",
            name="Audio Seconds",
            description="Audio duration",
            type="number",
            required=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="processing_seconds",
            name="Processing Seconds",
            description="Processing duration",
            type="number",
            required=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="rtf",
            name="RTF",
            description="Real-time factor",
            type="number",
            required=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="der",
            name="DER",
            description="Per-file DER",
            type="number",
            nullable=True,
            meta={"minimum": 0, "maximum": 1},
        ),
        FieldSpec(
            key="gt_speakers",
            name="GT Speakers",
            description="Ground truth speaker count",
            type="integer",
            nullable=True,
            meta={"minimum": 1},
        ),
        FieldSpec(
            key="pred_speakers",
            name="Pred Speakers",
            description="Predicted speaker count",
            type="integer",
            nullable=True,
            meta={"minimum": 1},
        ),
        FieldSpec(
            key="abs_count_error",
            name="Absolute Count Error",
            description="Absolute speaker count error",
            type="integer",
            nullable=True,
            meta={"minimum": 0},
        ),
        FieldSpec(
            key="bucket",
            name="Speaker Bucket",
            description="Speaker-count bucket",
            type="string",
            nullable=True,
            searchable=True,
            meta={"enum": ["1", "2", "3", "4", "5", "6-7", "8+"]},
        ),
        FieldSpec(
            key="extra",
            name="Extra",
            description="Extra per-file diagnostics",
            type="json",
            nullable=True,
        ),
    )

    return (
        FolderSpec(alias="benchmark_jobs", name="Benchmark Jobs", fields=jobs_fields),
        FolderSpec(alias="benchmark_reports", name="Benchmark Reports", fields=reports_fields),
        FolderSpec(
            alias="benchmark_report_items",
            name="Benchmark Report Items",
            fields=report_items_fields,
        ),
    )


def _inject_reference_targets(
    specs: tuple[FolderSpec, ...], folder_keys: dict[str, str]
) -> tuple[FolderSpec, ...]:
    def mapped_fields(folder: FolderSpec) -> tuple[FieldSpec, ...]:
        out: list[FieldSpec] = []
        for field_spec in folder.fields:
            if field_spec.type != "reference":
                out.append(field_spec)
                continue
            if folder.alias == "benchmark_jobs" and field_spec.key == "report_key":
                target_alias = "benchmark_reports"
            elif folder.alias == "benchmark_reports" and field_spec.key == "job_key":
                target_alias = "benchmark_jobs"
            elif folder.alias == "benchmark_report_items" and field_spec.key == "report_key":
                target_alias = "benchmark_reports"
            elif folder.alias == "benchmark_report_items" and field_spec.key == "job_key":
                target_alias = "benchmark_jobs"
            else:
                raise RuntimeError(
                    f"Unsupported reference mapping: {folder.alias}.{field_spec.key}"
                )
            target_key = folder_keys[target_alias]
            out.append(
                FieldSpec(
                    key=field_spec.key,
                    name=field_spec.name,
                    type=field_spec.type,
                    description=field_spec.description,
                    meta={"target": target_key},
                    required=field_spec.required,
                    nullable=field_spec.nullable,
                    multiple=field_spec.multiple,
                    localizable=field_spec.localizable,
                    searchable=field_spec.searchable,
                    private=field_spec.private,
                    vectorizable=field_spec.vectorizable,
                    parent=field_spec.parent,
                )
            )
        return tuple(out)

    return tuple(FolderSpec(alias=s.alias, name=s.name, fields=mapped_fields(s)) for s in specs)


def _build_client(args: argparse.Namespace) -> ManagementClient:
    if args.auth_mode == "jwt":
        token = args.token or os.getenv("FOXNOSE_ACCESS_TOKEN")
        if not token:
            raise RuntimeError(
                "Missing token. Set --token or FOXNOSE_ACCESS_TOKEN for auth-mode=jwt."
            )
        auth = JWTAuth.from_static_token(token)
    elif args.auth_mode == "simple":
        public_key = args.public_key or os.getenv("FOXNOSE_PUBLIC_KEY")
        secret_key = args.secret_key or os.getenv("FOXNOSE_SECRET_KEY")
        if not public_key or not secret_key:
            raise RuntimeError(
                "Missing public/secret key. "
                "Set --public-key and --secret-key or "
                "FOXNOSE_PUBLIC_KEY/FOXNOSE_SECRET_KEY."
            )
        auth = SimpleKeyAuth(public_key, secret_key)
    else:
        public_key = args.public_key or os.getenv("FOXNOSE_PUBLIC_KEY")
        private_key = args.private_key or os.getenv("FOXNOSE_PRIVATE_KEY")
        if not public_key or not private_key:
            raise RuntimeError(
                "Missing public/private key. "
                "Set --public-key and --private-key or "
                "FOXNOSE_PUBLIC_KEY/FOXNOSE_PRIVATE_KEY."
            )
        auth = SecureKeyAuth(public_key, private_key)

    return ManagementClient(
        base_url=args.base_url,
        environment_key=args.environment_key,
        auth=auth,
        timeout=args.timeout,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap FoxNose folders + schema versions + fields "
            "for diarization benchmark storage."
        )
    )
    parser.add_argument("--environment-key", required=True, help="FoxNose environment key")
    parser.add_argument(
        "--base-url", default="https://api.foxnose.net", help="Management API base URL"
    )
    parser.add_argument(
        "--auth-mode",
        choices=["jwt", "simple", "secure"],
        default="jwt",
        help="Management API auth mode",
    )
    parser.add_argument("--token", help="JWT access token (or FOXNOSE_ACCESS_TOKEN)")
    parser.add_argument("--public-key", help="API public key (or FOXNOSE_PUBLIC_KEY)")
    parser.add_argument(
        "--secret-key", help="API secret key for simple auth (or FOXNOSE_SECRET_KEY)"
    )
    parser.add_argument(
        "--private-key", help="API private key for secure auth (or FOXNOSE_PRIVATE_KEY)"
    )
    parser.add_argument(
        "--version-name",
        default="benchmark_schema_v1",
        help="Schema draft version name used for all folders",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions only (no API write operations).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    specs = _build_specs()

    if args.dry_run:
        print(
            json.dumps(
                {"folders": [s.alias for s in specs], "version_name": args.version_name}, indent=2
            )
        )
        return 0

    client = _build_client(args)
    try:
        folder_map: dict[str, Any] = {}
        for spec in specs:
            folder_map[spec.alias] = _ensure_folder(client, spec)

        folder_keys = {alias: folder.key for alias, folder in folder_map.items()}
        specs = _inject_reference_targets(specs, folder_keys)

        published_versions: dict[str, str | None] = {}
        for spec in specs:
            folder = folder_map[spec.alias]
            draft, should_publish = _ensure_draft_version(
                client=client,
                folder_key=folder.key,
                folder_alias=spec.alias,
                version_name=args.version_name,
                fields=spec.fields,
            )
            if not should_publish:
                published_versions[spec.alias] = None
                continue
            if draft is None:
                raise RuntimeError(f"Missing draft for {spec.alias}")
            _sync_fields(
                client,
                folder.key,
                spec.alias,
                draft.key,
                spec.fields,
            )
            published = _publish(client, folder.key, spec.alias, draft.key)
            published_versions[spec.alias] = published.key

        print("\n=== Bootstrap complete ===")
        for alias in [s.alias for s in specs]:
            print(
                json.dumps(
                    {
                        "folder_alias": alias,
                        "folder_key": folder_keys[alias],
                        "published_version_key": published_versions[alias],
                    },
                    ensure_ascii=True,
                )
            )
        return 0
    except FoxnoseAPIError as exc:
        print(
            json.dumps(
                {
                    "error": "foxnose_api_error",
                    "status_code": exc.status_code,
                    "message": exc.message,
                    "error_code": exc.error_code,
                    "detail": exc.detail,
                    "response_body": exc.response_body,
                },
                ensure_ascii=True,
            ),
            file=sys.stderr,
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        return 1
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
