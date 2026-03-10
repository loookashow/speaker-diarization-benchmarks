#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Callable

from foxnose_sdk.auth import JWTAuth, SecureKeyAuth, SimpleKeyAuth
from foxnose_sdk.errors import FoxnoseAPIError
from foxnose_sdk.management import ManagementClient


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


def _list_all(
    loader: Callable[[dict[str, Any]], Any],
) -> list[Any]:
    items: list[Any] = []
    offset = 0
    limit = 100
    while True:
        page = loader({"limit": limit, "offset": offset})
        results = list(getattr(page, "results", []))
        if not results:
            break
        items.extend(results)
        offset += len(results)
        if len(items) >= int(getattr(page, "count", len(items))):
            break
    return items


def _ensure_api(client: ManagementClient, args: argparse.Namespace) -> Any:
    apis = _list_all(lambda params: client.list_apis(params=params))
    existing = next((api for api in apis if api.prefix == args.api_prefix), None)
    if existing is None:
        created = client.create_api(
            {
                "name": args.api_name,
                "prefix": args.api_prefix,
                "description": args.api_description,
                "version": "1",
                "is_auth_required": args.require_auth,
            }
        )
        print(f"[api] created prefix={created.prefix} key={created.key}")
        return created

    print(f"[api] exists prefix={existing.prefix} key={existing.key}")
    needs_update = (
        existing.name != args.api_name
        or (existing.description or "") != args.api_description
        or bool(existing.is_auth_required) != bool(args.require_auth)
    )
    if needs_update:
        updated = client.update_api(
            existing.key,
            {
                "name": args.api_name,
                "description": args.api_description,
                "is_auth_required": args.require_auth,
            },
        )
        print(f"[api] updated key={updated.key}")
        return updated
    return existing


def _resolve_folders(client: ManagementClient, aliases: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for alias in aliases:
        folder = client.get_folder_by_path(alias)
        out[alias] = folder
        print(f"[folder] found alias={alias} key={folder.key}")
    return out


def _ensure_api_folders(client: ManagementClient, api_key: str, folders: dict[str, Any]) -> None:
    desired_methods = ["get_many", "get_one"]
    current_list = _list_all(lambda params: client.list_api_folders(api_key, params=params))
    current_by_folder = {item.folder: item for item in current_list}

    for alias, folder in folders.items():
        entry = current_by_folder.get(folder.key)
        if entry is None:
            client.add_api_folder(
                api_key=api_key,
                folder_key=folder.key,
                allowed_methods=desired_methods,
                description_get_many=f"List resources from {alias}.",
                description_get_one=f"Get one resource from {alias}.",
                description_search=f"Search resources in {alias}.",
                description_schema=f"Get JSON schema for {alias}.",
            )
            print(f"[api-folder] connected alias={alias} methods={desired_methods}")
            continue
        if sorted(entry.allowed_methods) != sorted(desired_methods):
            client.update_api_folder(
                api_key=api_key,
                folder_key=folder.key,
                allowed_methods=desired_methods,
                description_get_many=f"List resources from {alias}.",
                description_get_one=f"Get one resource from {alias}.",
                description_search=f"Search resources in {alias}.",
                description_schema=f"Get JSON schema for {alias}.",
            )
            print(f"[api-folder] updated alias={alias} methods={desired_methods}")
            continue
        print(f"[api-folder] ok alias={alias}")


def _ensure_flux_role(client: ManagementClient, args: argparse.Namespace, api_key: str) -> Any:
    roles = _list_all(lambda params: client.list_flux_roles(params=params))
    role = next((item for item in roles if item.name == args.role_name), None)
    if role is None:
        role = client.create_flux_role(
            {
                "name": args.role_name,
                "description": args.role_description,
            }
        )
        print(f"[role] created name={role.name} key={role.key}")
    else:
        print(f"[role] exists name={role.name} key={role.key}")

    permission = client.upsert_flux_role_permission(
        role.key,
        {
            "content_type": "flux-apis",
            "actions": ["read"],
            "all_objects": False,
        },
    )
    print(
        "[role] permission set "
        f"content_type={permission.content_type} "
        f"all_objects={permission.all_objects}"
    )

    existing_objects = _list_flux_permission_objects_robust(client, role.key)
    if api_key not in existing_objects:
        try:
            client.add_flux_permission_object(
                role.key,
                {
                    "content_type": "flux-apis",
                    "object_key": api_key,
                },
            )
            print(f"[role] api access added api_key={api_key}")
        except FoxnoseAPIError as exc:
            if exc.status_code == 422 and exc.error_code == "permission_object_already_added":
                print(f"[role] api access already present api_key={api_key}")
            else:
                raise
    else:
        print(f"[role] api access already present api_key={api_key}")
    return role


def _list_flux_permission_objects_robust(client: ManagementClient, role_key: str) -> set[str]:
    try:
        objects = client.list_flux_permission_objects(role_key, content_type="flux-apis")
        return {obj.object_key for obj in objects}
    except Exception:
        raw = client.request(
            "GET",
            f"/permissions/flux-api/roles/{role_key}/permissions/objects/",
            params={"content_type": "flux-apis"},
        )
        if isinstance(raw, dict):
            raw_items = raw.get("results", [])
        elif isinstance(raw, list):
            raw_items = raw
        else:
            raw_items = []
        out: set[str] = set()
        for item in raw_items:
            if isinstance(item, dict) and item.get("object_key"):
                out.add(str(item["object_key"]))
        return out


def _maybe_create_flux_key(
    client: ManagementClient, args: argparse.Namespace, role_key: str
) -> dict[str, Any] | None:
    if not args.create_flux_key:
        print("[flux-key] skipped by flag")
        return None

    if args.reuse_existing_key:
        keys = _list_all(lambda params: client.list_flux_api_keys(params=params))
        same = next(
            (
                key
                for key in keys
                if (key.description or "") == args.flux_key_description and key.role == role_key
            ),
            None,
        )
        if same is not None:
            print(f"[flux-key] reuse existing key={same.key} (secret masked)")
            return {
                "key": same.key,
                "description": same.description,
                "public_key": same.public_key,
                "secret_key": same.secret_key,
                "role": same.role,
                "created_at": str(same.created_at),
                "is_new": False,
            }

    created = client.create_flux_api_key(
        {
            "description": args.flux_key_description,
            "role": role_key,
        }
    )
    print(f"[flux-key] created key={created.key}")
    return {
        "key": created.key,
        "description": created.description,
        "public_key": created.public_key,
        "secret_key": created.secret_key,
        "role": created.role,
        "created_at": str(created.created_at),
        "is_new": True,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap FoxNose Flux API for diarization benchmarks: "
            "create/reuse API, connect folders, configure role, create Flux key."
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
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout seconds")

    parser.add_argument("--api-prefix", default="bench-v1", help="Flux API prefix")
    parser.add_argument(
        "--api-name", default="Diarization Benchmarks API", help="Flux API display name"
    )
    parser.add_argument(
        "--api-description",
        default="Read-only API for benchmark jobs and reports.",
        help="Flux API description",
    )
    parser.add_argument(
        "--require-auth",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether Flux API requires key auth",
    )
    parser.add_argument(
        "--folders",
        nargs="+",
        default=["benchmark_jobs", "benchmark_reports", "benchmark_report_items"],
        help="Folder aliases to connect",
    )
    parser.add_argument("--role-name", default="benchmark_flux_reader", help="Flux role name")
    parser.add_argument(
        "--role-description",
        default="Read access to diarization benchmark Flux API.",
        help="Flux role description",
    )

    parser.add_argument(
        "--create-flux-key",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Create Flux API key for the role",
    )
    parser.add_argument(
        "--flux-key-description",
        default="Diarization Benchmarks Client Key",
        help="Description for created Flux key",
    )
    parser.add_argument(
        "--reuse-existing-key",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reuse existing key by description+role instead of creating a new one",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned configuration only",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.dry_run:
        print(
            json.dumps(
                {
                    "environment_key": args.environment_key,
                    "api_prefix": args.api_prefix,
                    "folders": args.folders,
                    "require_auth": args.require_auth,
                    "create_flux_key": args.create_flux_key,
                },
                indent=2,
            )
        )
        return 0

    client = _build_client(args)
    try:
        api = _ensure_api(client, args)
        folders = _resolve_folders(client, args.folders)
        _ensure_api_folders(client, api.key, folders)
        role = _ensure_flux_role(client, args, api.key)
        key_info = _maybe_create_flux_key(client, args, role.key)

        result = {
            "environment_key": args.environment_key,
            "api": {
                "key": api.key,
                "name": api.name,
                "prefix": api.prefix,
                "is_auth_required": api.is_auth_required,
            },
            "role": {
                "key": role.key,
                "name": role.name,
            },
            "folders": {alias: folder.key for alias, folder in folders.items()},
            "flux_base_url": f"https://{args.environment_key}.fxns.io/{api.prefix}",
            "endpoints": {
                "list_jobs": f"https://{args.environment_key}.fxns.io/{api.prefix}/benchmark_jobs",
                "get_job": f"https://{args.environment_key}.fxns.io/{api.prefix}/benchmark_jobs/{{jobId}}",
                "list_reports": f"https://{args.environment_key}.fxns.io/{api.prefix}/benchmark_reports",
                "get_report": f"https://{args.environment_key}.fxns.io/{api.prefix}/benchmark_reports/{{reportKey}}",
            },
            "flux_api_key": key_info,
        }
        print("\n=== Flux bootstrap complete ===")
        print(json.dumps(result, ensure_ascii=True, indent=2))
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
