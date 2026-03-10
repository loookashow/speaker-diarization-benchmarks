from __future__ import annotations

import platform
import socket
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from foxnose_sdk.auth import JWTAuth, SecureKeyAuth, SimpleKeyAuth
from foxnose_sdk.errors import FoxnoseAPIError
from foxnose_sdk.management import ManagementClient

from app.config import Settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class FoxnoseStore:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._folder_keys: dict[str, str] = {}
        self._folder_lock = threading.Lock()

    @property
    def settings(self) -> Settings:
        return self._settings

    def _build_auth(self) -> JWTAuth | SimpleKeyAuth | SecureKeyAuth:
        mode = self._settings.foxnose_auth_mode
        if mode == "jwt":
            token = self._settings.foxnose_access_token
            if not token:
                raise RuntimeError("FOXNOSE_ACCESS_TOKEN is required for FOXNOSE_AUTH_MODE=jwt")
            return JWTAuth.from_static_token(token)

        if mode == "simple":
            public_key = self._settings.foxnose_public_key
            secret_key = self._settings.foxnose_secret_key
            if not public_key or not secret_key:
                raise RuntimeError(
                    "FOXNOSE_PUBLIC_KEY and FOXNOSE_SECRET_KEY are required for simple auth"
                )
            return SimpleKeyAuth(public_key, secret_key)

        if mode != "secure":
            raise RuntimeError(f"Unsupported FOXNOSE_AUTH_MODE: {mode}")

        public_key = self._settings.foxnose_public_key
        private_key = self._settings.foxnose_private_key
        if not public_key or not private_key:
            raise RuntimeError(
                "FOXNOSE_PUBLIC_KEY and FOXNOSE_PRIVATE_KEY are required for secure auth"
            )
        return SecureKeyAuth(public_key, private_key)

    @contextmanager
    def client(self) -> Iterator[ManagementClient]:
        if not self._settings.foxnose_environment_key:
            raise RuntimeError("FOXNOSE_ENV_KEY is required")

        client = ManagementClient(
            base_url=self._settings.foxnose_base_url,
            environment_key=self._settings.foxnose_environment_key,
            auth=self._build_auth(),
            timeout=30.0,
        )
        try:
            yield client
        finally:
            client.close()

    def _resolve_folder_key(self, client: ManagementClient, alias_or_key: str) -> str:
        with self._folder_lock:
            cached = self._folder_keys.get(alias_or_key)
        if cached:
            return cached

        try:
            folder = client.get_folder_by_path(alias_or_key)
            key = folder.key
        except FoxnoseAPIError as exc:
            if exc.status_code != 404:
                raise
            folder = client.get_folder(alias_or_key)
            key = folder.key

        with self._folder_lock:
            self._folder_keys[alias_or_key] = key
        return key

    def ensure_ready(self) -> None:
        with self.client() as client:
            self._resolve_folder_key(client, self._settings.foxnose_jobs_folder)
            self._resolve_folder_key(client, self._settings.foxnose_reports_folder)
            self._resolve_folder_key(client, self._settings.foxnose_report_items_folder)

    def create_job_resource(self, payload: dict[str, Any]) -> str:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_jobs_folder)
            resource = client.create_resource(folder_key, {"data": payload})
            return resource.key

    def update_job_resource(self, job_key: str, payload: dict[str, Any]) -> None:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_jobs_folder)
            # update_resource mutates resource metadata; job state lives in revision data.
            client.create_revision(folder_key, job_key, {"data": payload})

    def get_job_resource(self, job_key: str) -> dict[str, Any]:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_jobs_folder)
            data = dict(client.get_resource_data(folder_key, job_key))
            data["job_id"] = job_key
            return data

    def create_report_resource(self, payload: dict[str, Any]) -> str:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_reports_folder)
            resource = client.create_resource(folder_key, {"data": payload})
            return resource.key

    def get_report_resource(self, report_key: str) -> dict[str, Any]:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_reports_folder)
            data = dict(client.get_resource_data(folder_key, report_key))
            data["report_key"] = report_key
            return data

    def create_report_items(self, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        with self.client() as client:
            folder_key = self._resolve_folder_key(
                client, self._settings.foxnose_report_items_folder
            )
            for payload in items:
                client.create_resource(folder_key, {"data": payload})

    def list_reports(self, limit: int, offset: int) -> tuple[int, list[dict[str, Any]]]:
        with self.client() as client:
            folder_key = self._resolve_folder_key(client, self._settings.foxnose_reports_folder)
            page = client.list_resources(
                folder_key,
                params={"limit": limit, "offset": offset},
            )

            out: list[dict[str, Any]] = []
            for summary in page.results:
                data = dict(client.get_resource_data(folder_key, summary.key))
                data["report_key"] = summary.key
                out.append(data)
            return page.count, out

    def build_new_job_payload(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        now = utc_now_iso()
        systems = request_payload.get("systems", [])
        return {
            "status": "queued",
            "progress_percent": 0,
            "eta_seconds": None,
            "current_step": "queued",
            "submitted_at": now,
            "started_at": None,
            "finished_at": None,
            "dataset_id": request_payload["dataset_id"],
            "request": request_payload,
            "system_matrix": {"systems": systems},
            "report_key": None,
            "error": None,
            "worker_info": {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python": platform.python_version(),
            },
        }
