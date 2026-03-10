#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DatasetSpec:
    dataset_id: str
    root: Path
    audio_dir: str
    rttm_dir: str

    @property
    def audio_target(self) -> Path:
        return self.root / self.audio_dir

    @property
    def rttm_target(self) -> Path:
        return self.root / self.rttm_dir


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bind external Open Track dataset directories "
            "(audio/rttm) into local benchmark layout via symlinks."
        )
    )
    parser.add_argument(
        "--datasets-file",
        default="config/datasets.open_track.json",
        help="Dataset config (target layout).",
    )
    parser.add_argument(
        "--sources-file",
        default="config/open_track_sources.example.json",
        help="Source mapping file (dataset_id -> audio_src/rttm_src).",
    )
    parser.add_argument(
        "--datasets-root",
        default="data/datasets",
        help="Base root for relative dataset paths in datasets-file.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow replacing existing targets (files/dirs/symlinks).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned actions without filesystem changes.",
    )
    return parser.parse_args()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_specs(datasets_file: Path, datasets_root: Path) -> dict[str, DatasetSpec]:
    raw = load_json(datasets_file)
    entries = raw.get("datasets", raw) if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        raise RuntimeError("datasets file must be a list or {'datasets': [...]} format")

    out: dict[str, DatasetSpec] = {}
    for item in entries:
        if not isinstance(item, dict):
            continue
        dataset_id = str(item.get("dataset_id") or "").strip()
        if not dataset_id:
            continue

        root_raw = item.get("root", dataset_id)
        root = Path(str(root_raw)).expanduser()
        if not root.is_absolute():
            root = datasets_root / root

        spec = DatasetSpec(
            dataset_id=dataset_id,
            root=root,
            audio_dir=str(item.get("audio_dir", "audio")),
            rttm_dir=str(item.get("reference_rttm_dir", "rttm")),
        )
        out[dataset_id] = spec
    return out


def load_sources(sources_file: Path) -> list[dict[str, str]]:
    raw = load_json(sources_file)
    entries = raw.get("sources", raw) if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        raise RuntimeError("sources file must be a list or {'sources': [...]} format")

    out: list[dict[str, str]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue
        dataset_id = str(item.get("dataset_id") or "").strip()
        audio_src = str(item.get("audio_src") or "").strip()
        rttm_src = str(item.get("rttm_src") or "").strip()
        if not dataset_id:
            continue
        out.append(
            {
                "dataset_id": dataset_id,
                "audio_src": audio_src,
                "rttm_src": rttm_src,
            }
        )
    return out


def _ensure_replaceable(target: Path, force: bool) -> None:
    if not target.exists() and not target.is_symlink():
        return

    if target.is_symlink():
        return

    if target.is_dir():
        try:
            has_children = next(target.iterdir(), None) is not None
        except OSError:
            has_children = True
        if has_children and not force:
            raise RuntimeError(f"target is non-empty directory (use --force): {target}")
        return

    if not force:
        raise RuntimeError(f"target exists and is not directory/symlink (use --force): {target}")


def _remove_target(target: Path) -> None:
    if not target.exists() and not target.is_symlink():
        return
    if target.is_symlink() or target.is_file():
        target.unlink()
        return
    if target.is_dir():
        shutil.rmtree(target)
        return
    target.unlink()


def bind_path(
    *,
    source: Path,
    target: Path,
    force: bool,
    dry_run: bool,
) -> str:
    if not source.is_absolute():
        raise RuntimeError(f"source path must be absolute: {source}")
    if not source.exists():
        raise RuntimeError(f"source path does not exist: {source}")
    if not source.is_dir():
        raise RuntimeError(f"source path must be directory: {source}")

    _ensure_replaceable(target, force=force)

    if target.is_symlink():
        current = target.resolve()
        desired = source.resolve()
        if current == desired:
            return f"[skip] already linked: {target} -> {source}"

    if dry_run:
        return f"[plan] link {target} -> {source}"

    target.parent.mkdir(parents=True, exist_ok=True)
    _remove_target(target)
    target.symlink_to(source, target_is_directory=True)
    return f"[ok] linked: {target} -> {source}"


def main() -> int:
    args = parse_args()
    datasets_file = Path(args.datasets_file).expanduser()
    sources_file = Path(args.sources_file).expanduser()
    datasets_root = Path(args.datasets_root).expanduser()

    if not datasets_file.exists():
        raise SystemExit(f"datasets file not found: {datasets_file}")
    if not sources_file.exists():
        raise SystemExit(f"sources file not found: {sources_file}")

    specs = load_specs(datasets_file, datasets_root)
    sources = load_sources(sources_file)

    if not sources:
        print("[warn] no sources entries found")
        return 0

    errors = 0
    for item in sources:
        dataset_id = item["dataset_id"]
        spec = specs.get(dataset_id)
        if spec is None:
            print(f"[error] dataset_id not found in datasets config: {dataset_id}")
            errors += 1
            continue

        audio_src = Path(item["audio_src"]).expanduser()
        rttm_src = Path(item["rttm_src"]).expanduser()
        print(f"[info] dataset: {dataset_id}")
        try:
            print(
                bind_path(
                    source=audio_src,
                    target=spec.audio_target,
                    force=args.force,
                    dry_run=args.dry_run,
                )
            )
            print(
                bind_path(
                    source=rttm_src,
                    target=spec.rttm_target,
                    force=args.force,
                    dry_run=args.dry_run,
                )
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[error] {dataset_id}: {exc}")
            errors += 1

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
