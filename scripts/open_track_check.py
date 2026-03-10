#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class DatasetCheck:
    dataset_id: str
    audio_root: Path
    rttm_root: Path
    audio_glob: str
    audio_files: int
    rttm_files: int
    matched_items: int
    status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "audio_root": str(self.audio_root),
            "rttm_root": str(self.rttm_root),
            "audio_glob": self.audio_glob,
            "audio_files": self.audio_files,
            "rttm_files": self.rttm_files,
            "matched_items": self.matched_items,
            "status": self.status,
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Open Track dataset layout and file readiness."
    )
    parser.add_argument(
        "--datasets-file",
        default="config/datasets.open_track.json",
        help="Path to datasets JSON config.",
    )
    parser.add_argument(
        "--datasets-root",
        default="data/datasets",
        help="Root for relative dataset paths.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero exit code if any dataset is not ready.",
    )
    return parser.parse_args()


def load_datasets(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    entries = raw.get("datasets", raw) if isinstance(raw, dict) else raw
    if not isinstance(entries, list):
        raise RuntimeError("datasets file must be a list or {'datasets': [...]}")
    return [item for item in entries if isinstance(item, dict)]


def resolve_dataset_roots(item: dict[str, Any], datasets_root: Path) -> tuple[Path, Path, str]:
    dataset_id = str(item["dataset_id"])
    root_raw = item.get("root", dataset_id)
    ds_root = Path(str(root_raw)).expanduser()
    if not ds_root.is_absolute():
        ds_root = datasets_root / ds_root

    audio_dir = str(item.get("audio_dir", "audio"))
    rttm_dir = str(item.get("reference_rttm_dir", "rttm"))
    audio_glob = str(item.get("audio_glob", "**/*.wav"))

    return ds_root / audio_dir, ds_root / rttm_dir, audio_glob


def check_dataset(item: dict[str, Any], datasets_root: Path) -> DatasetCheck:
    dataset_id = str(item["dataset_id"])
    audio_root, rttm_root, audio_glob = resolve_dataset_roots(item, datasets_root)

    if not audio_root.exists() or not rttm_root.exists():
        return DatasetCheck(
            dataset_id=dataset_id,
            audio_root=audio_root,
            rttm_root=rttm_root,
            audio_glob=audio_glob,
            audio_files=0,
            rttm_files=0,
            matched_items=0,
            status="missing_dirs",
        )

    audio_paths = sorted(path for path in audio_root.glob(audio_glob) if path.is_file())
    rttm_paths = sorted(path for path in rttm_root.glob("**/*.rttm") if path.is_file())
    rttm_stems = {path.stem for path in rttm_paths}
    matched = sum(1 for path in audio_paths if path.stem in rttm_stems)

    if matched > 0:
        status = "ready"
    elif audio_paths or rttm_paths:
        status = "no_matches"
    else:
        status = "empty"

    return DatasetCheck(
        dataset_id=dataset_id,
        audio_root=audio_root,
        rttm_root=rttm_root,
        audio_glob=audio_glob,
        audio_files=len(audio_paths),
        rttm_files=len(rttm_paths),
        matched_items=matched,
        status=status,
    )


def print_table(results: list[DatasetCheck]) -> None:
    headers = ["dataset_id", "status", "audio", "rttm", "matched"]
    rows = [
        [
            row.dataset_id,
            row.status,
            str(row.audio_files),
            str(row.rttm_files),
            str(row.matched_items),
        ]
        for row in results
    ]

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def fmt_line(cells: list[str]) -> str:
        return " | ".join(cell.ljust(widths[i]) for i, cell in enumerate(cells))

    print(fmt_line(headers))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(fmt_line(row))


def main() -> int:
    args = parse_args()
    datasets_file = Path(args.datasets_file).expanduser()
    datasets_root = Path(args.datasets_root).expanduser()

    if not datasets_file.exists():
        raise SystemExit(f"datasets file not found: {datasets_file}")

    datasets = load_datasets(datasets_file)
    results = [check_dataset(item, datasets_root) for item in datasets]

    if args.json:
        payload = {
            "datasets_file": str(datasets_file),
            "datasets_root": str(datasets_root),
            "results": [row.as_dict() for row in results],
        }
        print(json.dumps(payload, ensure_ascii=True, indent=2))
    else:
        print(f"datasets_file: {datasets_file}")
        print(f"datasets_root: {datasets_root}")
        print_table(results)

    if args.strict and any(row.status != "ready" for row in results):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
