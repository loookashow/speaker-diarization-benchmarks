#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import tarfile
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare VoxConverse dataset layout (audio/rttm) from extracted files."
    )
    parser.add_argument(
        "--archive",
        type=Path,
        default=None,
        help="Optional archive (.zip/.tar/.tar.gz) to extract first.",
    )
    parser.add_argument(
        "--extract-root",
        type=Path,
        default=Path("data/downloads/voxconverse_raw"),
        help="Directory used as extraction root and auto-detection source.",
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data/datasets/voxconverse"),
        help="Target dataset root (creates audio and rttm symlinks).",
    )
    parser.add_argument(
        "--audio-src",
        type=Path,
        default=None,
        help="Optional explicit audio source directory.",
    )
    parser.add_argument(
        "--rttm-src",
        type=Path,
        default=None,
        help="Optional explicit RTTM source directory.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Replace existing dataset-root/audio and dataset-root/rttm.",
    )
    return parser.parse_args()


def _extract_archive(archive: Path, extract_root: Path) -> None:
    if not archive.exists():
        raise RuntimeError(f"Archive does not exist: {archive}")
    extract_root.mkdir(parents=True, exist_ok=True)

    if tarfile.is_tarfile(archive):
        with tarfile.open(archive) as tf:
            tf.extractall(extract_root)
        return

    if zipfile.is_zipfile(archive):
        with zipfile.ZipFile(archive) as zf:
            zf.extractall(extract_root)
        return

    raise RuntimeError(f"Unsupported archive format: {archive}")


def _count_files(root: Path, suffix: str) -> int:
    return sum(1 for p in root.rglob(f"*{suffix}") if p.is_file())


def _pick_best_dir(root: Path, suffix: str, preferred_tokens: tuple[str, ...]) -> Path | None:
    best: Path | None = None
    best_score = -1

    for candidate in [p for p in root.rglob("*") if p.is_dir()]:
        count = _count_files(candidate, suffix)
        if count <= 0:
            continue

        name = candidate.name.lower()
        bonus = 0
        for token in preferred_tokens:
            if token in name:
                bonus += 1000000

        score = bonus + count
        if score > best_score:
            best_score = score
            best = candidate

    return best


def _replace_link(link: Path, target: Path) -> None:
    if link.is_symlink() or link.is_file():
        link.unlink()
    elif link.is_dir():
        shutil.rmtree(link)
    link.symlink_to(target.resolve())


def main() -> int:
    args = parse_args()
    extract_root = args.extract_root.expanduser().resolve()
    dataset_root = args.dataset_root.expanduser().resolve()

    if args.archive is not None:
        archive = args.archive.expanduser().resolve()
        if args.clean and extract_root.exists():
            shutil.rmtree(extract_root)
        _extract_archive(archive, extract_root)

    if not extract_root.exists():
        raise RuntimeError(
            "Source root does not exist: "
            f"{extract_root}. Provide --archive or existing --extract-root."
        )

    audio_src = args.audio_src.expanduser().resolve() if args.audio_src else None
    rttm_src = args.rttm_src.expanduser().resolve() if args.rttm_src else None

    if audio_src is None:
        audio_src = _pick_best_dir(extract_root, ".wav", ("audio", "wav", "dev"))
    if rttm_src is None:
        rttm_src = _pick_best_dir(extract_root, ".rttm", ("rttm", "annotations", "dev"))

    if audio_src is None or rttm_src is None:
        raise RuntimeError(
            "Could not auto-detect audio/rttm directories. "
            "Pass --audio-src and --rttm-src explicitly."
        )
    if not audio_src.exists() or not rttm_src.exists():
        raise RuntimeError(f"audio/rttm source dirs do not exist: {audio_src}, {rttm_src}")

    dataset_root.mkdir(parents=True, exist_ok=True)
    audio_link = dataset_root / "audio"
    rttm_link = dataset_root / "rttm"

    if args.clean:
        _replace_link(audio_link, audio_src)
        _replace_link(rttm_link, rttm_src)
    else:
        if not audio_link.exists():
            _replace_link(audio_link, audio_src)
        if not rttm_link.exists():
            _replace_link(rttm_link, rttm_src)

    print(f"dataset_root={dataset_root}")
    print(f"audio_src={audio_src} (wav={_count_files(audio_src, '.wav')})")
    print(f"rttm_src={rttm_src} (rttm={_count_files(rttm_src, '.rttm')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
