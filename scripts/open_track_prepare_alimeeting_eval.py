#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Segment:
    start: float
    end: float
    speaker: str


def parse_textgrid_segments(path: Path) -> list[Segment]:
    segments: list[Segment] = []
    current_speaker: str | None = None
    in_interval = False
    start: float | None = None
    end: float | None = None
    text_value: str | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        speaker_match = re.match(r'name = "(.*)"$', line)
        if speaker_match:
            current_speaker = speaker_match.group(1).strip()
            continue

        if line.startswith("intervals ["):
            in_interval = True
            start = None
            end = None
            text_value = None
            continue

        if not in_interval:
            continue

        xmin_match = re.match(r"xmin = ([0-9.]+)$", line)
        if xmin_match and start is None:
            start = float(xmin_match.group(1))
            continue

        xmax_match = re.match(r"xmax = ([0-9.]+)$", line)
        if xmax_match:
            end = float(xmax_match.group(1))
            continue

        if line.startswith('text = "'):
            if line.endswith('"'):
                text_value = line[len('text = "') : -1]
            else:
                text_value = line[len('text = "') :]
            text_value = text_value.replace('\\"', '"').strip()

        if start is not None and end is not None and text_value is not None:
            in_interval = False
            if current_speaker and text_value and end > start:
                segments.append(Segment(start=start, end=end, speaker=current_speaker))

    return segments


def make_rttm_lines(file_id: str, segments: list[Segment]) -> list[str]:
    lines: list[str] = []
    for seg in segments:
        duration = seg.end - seg.start
        if duration <= 0:
            continue
        lines.append(
            f"SPEAKER {file_id} 1 {seg.start:.3f} {duration:.3f} <NA> <NA> {seg.speaker} <NA> <NA>"
        )
    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare AliMeeting Eval (far) into open-track local audio/rttm layout."
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path("data/downloads/alimeeting_eval/Eval_Ali/Eval_Ali_far"),
        help="Root with audio_dir + textgrid_dir",
    )
    parser.add_argument(
        "--dataset-root",
        type=Path,
        default=Path("data/datasets/alimeeting_ch1"),
        help="Target dataset root with audio/ and rttm/",
    )
    parser.add_argument(
        "--copy-audio",
        action="store_true",
        help="Copy audio files instead of creating symlinks.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove existing files in target audio/rttm before generating.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    audio_src = args.source_root / "audio_dir"
    tg_src = args.source_root / "textgrid_dir"
    audio_dst = args.dataset_root / "audio"
    rttm_dst = args.dataset_root / "rttm"

    if not audio_src.exists() or not tg_src.exists():
        raise RuntimeError(f"Expected source directories not found: {audio_src} and {tg_src}")

    audio_dst.mkdir(parents=True, exist_ok=True)
    rttm_dst.mkdir(parents=True, exist_ok=True)

    if args.clean:
        for path in audio_dst.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink()
        for path in rttm_dst.iterdir():
            if path.is_file() or path.is_symlink():
                path.unlink()

    prepared = 0
    missing_textgrid = 0
    empty_textgrid = 0

    for wav_path in sorted(audio_src.glob("*.wav")):
        audio_stem = wav_path.stem
        meeting_match = re.match(r"(.+)_MS\d+$", audio_stem)
        meeting_id = meeting_match.group(1) if meeting_match else audio_stem
        tg_path = tg_src / f"{meeting_id}.TextGrid"

        if not tg_path.exists():
            missing_textgrid += 1
            continue

        dst_audio_path = audio_dst / wav_path.name
        if dst_audio_path.exists() or dst_audio_path.is_symlink():
            dst_audio_path.unlink()

        if args.copy_audio:
            shutil.copy2(wav_path, dst_audio_path)
        else:
            dst_audio_path.symlink_to(wav_path.resolve())

        segments = parse_textgrid_segments(tg_path)
        rttm_lines = make_rttm_lines(audio_stem, segments)
        if not rttm_lines:
            empty_textgrid += 1
            if dst_audio_path.exists() or dst_audio_path.is_symlink():
                dst_audio_path.unlink()
            continue

        (rttm_dst / f"{audio_stem}.rttm").write_text(
            "\n".join(rttm_lines) + "\n",
            encoding="utf-8",
        )
        prepared += 1

    print(f"source_root={args.source_root}")
    print(f"dataset_root={args.dataset_root}")
    print(f"prepared={prepared}")
    print(f"missing_textgrid={missing_textgrid}")
    print(f"empty_textgrid={empty_textgrid}")

    return 0 if prepared > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
