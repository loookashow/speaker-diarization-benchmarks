#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATASETS_FILE="${1:-$ROOT_DIR/config/datasets.open_track.json}"
DATASETS_ROOT="${BENCH_DATASETS_ROOT:-$ROOT_DIR/data/datasets}"

if [[ ! -f "$DATASETS_FILE" ]]; then
  echo "[error] datasets config not found: $DATASETS_FILE" >&2
  exit 1
fi

echo "[info] using datasets file: $DATASETS_FILE"
echo "[info] using datasets root: $DATASETS_ROOT"

python - "$DATASETS_FILE" "$DATASETS_ROOT" <<'PY'
import json
import sys
from pathlib import Path

cfg_path = Path(sys.argv[1])
root = Path(sys.argv[2])
raw = json.loads(cfg_path.read_text(encoding="utf-8"))
datasets = raw.get("datasets", raw) if isinstance(raw, dict) else raw
if not isinstance(datasets, list):
    raise SystemExit("datasets config must be a list or {'datasets': [...]}")

created = []
for item in datasets:
    if not isinstance(item, dict):
        continue
    dataset_id = str(item.get("dataset_id", "")).strip()
    if not dataset_id:
        continue
    ds_root_raw = item.get("root", dataset_id)
    ds_root = Path(str(ds_root_raw)).expanduser()
    if not ds_root.is_absolute():
        ds_root = root / ds_root

    audio_dir = str(item.get("audio_dir", "audio"))
    rttm_dir = str(item.get("reference_rttm_dir", "rttm"))

    audio_root = ds_root / audio_dir
    rttm_root = ds_root / rttm_dir
    audio_root.mkdir(parents=True, exist_ok=True)
    rttm_root.mkdir(parents=True, exist_ok=True)
    created.append((dataset_id, str(audio_root), str(rttm_root)))

print(f"[ok] prepared {len(created)} datasets")
for dataset_id, audio_root, rttm_root in created:
    print(f"  - {dataset_id}:")
    print(f"    audio: {audio_root}")
    print(f"    rttm : {rttm_root}")
PY
