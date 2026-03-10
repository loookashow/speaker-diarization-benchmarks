# diarization-benchmarks

CPU diarization benchmark service focused on reproducible, hardware-explicit comparisons.

It runs diarization systems on local datasets, computes standardized metrics, and stores job/report
state in FoxNose.

## Why this project exists

Diarization results are often hard to compare fairly because teams use different datasets, hardware,
threading, and reporting formats. This repository provides one execution protocol and one report
format so benchmark numbers are directly comparable and auditable.

## Practical value

- Runs competing diarization systems under the same CPU conditions
- Tracks both quality and speed, not only DER
- Captures machine/runtime context (`machine_fingerprint`) for reproducibility
- Produces structured JSON artifacts ready for publishing and regression tracking
- Works as an API service, so teams can automate benchmark pipelines

## Fairness controls

- Fixed thread count per run (`cpu_threads`)
- Optional warmup and multi-run execution (`warmup_files`, `n_runs`)
- Explicit dataset selection metadata (`dataset_manifest`)
- Standardized metric definitions and report schema across systems

## What it measures

- `DER` (`collar=0.25`, `skip_overlap=True`)
- Speaker count estimation quality (`exact`, `within_1`, `mae`, bucketed stats)
- CPU speed (`RTF`)
- Multi-run stability for DER/RTF (`mean`, `median`, `p95`, `std`)

## Key capabilities

- API key protected HTTP API
- Async jobs with progress and ETA
- Multiple systems per job (`diarize`, `pyannote`, custom command runners)
- Repeat-aware runs (`n_runs`) with fixed CPU threading (`cpu_threads`)
- Structured reports containing `metrics_summary`, `run_config`, `dataset_manifest`,
  `machine_fingerprint`, and per-item rows
- Fly.io profile for on-demand dedicated CPU machines

## How it works

- API and worker: [FastAPI](https://fastapi.tiangolo.com/)
- Persistent storage and query model: [FoxNose](https://foxnose.net)
- Runtime flow: create job -> run benchmark worker -> persist report summary + report items
- Deployment target: ready-to-run on Fly.io, but the same containerized service can be deployed on
  other practical targets with long-running containers and persistent storage (for example: VM,
  Render, Railway, or similar PaaS)

## API

All `/v1/*` routes require header `X-API-Key` (or custom `BENCH_API_KEY_HEADER`).

- `POST /v1/jobs` -> create job, returns `job_id`
- `GET /v1/jobs/{job_id}` -> status/progress/eta
- `GET /v1/jobs/{job_id}/report` -> final report JSON
- `GET /v1/reports` -> paginated report list
- `GET /v1/datasets` -> resolved dataset catalog

## Quick start (local Python)

```bash
cp .env.example .env
# fill BENCH_API_KEYS and FOXNOSE_* values

mkdir -p data/work data/reports data/venvs data/datasets
export BENCH_DATASETS_FILE=./config/datasets.open_track.json
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Quick start (Docker)

```bash
cp .env.example .env
# fill BENCH_API_KEYS and FOXNOSE_* values

mkdir -p data/work data/reports data/venvs data/datasets
# docker-compose already points to /app/config/datasets.docker.json (Open Track profile)
docker compose build
docker compose up -d
docker compose logs -f benchmark-api
```

By default, Docker mounts:

- `./data/datasets` -> `/data/datasets` (ro)
- `./data/work` -> `/app/data/work`
- `./data/reports` -> `/app/data/reports`
- `./data/venvs` -> `/app/data/venvs`

## Example job request

```json
{
  "dataset_id": "voxconverse",
  "n_runs": 3,
  "cpu_threads": 4,
  "warmup_files": 1,
  "systems": [
    {
      "system_id": "diarize",
      "version": "0.1.0",
      "params": {
        "min_speakers": 1,
        "max_speakers": 20
      }
    }
  ]
}
```

Notes:

- `n_runs`: optional, default `1`, max `20`
- `cpu_threads`: optional, defaults to all logical CPUs

## Reports

Each completed report includes:

- `metrics_summary`, `der_summary`, `rtf_summary`
- `speaker_count_table`
- `run_config` (resolved run/thread settings)
- `dataset_manifest` (selection hash + dataset stats)
- `machine_fingerprint` (OS/CPU/RAM/Python/package versions + thread pinning)

Per-item rows (`benchmark_report_items`) include `run_index`.

## Dataset configuration

Two ways to configure datasets:

1. via `BENCH_DATASETS_FILE` (JSON list)
2. via default layout under `BENCH_DATASETS_ROOT`

Example file: [`config/datasets.example.json`](config/datasets.example.json)
Open Track profile: [`config/datasets.open_track.json`](config/datasets.open_track.json)

Default expected dataset structure:

```text
<root>/<dataset_id>/
  audio/
  rttm/
```

Open Track dataset IDs configured in this repo:

- `voxconverse`
- `ami_ihm`
- `ami_sdm`
- `aishell4`
- `alimeeting_ch1`
- `msdwild`

Important:

- `configured` means dataset IDs are recognized by the API/catalog.
- It does not mean data is already downloaded and ready.

Auto-prepared in this repository (helper scripts/workflow):

- `voxconverse` (workflow mode requires `voxconverse_archive_url` or pre-populated paths)
- `aishell4`
- `alimeeting_ch1`

Manual preparation required:

- `ami_ihm`
- `ami_sdm`
- `msdwild`

Note:

- Service expects normalized local layout (`audio` + `rttm`) for each dataset.
- If original corpus layout differs, pre-convert or symlink into this structure.

Open Track helper scripts:

```bash
# 1) Create expected directory layout under ./data/datasets
./scripts/open_track_init_layout.sh

# 2) Check readiness (audio/rttm/matched files)
./scripts/open_track_check.py --strict

# 3) API smoke run (oracle_reference, limit_files=1)
BENCH_API_KEY="..." ./scripts/open_track_smoke_api.py \
  --base-url "http://127.0.0.1:8080"
```

Quick local download for publicly available Open Track corpora:

```bash
# AISHELL-4 (SLR111 test split)
mkdir -p data/downloads data/downloads/aishell4_test
curl -L --fail -o data/downloads/aishell4_test.tar.gz \
  "https://openslr.trmal.net/resources/111/test.tar.gz"
tar -xzf data/downloads/aishell4_test.tar.gz -C data/downloads/aishell4_test
ln -sfn "$(pwd)/data/downloads/aishell4_test/test/wav" data/datasets/aishell4/audio
ln -sfn "$(pwd)/data/downloads/aishell4_test/test/TextGrid" data/datasets/aishell4/rttm

# AliMeeting (SLR119 eval split, far) + TextGrid -> RTTM conversion
mkdir -p data/downloads data/downloads/alimeeting_eval
curl -L --fail -o data/downloads/alimeeting_eval.tar.gz \
  "https://speech-lab-share-data.oss-cn-shanghai.aliyuncs.com/AliMeeting/openlr/Eval_Ali.tar.gz"
tar -xzf data/downloads/alimeeting_eval.tar.gz -C data/downloads/alimeeting_eval
python ./scripts/open_track_prepare_alimeeting_eval.py --clean

# Verify
python3 ./scripts/open_track_check.py
```

Notes:

- `ami_ihm`, `ami_sdm`, and `msdwild` are not auto-downloaded in this repo.
- You can bind already downloaded corpora into local layout with
  `./scripts/open_track_bind_sources.py`.

If you currently have only VoxConverse:

```bash
export BENCH_DATASETS_FILE=./config/datasets.vox_only.json
BENCH_API_KEY="..." ./scripts/open_track_smoke_api.py \
  --base-url "http://127.0.0.1:8080" \
  --dataset-id voxconverse
```

## Fly.io deployment

`fly.toml` is configured for on-demand CPU benchmarking:

- auto-start on request
- auto-stop when idle
- `min_machines_running = 0`
- dedicated performance CPU
- persistent volume mounted at `/data` for datasets

Typical deploy flow:

```bash
flyctl apps create diarization-benchmarks
flyctl volumes create bench_data --region iad --size 120

flyctl secrets set \
  BENCH_API_KEYS="..." \
  FOXNOSE_BASE_URL="https://api.foxnose.net" \
  FOXNOSE_ENV_KEY="..." \
  FOXNOSE_AUTH_MODE="simple" \
  FOXNOSE_PUBLIC_KEY="..." \
  FOXNOSE_SECRET_KEY="..." \
  FOXNOSE_BENCH_JOBS_FOLDER="benchmark_jobs" \
  FOXNOSE_BENCH_REPORTS_FOLDER="benchmark_reports" \
  FOXNOSE_BENCH_REPORT_ITEMS_FOLDER="benchmark_report_items"

flyctl deploy --remote-only --config fly.toml
```

Prepare datasets on Fly volume via GitHub Actions:

- Workflow: `.github/workflows/prepare-fly-datasets.yml`
- Trigger: `Actions -> Prepare Fly Datasets -> Run workflow`
- Selectable datasets in this workflow:
  - `voxconverse`
  - `aishell4`
  - `alimeeting_ch1`
- This workflow does not prepare:
  - `ami_ihm`
  - `ami_sdm`
  - `msdwild`
- It will:
  - start a Fly machine if needed
  - download selected datasets to `/data/downloads`
  - prepare normalized layout in `/data/datasets`
  - run `open_track_check.py` on the remote machine
  - optionally stop the machine after completion

Required GitHub secret:

- `FLY_API_TOKEN`

Note:

- For `voxconverse`, you can pass `voxconverse_archive_url` input (zip/tar/tar.gz with wav+rttm),
  or pre-populate `/data/datasets/voxconverse/{audio,rttm}` manually.

## Publishing benchmark artifacts

Script: [`scripts/publish_benchmarks.py`](scripts/publish_benchmarks.py)

Generates:

- `benchmarks/runs/YYYY-MM-DD/<job_id>.json`
- `benchmarks/latest.json`
- `benchmarks/latest.md`
- `benchmarks/history.csv`

Example:

```bash
python scripts/publish_benchmarks.py \
  --base-url "http://127.0.0.1:8080" \
  --api-key "<service_api_key>" \
  --job-id "<job_id>"
```

By default, published artifacts are sanitized for public use:

- removes `source.base_url`
- removes `report.artifact_paths.run_dir`
- removes `report.machine_fingerprint.hostname`

If you explicitly need internal fields, run with:

```bash
python scripts/publish_benchmarks.py --include-internal-fields
```

## Security and public repo checklist

- Never commit `.env` or private keys
- Keep only `.env.example` with placeholders
- Store real credentials in CI/Fly secrets
- Keep `data/datasets` and `data/downloads` outside git history

Pre-publish scan:

```bash
./scripts/public_repo_check.sh
```

## License

This repository is licensed under
[Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/).
