# FoxNose Schema Structure for Diarization Benchmarks

This structure is optimized for your flow:
- submit benchmark job
- poll status/progress/ETA
- fetch final JSON report
- list historical reports

It also accounts for FoxNose revision payload limits by splitting summary and per-file details.

## 1) Collection: `benchmark_jobs`
Use this as the job queue + status source.

### Fields
| Field | Type | Required | Searchable | Notes |
|---|---|---:|---:|---|
| `status` | `string` | yes | yes | enum: `queued`, `running`, `completed`, `failed`, `canceled` |
| `progress_percent` | `number` | yes | no | 0..100 |
| `eta_seconds` | `integer` | no | no | nullable |
| `current_step` | `string` | no | no | short status text |
| `submitted_at` | `datetime` | yes | yes | job create timestamp |
| `started_at` | `datetime` | no | yes | when worker picked job |
| `finished_at` | `datetime` | no | yes | when job ended |
| `dataset_id` | `string` | yes | yes | e.g. `voxconverse_dev` |
| `request` | `json` | yes | no | full run request payload |
| `system_matrix` | `json` | yes | no | normalized systems list for run |
| `report_key` | `reference` -> `benchmark_reports` | no | yes | set on completion |
| `error` | `text` | no | no | failure reason |
| `worker_info` | `json` | no | no | host, CPU, runtime versions |

### `request` JSON shape
```json
{
  "dataset_id": "voxconverse_dev",
  "systems": [
    {"system_id": "diarize", "version": "0.1.0", "params": {}},
    {"system_id": "pyannote_community_1", "version": "3.3.2", "params": {"hf_token_env": "HF_TOKEN"}}
  ],
  "limit_files": null,
  "warmup_files": 1,
  "metadata": {
    "label": "cpu-fly-bench",
    "requested_by": "alex"
  }
}
```

## 2) Collection: `benchmark_reports`
Use this for report summary per job (small and fast to fetch/list).

### Fields
| Field | Type | Required | Searchable | Notes |
|---|---|---:|---:|---|
| `job_key` | `reference` -> `benchmark_jobs` | yes | yes | 1:1 link |
| `status` | `string` | yes | yes | `completed` or `failed` |
| `created_at` | `datetime` | yes | yes | report creation time |
| `dataset_id` | `string` | yes | yes | duplicated for filtering |
| `systems` | `json` | yes | no | system/version list |
| `metrics_summary` | `json` | yes | no | compact leaderboard metrics |
| `speaker_count_table` | `json` | yes | no | bucket table (1,2,3,4,5,6-7,8+) |
| `der_summary` | `json` | yes | no | weighted + median DER by system |
| `rtf_summary` | `json` | yes | no | mean + median RTF by system |
| `run_config` | `json` | yes | no | resolved `n_runs`/threads/warmup/limit |
| `dataset_manifest` | `json` | no | no | selected files/hash and dataset stats |
| `machine_fingerprint` | `json` | yes | no | OS/CPU/RAM/runtime/package snapshot |
| `items_count` | `integer` | yes | no | number of rows in `benchmark_report_items` |
| `artifact_paths` | `json` | no | no | local files on Fly volume (optional) |
| `error` | `text` | no | no | present when status=`failed` |

### `metrics_summary` JSON shape (example)
```json
{
  "winner_by_der": "diarize@0.1.0",
  "winner_by_rtf": "diarize@0.1.0",
  "systems": [
    {
      "system": "diarize@0.1.0",
      "der_weighted": 0.108,
      "der_median": 0.037,
      "rtf_mean": 0.12,
      "rtf_median": 0.12,
      "speaker_count_exact": 0.51,
      "speaker_count_within_1": 0.81
    }
  ]
}
```

## 3) Collection: `benchmark_report_items`
Per-file/per-system metrics. This avoids 1MB report payload overflow.

### Fields
| Field | Type | Required | Searchable | Notes |
|---|---|---:|---:|---|
| `report_key` | `reference` -> `benchmark_reports` | yes | yes | parent report |
| `job_key` | `reference` -> `benchmark_jobs` | yes | yes | shortcut filter |
| `dataset_id` | `string` | yes | yes | filter by dataset |
| `system_id` | `string` | yes | yes | e.g. `diarize` |
| `system_version` | `string` | yes | yes | e.g. `0.1.0` |
| `audio_id` | `string` | yes | yes | file stem/key |
| `run_index` | `integer` | yes | no | benchmark run index (1..N) |
| `audio_seconds` | `number` | yes | no | audio duration |
| `processing_seconds` | `number` | yes | no | wall-clock processing time |
| `rtf` | `number` | yes | no | processing_seconds / audio_seconds |
| `der` | `number` | no | no | per-file DER if reference exists |
| `gt_speakers` | `integer` | no | no | from reference RTTM |
| `pred_speakers` | `integer` | no | no | predicted |
| `abs_count_error` | `integer` | no | no | `abs(gt-pred)` |
| `bucket` | `string` | no | yes | one of `1`,`2`,`3`,`4`,`5`,`6-7`,`8+` |
| `extra` | `json` | no | no | debug stats |

## Endpoint mapping after Flux API is generated
With folders connected to one Flux API prefix (e.g. `bench-v1`):

1. Submit job:
- Management API: create resource in `benchmark_jobs` with `status="queued"`
- returned `resource.key` is your `jobId`

2. Poll status:
- Flux API: `GET /bench-v1/benchmark_jobs/{jobId}`
- read `status`, `progress_percent`, `eta_seconds`, `current_step`

3. Get report by job:
- read `report_key` from job
- Flux API: `GET /bench-v1/benchmark_reports/{report_key}`

4. List reports:
- Flux API: `GET /bench-v1/benchmark_reports?limit=...`
- for details, query `benchmark_report_items` by `report_key`

## Notes for your benchmark logic
- Keep datasets on Fly volume (local disk on server) and only write metadata/results to FoxNose.
- For large runs, store heavy artifacts (RTTM dumps, per-file raw traces) on disk and keep paths in `artifact_paths`.
- For speaker-count table in report summary, precompute buckets exactly as: `1`, `2`, `3`, `4`, `5`, `6-7`, `8+`.
