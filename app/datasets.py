from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import Settings
from app.schemas import DatasetInfo


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    root: Path
    audio_dir: str
    reference_rttm_dir: str
    audio_glob: str

    @property
    def audio_root(self) -> Path:
        return self.root / self.audio_dir

    @property
    def reference_root(self) -> Path:
        return self.root / self.reference_rttm_dir


@dataclass(frozen=True)
class DatasetItem:
    audio_id: str
    audio_path: Path
    reference_rttm_path: Path


class DatasetCatalog:
    def __init__(self, specs: dict[str, DatasetSpec]) -> None:
        self._specs = specs

    @classmethod
    def from_settings(cls, settings: Settings) -> "DatasetCatalog":
        specs = cls._load_specs(settings)
        return cls(specs)

    @staticmethod
    def _load_specs(settings: Settings) -> dict[str, DatasetSpec]:
        if settings.datasets_file and settings.datasets_file.exists():
            raw = json.loads(settings.datasets_file.read_text(encoding="utf-8"))
            entries = raw.get("datasets", raw) if isinstance(raw, dict) else raw
            if not isinstance(entries, list):
                raise RuntimeError(
                    'BENCH_DATASETS_FILE must contain a list or {"datasets": [...]}.'
                )
            specs = [DatasetCatalog._parse_spec(item, settings) for item in entries]
            return {spec.dataset_id: spec for spec in specs}

        local_project_vox_root = Path(__file__).resolve().parents[1] / "data/datasets/voxconverse"
        if local_project_vox_root.exists():
            default_vox_root: str | Path = local_project_vox_root
            default_vox_audio_dir = "audio"
            default_vox_rttm_dir = "rttm"
        else:
            default_vox_root = "voxconverse"
            default_vox_audio_dir = "audio"
            default_vox_rttm_dir = "rttm"

        defaults = [
            {
                "dataset_id": "voxconverse",
                "root": default_vox_root,
                "audio_dir": default_vox_audio_dir,
                "reference_rttm_dir": default_vox_rttm_dir,
                "audio_glob": "**/*.wav",
            },
            {
                "dataset_id": "ami_ihm",
                "root": "ami_ihm",
                "audio_dir": "audio",
                "reference_rttm_dir": "rttm",
                "audio_glob": "**/*.wav",
            },
            {
                "dataset_id": "ami_sdm",
                "root": "ami_sdm",
                "audio_dir": "audio",
                "reference_rttm_dir": "rttm",
                "audio_glob": "**/*.wav",
            },
            {
                "dataset_id": "aishell4",
                "root": "aishell4",
                "audio_dir": "audio",
                "reference_rttm_dir": "rttm",
                "audio_glob": "**/*.flac",
            },
            {
                "dataset_id": "alimeeting_ch1",
                "root": "alimeeting_ch1",
                "audio_dir": "audio",
                "reference_rttm_dir": "rttm",
                "audio_glob": "**/*.wav",
            },
            {
                "dataset_id": "msdwild",
                "root": "msdwild",
                "audio_dir": "audio",
                "reference_rttm_dir": "rttm",
                "audio_glob": "**/*.wav",
            },
        ]
        specs = [DatasetCatalog._parse_spec(item, settings) for item in defaults]
        return {spec.dataset_id: spec for spec in specs}

    @staticmethod
    def _parse_spec(raw: dict[str, Any], settings: Settings) -> DatasetSpec:
        dataset_id = str(raw["dataset_id"])
        root_raw = raw.get("root", dataset_id)
        root = Path(str(root_raw)).expanduser()
        if not root.is_absolute():
            root = settings.datasets_root / root

        return DatasetSpec(
            dataset_id=dataset_id,
            root=root,
            audio_dir=str(raw.get("audio_dir", "audio")),
            reference_rttm_dir=str(raw.get("reference_rttm_dir", "rttm")),
            audio_glob=str(raw.get("audio_glob", "**/*.wav")),
        )

    def get(self, dataset_id: str) -> DatasetSpec:
        try:
            return self._specs[dataset_id]
        except KeyError as exc:
            raise KeyError(f"Unknown dataset_id: {dataset_id}") from exc

    def list_infos(self) -> list[DatasetInfo]:
        infos: list[DatasetInfo] = []
        for dataset_id in sorted(self._specs):
            spec = self._specs[dataset_id]
            infos.append(
                DatasetInfo(
                    dataset_id=spec.dataset_id,
                    root=str(spec.root),
                    audio_dir=spec.audio_dir,
                    reference_rttm_dir=spec.reference_rttm_dir,
                    audio_glob=spec.audio_glob,
                )
            )
        return infos

    def collect_items(self, dataset_id: str, limit_files: int | None = None) -> list[DatasetItem]:
        spec = self.get(dataset_id)
        if not spec.audio_root.exists():
            raise RuntimeError(f"Audio dir does not exist: {spec.audio_root}")
        if not spec.reference_root.exists():
            raise RuntimeError(f"RTTM dir does not exist: {spec.reference_root}")

        audio_paths = sorted(
            path for path in spec.audio_root.glob(spec.audio_glob) if path.is_file()
        )
        if limit_files is not None:
            audio_paths = audio_paths[:limit_files]

        items: list[DatasetItem] = []
        for audio_path in audio_paths:
            reference = spec.reference_root / f"{audio_path.stem}.rttm"
            if not reference.exists():
                continue
            items.append(
                DatasetItem(
                    audio_id=audio_path.stem,
                    audio_path=audio_path,
                    reference_rttm_path=reference,
                )
            )

        if not items:
            raise RuntimeError(
                f"No dataset items with matching RTTM found for dataset_id={dataset_id}"
            )
        return items
