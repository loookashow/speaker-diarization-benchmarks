from __future__ import annotations

import hashlib
import json
import logging
import os
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SystemSpec:
    system_id: str
    version: str
    params: dict[str, Any]


class SystemExecutor:
    _THREAD_ENV_KEYS = (
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "BLIS_NUM_THREADS",
        "TORCH_NUM_THREADS",
    )

    def __init__(
        self,
        spec: SystemSpec,
        settings: Settings,
        cpu_threads: int | None = None,
    ) -> None:
        self.spec = spec
        self._settings = settings
        self._venv_python = Path(settings.python_bin)
        self._venv_bin_dir: Path | None = None
        self._builtin_diarize = None
        self._builtin_pyannote_pipeline = None
        self._cpu_threads = max(int(cpu_threads or (os.cpu_count() or 1)), 1)

    @property
    def system_ref(self) -> str:
        return f"{self.spec.system_id}@{self.spec.version}"

    def _venv_dir(self) -> Path:
        payload = {
            "system_id": self.spec.system_id,
            "version": self.spec.version,
            "packages": self._packages(),
        }
        digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
        safe_name = self.spec.system_id.replace("/", "_")
        return self._settings.venv_root / f"{safe_name}-{digest}"

    def _packages(self) -> list[str]:
        raw = self.spec.params.get("packages")
        if raw is None:
            if self.spec.system_id == "oracle_reference":
                return []
            return [f"{self.spec.system_id}=={self.spec.version}"]
        if isinstance(raw, str):
            return [raw]
        if isinstance(raw, list):
            return [str(item) for item in raw if str(item).strip()]
        raise RuntimeError(
            f"Invalid packages config for {self.system_ref}: expected string or list"
        )

    def prepare(self) -> None:
        self._apply_cpu_thread_limits()

        if self._prepare_builtin_runner():
            return

        if self.spec.system_id == "oracle_reference":
            return

        if bool(self.spec.params.get("skip_install", False)):
            return

        packages = self._packages()
        if not packages:
            return

        venv_dir = self._venv_dir()
        python_bin = venv_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        pip_bin = venv_dir / ("Scripts/pip" if os.name == "nt" else "bin/pip")
        marker_path = venv_dir / ".installed.json"

        desired_state = {
            "packages": packages,
            "python": str(self._settings.python_bin),
        }

        if marker_path.exists() and python_bin.exists():
            current_state = json.loads(marker_path.read_text(encoding="utf-8"))
            if current_state == desired_state:
                self._venv_python = python_bin
                self._venv_bin_dir = pip_bin.parent
                return

        if not python_bin.exists():
            subprocess.run(
                [self._settings.python_bin, "-m", "venv", str(venv_dir)],
                check=True,
                capture_output=True,
                text=True,
            )

        subprocess.run(
            [str(python_bin), "-m", "pip", "install", "--upgrade", "pip"],
            check=True,
            capture_output=True,
            text=True,
        )

        install_cmd = [str(python_bin), "-m", "pip", "install", *packages]
        subprocess.run(install_cmd, check=True, capture_output=True, text=True)

        marker_path.write_text(
            json.dumps(desired_state, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
        self._venv_python = python_bin
        self._venv_bin_dir = pip_bin.parent

    def _apply_cpu_thread_limits(self) -> None:
        value = str(self._cpu_threads)
        for key in self._THREAD_ENV_KEYS:
            os.environ[key] = value
        os.environ["TOKENIZERS_PARALLELISM"] = "false"

        try:
            import torch
        except Exception:
            return

        try:
            torch.set_num_threads(self._cpu_threads)
        except Exception:
            return

        if hasattr(torch, "set_num_interop_threads"):
            try:
                torch.set_num_interop_threads(max(1, min(self._cpu_threads, 2)))
            except RuntimeError:
                logger.debug(
                    "torch interop threads are already configured for %s; keeping current value",
                    self.system_ref,
                )

    def _prepare_builtin_runner(self) -> bool:
        if self.spec.system_id == "diarize" and self.spec.params.get("command") is None:
            from diarize import diarize as diarize_fn  # type: ignore[import-not-found]

            self._builtin_diarize = diarize_fn
            return True

        if self.spec.system_id.startswith("pyannote") and self.spec.params.get("command") is None:
            import torch
            from pyannote.audio import Pipeline  # type: ignore[import-not-found]

            model = str(self.spec.params.get("model") or "pyannote/speaker-diarization-3.1")
            token_env = str(self.spec.params.get("hf_token_env") or "HUGGING_FACE_TOKEN")
            token = os.getenv(token_env) or os.getenv("HF_TOKEN")
            if not token:
                raise RuntimeError(
                    f"Missing HuggingFace token for {self.system_ref}. Set {token_env} or HF_TOKEN."
                )

            pipeline = Pipeline.from_pretrained(model, token=token)
            pipeline.to(torch.device("cpu"))
            self._builtin_pyannote_pipeline = pipeline
            return True

        return False

    @staticmethod
    def _replace_tokens(template: str, values: dict[str, str]) -> str:
        value = template
        for key, replacement in values.items():
            value = value.replace("{" + key + "}", replacement)
        return value

    def _render_command(self, values: dict[str, str]) -> list[str]:
        raw = self.spec.params.get("command")
        if raw is None:
            raise RuntimeError(
                f"System {self.system_ref} requires params.command (or use oracle_reference)."
            )
        if isinstance(raw, str):
            parts = shlex.split(raw)
        elif isinstance(raw, list):
            parts = [str(item) for item in raw]
        else:
            raise RuntimeError(
                f"Invalid params.command for {self.system_ref}: expected string or list"
            )
        return [self._replace_tokens(part, values) for part in parts]

    def _render_env(self, values: dict[str, str]) -> dict[str, str]:
        env = dict(os.environ)
        for key, value in (self.spec.params.get("env") or {}).items():
            env[str(key)] = self._replace_tokens(str(value), values)

        for key in self._THREAD_ENV_KEYS:
            env[key] = str(self._cpu_threads)
        env["TOKENIZERS_PARALLELISM"] = "false"
        env["BENCH_CPU_THREADS"] = str(self._cpu_threads)

        env["PYTHON_BIN"] = str(self._venv_python)
        if self._venv_bin_dir:
            path_sep = os.pathsep
            env["PATH"] = f"{self._venv_bin_dir}{path_sep}{env.get('PATH', '')}"
        return env

    def run(
        self,
        *,
        audio_path: Path,
        output_rttm: Path,
        work_dir: Path,
        reference_rttm: Path,
    ) -> float:
        self._apply_cpu_thread_limits()
        output_rttm.parent.mkdir(parents=True, exist_ok=True)

        if self.spec.system_id == "oracle_reference":
            start = time.perf_counter()
            shutil.copyfile(reference_rttm, output_rttm)
            return time.perf_counter() - start

        if self._builtin_diarize is not None:
            start = time.perf_counter()
            kwargs: dict[str, Any] = {}
            if "min_speakers" in self.spec.params:
                kwargs["min_speakers"] = int(self.spec.params["min_speakers"])
            if "max_speakers" in self.spec.params:
                kwargs["max_speakers"] = int(self.spec.params["max_speakers"])
            if "num_speakers" in self.spec.params:
                raw = self.spec.params["num_speakers"]
                kwargs["num_speakers"] = None if raw is None else int(raw)

            result = self._builtin_diarize(str(audio_path), **kwargs)
            result.to_rttm(output_rttm)
            return time.perf_counter() - start

        if self._builtin_pyannote_pipeline is not None:
            start = time.perf_counter()
            output = self._builtin_pyannote_pipeline(str(audio_path))
            diarization = (
                output.speaker_diarization if hasattr(output, "speaker_diarization") else output
            )

            file_id = audio_path.stem
            lines: list[str] = []
            for turn, _track, speaker in diarization.itertracks(yield_label=True):
                duration = float(turn.end) - float(turn.start)
                lines.append(
                    f"SPEAKER {file_id} 1 {float(turn.start):.6f} {duration:.6f} "
                    f"<NA> <NA> {speaker} <NA> <NA>"
                )
            text = "\n".join(lines)
            if text:
                text += "\n"
            output_rttm.write_text(text, encoding="utf-8")
            return time.perf_counter() - start

        values = {
            "audio_path": str(audio_path),
            "output_rttm": str(output_rttm),
            "work_dir": str(work_dir),
            "reference_rttm": str(reference_rttm),
            "system_id": self.spec.system_id,
            "version": self.spec.version,
            "python_bin": str(self._venv_python),
            "system_ref": self.system_ref,
        }

        command = self._render_command(values)
        env = self._render_env(values)
        cwd_raw = self.spec.params.get("cwd")
        cwd = Path(self._replace_tokens(str(cwd_raw), values)).expanduser() if cwd_raw else None

        start = time.perf_counter()
        proc = subprocess.run(
            command,
            cwd=str(cwd) if cwd else None,
            env=env,
            capture_output=True,
            text=True,
        )
        elapsed = time.perf_counter() - start

        if proc.returncode != 0:
            raise RuntimeError(
                "System command failed for "
                f"{self.system_ref}. code={proc.returncode}; "
                f"stdout={proc.stdout[-1000:]}; stderr={proc.stderr[-1000:]}"
            )

        if not output_rttm.exists():
            raise RuntimeError(
                f"System {self.system_ref} completed but output RTTM missing: {output_rttm}"
            )

        return elapsed
