FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    BENCH_DATASETS_ROOT=/data/datasets \
    BENCH_WORK_DIR=/app/data/work \
    BENCH_REPORTS_DIR=/app/data/reports \
    BENCH_VENV_ROOT=/app/data/venvs

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ffmpeg \
        libsndfile1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
COPY config ./config
COPY scripts ./scripts
COPY .env.example ./

RUN python -m pip install --upgrade pip \
    && pip install .

# Optional heavy built-ins for direct `diarize`/`pyannote_*` systems without custom commands.
ARG INSTALL_BUILTINS=0
RUN if [ "$INSTALL_BUILTINS" = "1" ]; then \
      pip install diarize pyannote.audio; \
    fi

RUN adduser --disabled-password --gecos "" appuser \
    && mkdir -p /app/data/work /app/data/reports /app/data/venvs /data/datasets \
    && chown -R appuser:appuser /app /data

USER appuser

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
