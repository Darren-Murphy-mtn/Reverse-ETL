from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "pipeline.yml"


@dataclass(frozen=True)
class ReverseEtlConfig:
    marker: str
    stale_label: str
    max_snapshot_age_hours: float
    write_delay_seconds: float


@dataclass(frozen=True)
class Config:
    source_repos: list[str]
    sandbox_repo: str
    window_days: int
    raw_json: Path
    raw_parquet: Path
    warehouse: Path
    sync_logs: Path
    reverse_etl: ReverseEtlConfig

    @property
    def extract_repos(self) -> list[str]:
        return [*self.source_repos, self.sandbox_repo]


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else ROOT / path


def load_config(path: Path = DEFAULT_CONFIG) -> Config:
    load_dotenv(ROOT / ".env")
    raw = yaml.safe_load(path.read_text())
    paths = raw["paths"]
    warehouse = os.environ.get("DUCKDB_PATH") or paths["warehouse"]
    return Config(
        source_repos=list(raw["source_repos"]),
        sandbox_repo=raw["sandbox_repo"],
        window_days=int(raw["window_days"]),
        raw_json=_resolve(paths["raw_json"]),
        raw_parquet=_resolve(paths["raw_parquet"]),
        warehouse=_resolve(warehouse),
        sync_logs=_resolve(paths["sync_logs"]),
        reverse_etl=ReverseEtlConfig(**raw["reverse_etl"]),
    )


def env_token(name: str) -> str | None:
    load_dotenv(ROOT / ".env")
    value = os.environ.get(name, "").strip()
    return value or None
