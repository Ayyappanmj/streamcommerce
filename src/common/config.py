"""Central configuration. Everything is read from environment variables (or a .env file)."""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

from src.common.exceptions import ConfigError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
VALID_FORMATS = ("parquet", "csv")
VALID_BACKENDS = ("sqlite", "postgres")


def load_env_file(path: Path) -> None:
    """Load KEY=VALUE lines into os.environ without overriding existing variables."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if value.startswith(('"', "'")) and len(value) >= 2 and value[0] == value[-1]:
            val = value[1:-1]
        else:
            val = value.split("#")[0].strip()
        os.environ.setdefault(key.strip(), val)


@dataclass(frozen=True)
class Settings:
    """Runtime settings. Use :meth:`from_env` in applications and :meth:`for_data_dir` in tests."""

    data_dir: Path
    processed_format: str = "parquet"
    metadata_backend: str = "sqlite"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "streamcommerce"
    postgres_user: str = "streamcommerce"
    postgres_password: str = ""
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "orders.events"
    kafka_dlq_topic: str = "orders.events.dlq"
    kafka_group_id: str = "streamcommerce-consumer"
    late_event_seconds: int = 3600
    quality_rules_path: str = ""
    log_level: str = "INFO"

    # ---- derived paths -------------------------------------------------
    @property
    def incoming_dir(self) -> Path:
        return self.data_dir / "raw" / "incoming"

    @property
    def landed_dir(self) -> Path:
        return self.data_dir / "raw" / "landed"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    @property
    def summaries_dir(self) -> Path:
        return self.data_dir / "processed" / "summaries"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse" / "streamcommerce.duckdb"

    @property
    def events_db_path(self) -> Path:
        return self.data_dir / "processed" / "events.sqlite"

    @property
    def metadata_sqlite_path(self) -> Path:
        return self.data_dir / "metadata.db"

    @property
    def postgres_dsn(self) -> str:
        """libpq connection string. Contains the password: never log it."""
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )

    # ---- constructors --------------------------------------------------
    @classmethod
    def from_env(cls) -> Settings:
        load_env_file(PROJECT_ROOT / ".env")
        env = os.environ
        try:
            settings = cls(
                data_dir=Path(env.get("STREAMCOMMERCE_DATA_DIR", str(PROJECT_ROOT / "data"))),
                processed_format=env.get("PROCESSED_FORMAT", "parquet").lower(),
                metadata_backend=env.get("METADATA_BACKEND", "sqlite").lower(),
                postgres_host=env.get("POSTGRES_HOST", "localhost"),
                postgres_port=int(env.get("POSTGRES_PORT", "5432")),
                postgres_db=env.get("POSTGRES_DB", "streamcommerce"),
                postgres_user=env.get("POSTGRES_USER", "streamcommerce"),
                postgres_password=env.get("POSTGRES_PASSWORD", ""),
                kafka_bootstrap=env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                kafka_topic=env.get("KAFKA_TOPIC", "orders.events"),
                kafka_dlq_topic=env.get("KAFKA_DLQ_TOPIC", "orders.events.dlq"),
                kafka_group_id=env.get("KAFKA_GROUP_ID", "streamcommerce-consumer"),
                late_event_seconds=int(env.get("LATE_EVENT_SECONDS", "3600")),
                quality_rules_path=env.get("QUALITY_RULES_PATH", ""),
                log_level=env.get("LOG_LEVEL", "INFO").upper(),
            )
        except ValueError as exc:
            raise ConfigError(f"Invalid numeric configuration value: {exc}") from exc
        settings.validate()
        return settings

    @classmethod
    def for_data_dir(cls, data_dir: Path, **overrides: object) -> Settings:
        """Create validated settings rooted at ``data_dir`` (used by tests and scripts)."""
        base = cls(data_dir=Path(data_dir), processed_format="csv")
        settings = replace(base, **overrides)  # type: ignore[arg-type]
        settings.validate()
        return settings

    def validate(self) -> None:
        """Raise :class:`ConfigError` on invalid values."""
        if self.processed_format not in VALID_FORMATS:
            raise ConfigError(f"PROCESSED_FORMAT must be one of {VALID_FORMATS}, got {self.processed_format!r}")
        if self.metadata_backend not in VALID_BACKENDS:
            raise ConfigError(f"METADATA_BACKEND must be one of {VALID_BACKENDS}, got {self.metadata_backend!r}")
        if self.metadata_backend == "postgres" and not self.postgres_password:
            raise ConfigError("METADATA_BACKEND=postgres requires POSTGRES_PASSWORD to be set")
        if not 1 <= self.postgres_port <= 65535:
            raise ConfigError(f"POSTGRES_PORT out of range: {self.postgres_port}")
        if self.late_event_seconds < 0:
            raise ConfigError("LATE_EVENT_SECONDS must be >= 0")

    def ensure_dirs(self) -> None:
        for path in (self.incoming_dir, self.landed_dir, self.processed_dir, self.summaries_dir,
                     self.quarantine_dir, self.warehouse_path.parent):
            path.mkdir(parents=True, exist_ok=True)
