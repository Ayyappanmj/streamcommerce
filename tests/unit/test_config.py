import json

import pytest

from src.common.config import Settings
from src.common.exceptions import ConfigError, DataQualityError
from src.quality.quality_rules import DEFAULT_RULES, load_rules


def test_invalid_format_backend_and_port_rejected(tmp_path):
    with pytest.raises(ConfigError):
        Settings.for_data_dir(tmp_path, processed_format="xml")
    with pytest.raises(ConfigError):
        Settings.for_data_dir(tmp_path, metadata_backend="oracle")
    with pytest.raises(ConfigError):
        Settings.for_data_dir(tmp_path, postgres_port=70000)


def test_postgres_backend_requires_password(tmp_path):
    with pytest.raises(ConfigError):
        Settings.for_data_dir(tmp_path, metadata_backend="postgres")
    ok = Settings.for_data_dir(tmp_path, metadata_backend="postgres", postgres_password="x")
    assert "password=x" in ok.postgres_dsn


def test_from_env_reads_variables(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMCOMMERCE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PROCESSED_FORMAT", "csv")
    monkeypatch.setenv("LATE_EVENT_SECONDS", "120")
    s = Settings.from_env()
    assert s.data_dir == tmp_path and s.processed_format == "csv" and s.late_event_seconds == 120


def test_from_env_rejects_non_numeric(monkeypatch, tmp_path):
    monkeypatch.setenv("STREAMCOMMERCE_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("POSTGRES_PORT", "abc")
    with pytest.raises(ConfigError):
        Settings.from_env()


def test_quality_rules_can_be_disabled_or_replaced(tmp_path):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps({"disable": ["orders_total_non_negative"]}))
    names = [r["name"] for r in load_rules(path)["orders"]]
    assert "orders_total_non_negative" not in names and "orders_required_fields" in names
    assert any(r["name"] == "orders_total_non_negative" for r in DEFAULT_RULES["orders"])  # defaults untouched
    path.write_text("{not json")
    with pytest.raises(DataQualityError):
        load_rules(path)
