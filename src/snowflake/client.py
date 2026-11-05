"""Optional Snowflake connection. Never imported by the core pipeline; disabled unless explicitly used.

Authentication: key-pair (default, ``SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT``) using SNOWFLAKE_PRIVATE_KEY_PATH
(+ optional SNOWFLAKE_PRIVATE_KEY_PASSPHRASE). Other authenticators supported by the connector can be selected
with SNOWFLAKE_AUTHENTICATOR (e.g. ``externalbrowser``). Credentials are read from the environment only.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from src.common.exceptions import MissingDependencyError, SnowflakeConfigError

REQUIRED = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_SCHEMA"]


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str
    authenticator: str
    private_key_path: str | None
    private_key_passphrase: str | None

    def __repr__(self) -> str:  # never expose secrets
        return f"SnowflakeConfig(account={self.account!r}, user={self.user!r}, database={self.database!r})"


def load_config(env: dict[str, str] | None = None) -> SnowflakeConfig:
    """Read and validate Snowflake settings. Raises :class:`SnowflakeConfigError` listing missing variables."""
    env = dict(os.environ) if env is None else env
    missing = [k for k in REQUIRED if not env.get(k)]
    auth = env.get("SNOWFLAKE_AUTHENTICATOR", "SNOWFLAKE_JWT")
    key_path = env.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if auth.upper() == "SNOWFLAKE_JWT" and not key_path:
        missing.append("SNOWFLAKE_PRIVATE_KEY_PATH")
    if missing:
        raise SnowflakeConfigError(f"Snowflake is not configured. Missing environment variables: {missing}")
    return SnowflakeConfig(env["SNOWFLAKE_ACCOUNT"], env["SNOWFLAKE_USER"], env["SNOWFLAKE_ROLE"],
                           env["SNOWFLAKE_WAREHOUSE"], env["SNOWFLAKE_DATABASE"], env["SNOWFLAKE_SCHEMA"], auth,
                           key_path, env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None)


def _private_key_bytes(path: str, passphrase: str | None) -> bytes:
    try:
        from cryptography.hazmat.primitives import serialization
    except ImportError as exc:
        raise MissingDependencyError("cryptography is required for key-pair auth (installed with the connector)") from exc
    key = serialization.load_pem_private_key(Path(path).read_bytes(),
                                             password=passphrase.encode() if passphrase else None)
    return key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption())


def connect(config: SnowflakeConfig):
    """Open a Snowflake connection (caller must close it)."""
    try:
        import snowflake.connector
    except ImportError as exc:
        raise MissingDependencyError(
            "snowflake-connector-python is not installed (pip install -r requirements-snowflake.txt)") from exc
    kwargs: dict = {"account": config.account, "user": config.user, "role": config.role,
                    "warehouse": config.warehouse, "database": config.database, "schema": config.schema}
    if config.authenticator.upper() == "SNOWFLAKE_JWT":
        kwargs["private_key"] = _private_key_bytes(config.private_key_path or "", config.private_key_passphrase)
    else:
        kwargs["authenticator"] = config.authenticator
    return snowflake.connector.connect(**kwargs)
