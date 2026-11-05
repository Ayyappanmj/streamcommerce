"""Load operational reference data (customers, products) into PostgreSQL with idempotent upserts."""
from __future__ import annotations

import logging

from src.common.config import Settings
from src.common.exceptions import MetadataError, MissingDependencyError
from src.metadata.repository import MetadataRepository
from src.processing.storage import read_table

log = logging.getLogger(__name__)

UPSERTS = {
    "ref_customers": ("customers", ["customer_id", "email", "country", "segment", "signup_ts"], "customer_id"),
    "ref_products": ("products", ["product_id", "product_name", "category", "unit_price", "created_ts"], "product_id"),
}


def load_reference_data(settings: Settings) -> dict[str, int]:
    """Upsert processed customers/products into PostgreSQL ``ref_*`` tables (requires METADATA_BACKEND=postgres)."""
    if settings.metadata_backend != "postgres":
        raise MetadataError("Reference-data load needs METADATA_BACKEND=postgres (see .env.example)")
    try:
        import psycopg
    except ImportError as exc:
        raise MissingDependencyError("psycopg is not installed (pip install 'psycopg[binary]')") from exc
    MetadataRepository(settings).init_schema()
    counts: dict[str, int] = {}
    with psycopg.connect(settings.postgres_dsn, connect_timeout=5) as conn:  # commits or rolls back
        with conn.cursor() as cur:
            for table, (dataset, cols, key) in UPSERTS.items():
                df = read_table(settings, dataset)
                if df is None:
                    raise MetadataError(f"Processed dataset {dataset!r} not found; run the batch pipeline first")
                rows = [tuple(None if v is None or v != v else v for v in r) for r in df[cols].itertuples(index=False)]
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != key)
                sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))}) "
                       f"ON CONFLICT ({key}) DO UPDATE SET {updates}")  # identifiers are fixed constants above
                cur.executemany(sql, rows)
                counts[table] = len(rows)
    return counts
