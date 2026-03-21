"""
init_db.py — idempotent PostgreSQL initializer (Phase 0)

Usage:
  python init_db.py
  DATABASE_URL=postgresql://localhost/apex_ledger python init_db.py
"""
from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

import asyncpg


ADVISORY_LOCK_KEY = 814_288_719  # stable "ledger" lock id


async def init_db(db_url: str, schema_path: Path) -> None:
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute("SELECT pg_advisory_lock($1)", ADVISORY_LOCK_KEY)
        await conn.execute(schema_sql)
    finally:
        try:
            await conn.execute("SELECT pg_advisory_unlock($1)", ADVISORY_LOCK_KEY)
        finally:
            await conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-url",
        default=os.environ.get("DATABASE_URL") or os.environ.get("DB_URL") or "postgresql://localhost/apex_ledger",
    )
    parser.add_argument("--schema", default="schema.sql")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    schema_path = Path(args.schema)
    if not schema_path.exists():
        raise SystemExit(f"schema file not found: {schema_path}")
    asyncio.run(init_db(args.db_url, schema_path))


if __name__ == "__main__":
    main()

