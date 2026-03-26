from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import asyncpg

from ledger.domain.aggregates.compliance_record import ComplianceRecord
from ledger.schema.events import StoredEvent
from ledger.temporal import load_stream_as_of, parse_as_of
from ledger.utils import ensure_dict


@dataclass(slots=True)
class ComplianceSnapshot:
    application_id: str
    as_of: str
    overall_verdict: str | None
    has_hard_block: bool
    failed_rule_ids: list[str]


class ComplianceAuditView:
    """
    Read API for compliance with temporal query support.

    - `get_compliance_at()` reconstructs deterministically from the compliance stream up to a timestamp.
    - `rebuild_from_scratch()` rebuilds the `compliance_audit` table using a shadow table + atomic swap.
    """

    def __init__(self, *, store: Any, pool: asyncpg.Pool) -> None:
        self.store = store
        self.pool = pool

    async def get_compliance_at(self, *, application_id: str, timestamp: str) -> ComplianceSnapshot:
        stream_id = f"compliance-{application_id}"
        events = await load_stream_as_of(self.store, stream_id, as_of=parse_as_of(timestamp))
        if not events:
            return ComplianceSnapshot(
                application_id=application_id,
                as_of=timestamp,
                overall_verdict=None,
                has_hard_block=False,
                failed_rule_ids=[],
            )
        stored = [StoredEvent.from_row(r) for r in events]
        agg = ComplianceRecord.rebuild(stored)  # type: ignore[arg-type]
        return ComplianceSnapshot(
            application_id=application_id,
            as_of=timestamp,
            overall_verdict=agg.overall_verdict,
            has_hard_block=bool(agg.has_hard_block),
            failed_rule_ids=list(agg.failed_rule_ids),
        )

    async def rebuild_from_scratch(self) -> None:
        tmp = f"compliance_audit_rebuild_{uuid.uuid4().hex}"
        async with self.pool.acquire() as conn:
            await conn.execute(f"CREATE TABLE {tmp} (LIKE compliance_audit INCLUDING ALL)")

        async for e in self.store.load_all(from_global_position=0, event_types=["ComplianceRuleFailed", "ComplianceCheckCompleted"], batch_size=500):
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await self._apply_to_table(conn, table=tmp, event=e)

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("ALTER TABLE compliance_audit RENAME TO compliance_audit_old")
            await conn.execute(f"ALTER TABLE {tmp} RENAME TO compliance_audit")
            await conn.execute("DROP TABLE compliance_audit_old")

    async def _apply_to_table(self, conn: asyncpg.Connection, *, table: str, event: dict[str, Any]) -> None:
        et = str(event.get("event_type") or "")
        #p = dict(event.get("payload") or {})
        p = ensure_dict(event.get("payload") or {})
        gp = int(event["global_position"])
        app_id = p.get("application_id")
        if not app_id:
            return

        if et == "ComplianceRuleFailed":
            rule_id = str(p.get("rule_id") or "")
            await conn.execute(
                f"""
                INSERT INTO {table}(application_id, failed_rule_ids, last_global_position, updated_at)
                VALUES ($1, ARRAY[$2]::TEXT[], $3, clock_timestamp())
                ON CONFLICT (application_id) DO UPDATE
                  SET failed_rule_ids = (
                        SELECT ARRAY(SELECT DISTINCT unnest({table}.failed_rule_ids || EXCLUDED.failed_rule_ids))
                    ),
                      last_global_position=GREATEST({table}.last_global_position, EXCLUDED.last_global_position),
                      updated_at=clock_timestamp()
                """,
                str(app_id),
                rule_id,
                gp,
            )
            return

        if et == "ComplianceCheckCompleted":
            await conn.execute(
                f"""
                INSERT INTO {table}(
                    application_id, overall_verdict, has_hard_block,
                    rules_passed, rules_failed, rules_noted,
                    last_global_position, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,clock_timestamp())
                ON CONFLICT (application_id) DO UPDATE
                  SET overall_verdict=EXCLUDED.overall_verdict,
                      has_hard_block=EXCLUDED.has_hard_block,
                      rules_passed=EXCLUDED.rules_passed,
                      rules_failed=EXCLUDED.rules_failed,
                      rules_noted=EXCLUDED.rules_noted,
                      last_global_position=GREATEST({table}.last_global_position, EXCLUDED.last_global_position),
                      updated_at=clock_timestamp()
                """,
                str(app_id),
                str(p.get("overall_verdict") or ""),
                bool(p.get("has_hard_block") or False),
                int(p.get("rules_passed") or 0),
                int(p.get("rules_failed") or 0),
                int(p.get("rules_noted") or 0),
                gp,
            )

