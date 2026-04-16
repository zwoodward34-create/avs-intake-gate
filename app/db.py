from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class IntakeRow:
    id: int
    created_at: str
    updated_at: str
    inquiry_date: Optional[str]
    project_name: str
    client_name: Optional[str]
    architect_name: Optional[str]
    lead_contact: Optional[str]
    location_region: Optional[str]
    submitted_by: Optional[str]
    status: str
    recommendation: Optional[str]
    recommendation_reason: Optional[str]
    red_flags_json: str
    red_flag_counts_json: str
    answers_json: str
    mo_decision: Optional[str]
    mo_notes: Optional[str]
    mo_conditions: Optional[str]
    mo_reviewed_at: Optional[str]
    mo_fee_decision: Optional[str] = None   # ACCEPTED | DECLINED | OVERRIDE
    mo_fee_override: Optional[str] = None   # numeric string when OVERRIDE
    proposal_checklist_json: Optional[str] = None
    proposal_completed_at: Optional[str] = None

    @property
    def red_flags(self) -> list[dict[str, Any]]:
        return json.loads(self.red_flags_json or "[]")

    @property
    def red_flag_counts(self) -> dict[str, Any]:
        return json.loads(self.red_flag_counts_json or "{}")

    @property
    def answers(self) -> dict[str, Any]:
        return json.loads(self.answers_json or "{}")

    @property
    def proposal_checklist(self) -> dict[str, bool]:
        return json.loads(self.proposal_checklist_json or "{}")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS intakes (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,

              inquiry_date TEXT,
              project_name TEXT NOT NULL,
              client_name TEXT,
              architect_name TEXT,
              lead_contact TEXT,
              location_region TEXT,
              submitted_by TEXT,

              status TEXT NOT NULL,
              recommendation TEXT,
              recommendation_reason TEXT,
              red_flags_json TEXT NOT NULL,
              red_flag_counts_json TEXT NOT NULL,
              answers_json TEXT NOT NULL,

              mo_decision TEXT,
              mo_notes TEXT,
              mo_conditions TEXT,
              mo_reviewed_at TEXT,
              mo_fee_decision TEXT,
              mo_fee_override TEXT,
              proposal_checklist_json TEXT,
              proposal_completed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_intakes_updated_at ON intakes(updated_at);
            CREATE INDEX IF NOT EXISTS idx_intakes_status ON intakes(status);
            """
        )
        # Migrate: add columns if they don't exist yet
        cols = [r[1] for r in conn.execute("PRAGMA table_info(intakes)").fetchall()]
        if "proposal_checklist_json" not in cols:
            conn.execute("ALTER TABLE intakes ADD COLUMN proposal_checklist_json TEXT")
        if "proposal_completed_at" not in cols:
            conn.execute("ALTER TABLE intakes ADD COLUMN proposal_completed_at TEXT")
        if "mo_fee_decision" not in cols:
            conn.execute("ALTER TABLE intakes ADD COLUMN mo_fee_decision TEXT")
        if "mo_fee_override" not in cols:
            conn.execute("ALTER TABLE intakes ADD COLUMN mo_fee_override TEXT")


def list_intakes(db_path: Path, *, status: Optional[str] = None) -> list[IntakeRow]:
    with connect(db_path) as conn:
        if status:
            rows = conn.execute(
                "SELECT * FROM intakes WHERE status = ? ORDER BY updated_at DESC, id DESC",
                (status,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM intakes ORDER BY updated_at DESC, id DESC"
            ).fetchall()
    return [IntakeRow(**dict(r)) for r in rows]


def list_pending_mo(db_path: Path) -> list[IntakeRow]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM intakes WHERE status = 'PENDING_MO_REVIEW' ORDER BY created_at ASC, id ASC"
        ).fetchall()
    return [IntakeRow(**dict(r)) for r in rows]


def get_intake(db_path: Path, intake_id: int) -> Optional[IntakeRow]:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM intakes WHERE id = ?", (intake_id,)).fetchone()
    return IntakeRow(**dict(row)) if row else None


def create_intake(
    db_path: Path,
    *,
    inquiry_date: Optional[str],
    project_name: str,
    client_name: Optional[str],
    architect_name: Optional[str],
    lead_contact: Optional[str],
    location_region: Optional[str],
    submitted_by: Optional[str],
    status: str,
    recommendation: Optional[str],
    recommendation_reason: Optional[str],
    red_flags: list[dict[str, Any]],
    red_flag_counts: dict[str, Any],
    answers: dict[str, Any],
) -> int:
    created_at = _utc_now_iso()
    updated_at = created_at
    with connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO intakes (
              created_at, updated_at,
              inquiry_date, project_name, client_name, architect_name, lead_contact, location_region, submitted_by,
              status, recommendation, recommendation_reason,
              red_flags_json, red_flag_counts_json, answers_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                created_at,
                updated_at,
                inquiry_date,
                project_name,
                client_name,
                architect_name,
                lead_contact,
                location_region,
                submitted_by,
                status,
                recommendation,
                recommendation_reason,
                json.dumps(red_flags, ensure_ascii=False),
                json.dumps(red_flag_counts, ensure_ascii=False),
                json.dumps(answers, ensure_ascii=False),
            ),
        )
        intake_id = int(cur.lastrowid)
    return intake_id


def update_intake(
    db_path: Path,
    intake_id: int,
    *,
    inquiry_date: Optional[str],
    project_name: str,
    client_name: Optional[str],
    architect_name: Optional[str],
    lead_contact: Optional[str],
    location_region: Optional[str],
    submitted_by: Optional[str],
    status: str,
    recommendation: Optional[str],
    recommendation_reason: Optional[str],
    red_flags: list[dict[str, Any]],
    red_flag_counts: dict[str, Any],
    answers: dict[str, Any],
) -> None:
    updated_at = _utc_now_iso()
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE intakes
            SET
              updated_at = ?,
              inquiry_date = ?,
              project_name = ?,
              client_name = ?,
              architect_name = ?,
              lead_contact = ?,
              location_region = ?,
              submitted_by = ?,
              status = ?,
              recommendation = ?,
              recommendation_reason = ?,
              red_flags_json = ?,
              red_flag_counts_json = ?,
              answers_json = ?
            WHERE id = ?
            """,
            (
                updated_at,
                inquiry_date,
                project_name,
                client_name,
                architect_name,
                lead_contact,
                location_region,
                submitted_by,
                status,
                recommendation,
                recommendation_reason,
                json.dumps(red_flags, ensure_ascii=False),
                json.dumps(red_flag_counts, ensure_ascii=False),
                json.dumps(answers, ensure_ascii=False),
                intake_id,
            ),
        )


CHECKLIST_KEYS = [
    "rfp_extracted",
    "project_classified",
    "complexity_assessed",
    "fee_estimated",
    "scope_items_determined",
    "proposal_drafted",
    "proposal_sent",
]


def set_proposal_checklist(
    db_path: Path,
    intake_id: int,
    checklist: dict[str, bool],
) -> None:
    updated_at = _utc_now_iso()
    all_done = all(checklist.get(k, False) for k in CHECKLIST_KEYS)
    # Only set proposal_completed_at when all boxes first become checked;
    # preserve existing timestamp if already set and still all done.
    with connect(db_path) as conn:
        existing = conn.execute(
            "SELECT proposal_completed_at FROM intakes WHERE id = ?", (intake_id,)
        ).fetchone()
        existing_ts = existing["proposal_completed_at"] if existing else None
        if all_done:
            completed_at = existing_ts or updated_at
        else:
            completed_at = None
        conn.execute(
            """UPDATE intakes
               SET updated_at = ?, proposal_checklist_json = ?, proposal_completed_at = ?
               WHERE id = ?""",
            (updated_at, json.dumps(checklist, ensure_ascii=False), completed_at, intake_id),
        )


def set_mo_review(
    db_path: Path,
    intake_id: int,
    *,
    mo_decision: str,
    mo_notes: Optional[str],
    mo_conditions: Optional[str],
    mo_fee_decision: Optional[str],
    mo_fee_override: Optional[str],
    status: str,
) -> None:
    updated_at = _utc_now_iso()
    reviewed_at = updated_at
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE intakes
            SET
              updated_at = ?,
              mo_decision = ?,
              mo_notes = ?,
              mo_conditions = ?,
              mo_reviewed_at = ?,
              mo_fee_decision = ?,
              mo_fee_override = ?,
              status = ?
            WHERE id = ?
            """,
            (
                updated_at,
                mo_decision,
                mo_notes,
                mo_conditions,
                reviewed_at,
                mo_fee_decision,
                mo_fee_override,
                status,
                intake_id,
            ),
        )
