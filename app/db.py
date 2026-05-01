from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from supabase import create_client, Client


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _client() -> Client:
    return create_client(
        os.environ["SUPABASE_URL"],
        os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"],
    )


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
    mo_fee_decision: Optional[str] = None
    mo_fee_override: Optional[str] = None
    proposal_checklist_json: Optional[str] = None
    proposal_completed_at: Optional[str] = None
    ifp_due_date: Optional[str] = None
    proposal_text: Optional[str] = None
    proposal_generated_at: Optional[str] = None
    project_number: Optional[str] = None

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

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "IntakeRow":
        return cls(
            id=d["id"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            inquiry_date=d.get("inquiry_date"),
            project_name=d["project_name"],
            client_name=d.get("client_name"),
            architect_name=d.get("architect_name"),
            lead_contact=d.get("lead_contact"),
            location_region=d.get("location_region"),
            submitted_by=d.get("submitted_by"),
            status=d["status"],
            recommendation=d.get("recommendation"),
            recommendation_reason=d.get("recommendation_reason"),
            red_flags_json=d.get("red_flags_json") or "[]",
            red_flag_counts_json=d.get("red_flag_counts_json") or "{}",
            answers_json=d.get("answers_json") or "{}",
            mo_decision=d.get("mo_decision"),
            mo_notes=d.get("mo_notes"),
            mo_conditions=d.get("mo_conditions"),
            mo_reviewed_at=d.get("mo_reviewed_at"),
            mo_fee_decision=d.get("mo_fee_decision"),
            mo_fee_override=d.get("mo_fee_override"),
            proposal_checklist_json=d.get("proposal_checklist_json"),
            proposal_completed_at=d.get("proposal_completed_at"),
            ifp_due_date=d.get("ifp_due_date"),
            proposal_text=d.get("proposal_text"),
            proposal_generated_at=d.get("proposal_generated_at"),
            project_number=d.get("project_number"),
        )


def init_db() -> None:
    pass  # Table is managed in Supabase


def list_intakes(*, status: Optional[str] = None) -> list[IntakeRow]:
    q = (
        _client()
        .table("intakes")
        .select("*")
        .order("updated_at", desc=True)
        .order("id", desc=True)
    )
    if status:
        q = q.eq("status", status)
    resp = q.execute()
    return [IntakeRow.from_dict(r) for r in resp.data]


def list_pending_mo() -> list[IntakeRow]:
    resp = (
        _client()
        .table("intakes")
        .select("*")
        .eq("status", "PENDING_MO_REVIEW")
        .order("created_at")
        .order("id")
        .execute()
    )
    return [IntakeRow.from_dict(r) for r in resp.data]


def get_intake(intake_id: int) -> Optional[IntakeRow]:
    resp = (
        _client()
        .table("intakes")
        .select("*")
        .eq("id", intake_id)
        .maybe_single()
        .execute()
    )
    return IntakeRow.from_dict(resp.data) if resp.data else None


def create_intake(
    *,
    inquiry_date: Optional[str],
    ifp_due_date: Optional[str],
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
    now = _utc_now_iso()
    resp = (
        _client()
        .table("intakes")
        .insert({
            "created_at":            now,
            "updated_at":            now,
            "inquiry_date":          inquiry_date,
            "ifp_due_date":          ifp_due_date,
            "project_name":          project_name,
            "client_name":           client_name,
            "architect_name":        architect_name,
            "lead_contact":          lead_contact,
            "location_region":       location_region,
            "submitted_by":          submitted_by,
            "status":                status,
            "recommendation":        recommendation,
            "recommendation_reason": recommendation_reason,
            "red_flags_json":        json.dumps(red_flags, ensure_ascii=False),
            "red_flag_counts_json":  json.dumps(red_flag_counts, ensure_ascii=False),
            "answers_json":          json.dumps(answers, ensure_ascii=False),
        })
        .execute()
    )
    return int(resp.data[0]["id"])


def update_intake(
    intake_id: int,
    *,
    inquiry_date: Optional[str],
    ifp_due_date: Optional[str],
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
    (
        _client()
        .table("intakes")
        .update({
            "updated_at":            _utc_now_iso(),
            "inquiry_date":          inquiry_date,
            "ifp_due_date":          ifp_due_date,
            "project_name":          project_name,
            "client_name":           client_name,
            "architect_name":        architect_name,
            "lead_contact":          lead_contact,
            "location_region":       location_region,
            "submitted_by":          submitted_by,
            "status":                status,
            "recommendation":        recommendation,
            "recommendation_reason": recommendation_reason,
            "red_flags_json":        json.dumps(red_flags, ensure_ascii=False),
            "red_flag_counts_json":  json.dumps(red_flag_counts, ensure_ascii=False),
            "answers_json":          json.dumps(answers, ensure_ascii=False),
        })
        .eq("id", intake_id)
        .execute()
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
    intake_id: int,
    checklist: dict[str, bool],
) -> None:
    all_done = all(checklist.get(k, False) for k in CHECKLIST_KEYS)
    existing = get_intake(intake_id)
    existing_ts = existing.proposal_completed_at if existing else None
    completed_at = (existing_ts or _utc_now_iso()) if all_done else None
    (
        _client()
        .table("intakes")
        .update({
            "updated_at":               _utc_now_iso(),
            "proposal_checklist_json":  json.dumps(checklist, ensure_ascii=False),
            "proposal_completed_at":    completed_at,
        })
        .eq("id", intake_id)
        .execute()
    )


@dataclass(frozen=True)
class TemplateRow:
    id: int
    created_at: str
    name: str
    description: str
    answers_json: str

    @property
    def answers(self) -> dict[str, Any]:
        return json.loads(self.answers_json or "{}")

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TemplateRow":
        return cls(
            id=d["id"],
            created_at=d["created_at"],
            name=d["name"],
            description=d.get("description") or "",
            answers_json=d.get("answers_json") or "{}",
        )


def list_templates() -> list[TemplateRow]:
    resp = (
        _client()
        .table("templates")
        .select("*")
        .order("name")
        .execute()
    )
    return [TemplateRow.from_dict(r) for r in resp.data]


def create_template(*, name: str, description: str, answers: dict[str, Any]) -> int:
    resp = (
        _client()
        .table("templates")
        .insert({
            "name": name,
            "description": description,
            "answers_json": json.dumps(answers, ensure_ascii=False),
        })
        .execute()
    )
    return int(resp.data[0]["id"])


def delete_intake(intake_id: int) -> None:
    _client().table("intakes").delete().eq("id", intake_id).execute()


def delete_template(template_id: int) -> None:
    _client().table("templates").delete().eq("id", template_id).execute()


def save_proposal(intake_id: int, text: str) -> None:
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({
            "proposal_text":         text,
            "proposal_generated_at": now,
            "updated_at":            now,
        })
        .eq("id", intake_id)
        .execute()
    )


def set_status(intake_id: int, status: str) -> None:
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({"status": status, "updated_at": now})
        .eq("id", intake_id)
        .execute()
    )


def set_mo_review(
    intake_id: int,
    *,
    mo_decision: str,
    mo_notes: Optional[str],
    mo_conditions: Optional[str],
    mo_fee_decision: Optional[str],
    mo_fee_override: Optional[str],
    status: str,
) -> None:
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({
            "updated_at":      now,
            "mo_decision":     mo_decision,
            "mo_notes":        mo_notes,
            "mo_conditions":   mo_conditions,
            "mo_reviewed_at":  now,
            "mo_fee_decision": mo_fee_decision,
            "mo_fee_override": mo_fee_override,
            "status":          status,
        })
        .eq("id", intake_id)
        .execute()
    )


# ── Calendar Events ──────────────────────────────────────────────────────────

PHASE_COLORS = {
    "RFP": "#94a3b8",   # slate-400
    "50%": "#93c5fd",   # blue-300
    "75%": "#3b82f6",   # blue-500
    "90%": "#4f46e5",   # indigo-600
    "DD":  "#34d399",   # emerald-400
    "CA":  "#fb923c",   # orange-400
    "CD":  "#be123c",   # rose-700  (darkened to distinguish from CA orange)
    "IFP": "#dc2626",   # red-600   (swapped to clear red)
    "REV": "#a855f7",   # purple-500
    "SD":  "#06b6d4",   # cyan-500
}

TEAM_MEMBERS = ["MK", "NK", "RS", "RO", "SW", "JP", "JW", "JR", "RK"]

TEAM_COLORS = {
    "JW": "#2563eb",  # blue-600
    "SW": "#7c3aed",  # violet-600
    "JP": "#db2777",  # pink-600
    "JR": "#16a34a",  # green-600
    "MK": "#0891b2",  # cyan-700
    "NK": "#d97706",  # amber-600
    "RS": "#ea580c",  # orange-600
    "RO": "#9333ea",  # purple-600
    "RK": "#0d9488",  # teal-600
}

ENGINEER_ROLES = {
    "MK": "Principal",
    "NK": "Sr. Engineer",
    "RS": "Engineer",
    "RO": "Engineer",
    "SW": "Engineer",
    "JP": "Designer",
    "JW": "Designer",
    "JR": "Project Manager",
    "RK": "Engineer",
}

VALID_PHASES = list(PHASE_COLORS.keys())


def format_event_title(event: dict) -> str:
    if event.get("is_ooo"):
        team = "/".join(event.get("team") or [])
        return f"OOO: {team}"
    team = "(" + "/".join(event.get("team") or []) + ")"
    type_ = "(" + (event.get("project_type") or "") + ")"
    return (
        f"{event.get('project_number', '')}-{event.get('client', '')}"
        f"-{event.get('location', '')} {event.get('phase', '')} - {team} {type_}"
    )


@dataclass(frozen=True)
class CalendarEventRow:
    id: str
    project_number: str
    client: str
    location: str
    phase: str
    team: list
    project_type: str
    start_date: str
    end_date: str
    is_ooo: bool
    metadata: Optional[dict]
    created_at: str
    updated_at: str
    tier: Optional[int] = None
    phase_jump: bool = False

    @property
    def title(self) -> str:
        return format_event_title({
            "is_ooo": self.is_ooo,
            "team": self.team,
            "project_number": self.project_number,
            "client": self.client,
            "location": self.location,
            "phase": self.phase,
            "project_type": self.project_type,
        })

    @property
    def color(self) -> str:
        return PHASE_COLORS.get(self.phase, "#9ca3af")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "project_number": self.project_number,
            "client": self.client,
            "location": self.location,
            "phase": self.phase,
            "team": self.team,
            "project_type": self.project_type,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "is_ooo": self.is_ooo,
            "tier": self.tier,
            "phase_jump": self.phase_jump,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "title": self.title,
            "color": self.color,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CalendarEventRow":
        return cls(
            id=d["id"],
            project_number=d.get("project_number") or "",
            client=d.get("client") or "",
            location=d.get("location") or "",
            phase=d.get("phase") or "",
            team=d.get("team") or [],
            project_type=d.get("project_type") or "",
            start_date=d.get("start_date") or "",
            end_date=d.get("end_date") or "",
            is_ooo=bool(d.get("is_ooo", False)),
            tier=d.get("tier"),
            phase_jump=bool(d.get("phase_jump", False)),
            metadata=d.get("metadata"),
            created_at=d.get("created_at") or "",
            updated_at=d.get("updated_at") or "",
        )


def list_calendar_events(
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> list[CalendarEventRow]:
    q = (
        _client()
        .table("calendar_events")
        .select("*")
        .order("start_date")
    )
    if start:
        q = q.gte("end_date", start)
    if end:
        q = q.lte("start_date", end)
    resp = q.execute()
    return [CalendarEventRow.from_dict(r) for r in resp.data]


def get_calendar_event(event_id: str) -> Optional[CalendarEventRow]:
    resp = (
        _client()
        .table("calendar_events")
        .select("*")
        .eq("id", event_id)
        .maybe_single()
        .execute()
    )
    return CalendarEventRow.from_dict(resp.data) if resp.data else None


def create_calendar_event(
    *,
    project_number: str,
    client: str,
    location: str,
    phase: str,
    team: list,
    project_type: str,
    start_date: str,
    end_date: str,
    is_ooo: bool = False,
    tier: Optional[int] = None,
    phase_jump: bool = False,
    metadata: Optional[dict] = None,
) -> str:
    now = _utc_now_iso()
    resp = (
        _client()
        .table("calendar_events")
        .insert({
            "project_number": project_number,
            "client":         client,
            "location":       location,
            "phase":          phase,
            "team":           team,
            "project_type":   project_type,
            "start_date":     start_date,
            "end_date":       end_date,
            "is_ooo":         is_ooo,
            "tier":           tier,
            "phase_jump":     phase_jump,
            "metadata":       metadata,
            "created_at":     now,
            "updated_at":     now,
        })
        .execute()
    )
    return str(resp.data[0]["id"])


def update_calendar_event(
    event_id: str,
    *,
    project_number: str,
    client: str,
    location: str,
    phase: str,
    team: list,
    project_type: str,
    start_date: str,
    end_date: str,
    is_ooo: bool = False,
    tier: Optional[int] = None,
    phase_jump: bool = False,
    metadata: Optional[dict] = None,
) -> None:
    (
        _client()
        .table("calendar_events")
        .update({
            "project_number": project_number,
            "client":         client,
            "location":       location,
            "phase":          phase,
            "team":           team,
            "project_type":   project_type,
            "start_date":     start_date,
            "end_date":       end_date,
            "is_ooo":         is_ooo,
            "tier":           tier,
            "phase_jump":     phase_jump,
            "metadata":       metadata,
            "updated_at":     _utc_now_iso(),
        })
        .eq("id", event_id)
        .execute()
    )


def delete_calendar_event(event_id: str) -> None:
    _client().table("calendar_events").delete().eq("id", event_id).execute()


def count_ifp_on_date(check_date: str) -> int:
    """Count non-OOO IFP events that span the given date (YYYY-MM-DD)."""
    resp = (
        _client()
        .table("calendar_events")
        .select("id", count="exact")
        .eq("phase", "IFP")
        .eq("is_ooo", False)
        .lte("start_date", f"{check_date}T23:59:59Z")
        .gte("end_date", f"{check_date}T00:00:00Z")
        .execute()
    )
    return resp.count or 0


# ── Constants ────────────────────────────────────────────────────────────────

BILLING_RATE: float = 150.0

DEFAULT_PHASE_SPLITS: dict[str, float] = {
    "SD":  0.05,
    "50%": 0.20,
    "75%": 0.20,
    "90%": 0.15,
    "IFP": 0.25,
    "CA":  0.10,
    "REV": 0.05,
}

TEAM_FULL_NAMES: dict[str, str] = {
    "MK": "Mo Kateeb",
    "NK": "Nathan Kline",
    "RS": "R. Schwan",
    "RO": "R. Ochoa",
    "SW": "S. Woodward",
    "JP": "J. Peterson",
    "JW": "J. Woodward",
    "JR": "J. Rodriguez",
    "RK": "R. Kline",
}


# ── Project Number Sequence ──────────────────────────────────────────────────

def assign_next_project_number() -> str:
    resp = _client().rpc("increment_project_number", {}).execute()
    return str(resp.data).zfill(4)


def get_project_number_seed() -> dict[str, Any]:
    resp = (
        _client()
        .table("project_number_sequence")
        .select("*")
        .eq("id", 1)
        .maybe_single()
        .execute()
    )
    return resp.data or {"last_number": 9000, "updated_at": None}


def set_project_number_seed(seed: int) -> None:
    (
        _client()
        .table("project_number_sequence")
        .update({"last_number": seed, "updated_at": _utc_now_iso()})
        .eq("id", 1)
        .execute()
    )


def set_intake_project_number(intake_id: int, project_number: str) -> None:
    (
        _client()
        .table("intakes")
        .update({"project_number": project_number, "updated_at": _utc_now_iso()})
        .eq("id", intake_id)
        .execute()
    )


# ── Phase Budgets ────────────────────────────────────────────────────────────

def generate_phase_budgets(intake_id: int, project_number: str, approved_fee: float) -> None:
    now = _utc_now_iso()
    rows = [
        {
            "intake_id": intake_id,
            "project_number": project_number,
            "phase_code": phase_code,
            "budgeted_hours": round((approved_fee * split) / BILLING_RATE, 2),
            "approved_fee": approved_fee,
            "billing_rate": BILLING_RATE,
            "created_at": now,
            "updated_at": now,
        }
        for phase_code, split in DEFAULT_PHASE_SPLITS.items()
    ]
    (
        _client()
        .table("phase_budgets")
        .upsert(rows, on_conflict="intake_id,phase_code")
        .execute()
    )


def list_phase_budgets(intake_id: int) -> list[dict[str, Any]]:
    budgets_resp = (
        _client()
        .table("phase_budgets")
        .select("*")
        .eq("intake_id", intake_id)
        .execute()
    )
    budgets = budgets_resp.data or []

    te_resp = (
        _client()
        .table("time_entries")
        .select("phase_code,hours")
        .eq("intake_id", intake_id)
        .execute()
    )
    hours_by_phase: dict[str, float] = {}
    for e in (te_resp.data or []):
        p = e["phase_code"]
        hours_by_phase[p] = hours_by_phase.get(p, 0.0) + float(e["hours"])

    phase_order = list(DEFAULT_PHASE_SPLITS.keys())

    def _sort_key(b: dict) -> int:
        try:
            return phase_order.index(b["phase_code"])
        except ValueError:
            return 99

    result = []
    for b in sorted(budgets, key=_sort_key):
        phase = b["phase_code"]
        budgeted = float(b["budgeted_hours"])
        used = round(hours_by_phase.get(phase, 0.0), 2)
        remaining = round(budgeted - used, 2)
        pct = round((used / budgeted * 100) if budgeted > 0 else 0.0, 1)
        result.append({
            "id": b["id"],
            "intake_id": b["intake_id"],
            "project_number": b["project_number"],
            "phase_code": phase,
            "budgeted_hours": budgeted,
            "approved_fee": float(b["approved_fee"]),
            "billing_rate": float(b["billing_rate"]),
            "hours_used": used,
            "remaining": remaining,
            "pct_used": pct,
        })
    return result


def update_phase_budget(intake_id: int, phase_code: str, budgeted_hours: float) -> None:
    (
        _client()
        .table("phase_budgets")
        .update({"budgeted_hours": budgeted_hours, "updated_at": _utc_now_iso()})
        .eq("intake_id", intake_id)
        .eq("phase_code", phase_code)
        .execute()
    )


def update_intake_ifp_date(intake_id: int, ifp_due_date: str) -> None:
    (
        _client()
        .table("intakes")
        .update({"ifp_due_date": ifp_due_date, "updated_at": _utc_now_iso()})
        .eq("id", intake_id)
        .execute()
    )


def list_time_entries_for_intake(intake_id: int) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("time_entries")
        .select("*")
        .eq("intake_id", intake_id)
        .order("entry_date", desc=True)
        .order("id", desc=True)
        .execute()
    )
    return resp.data or []


# ── Time Entries ─────────────────────────────────────────────────────────────

def create_time_entry(
    *,
    engineer_initials: str,
    project_number: str,
    intake_id: Optional[int],
    phase_code: str,
    entry_date: str,
    hours: float,
    notes: Optional[str],
) -> int:
    now = _utc_now_iso()
    resp = (
        _client()
        .table("time_entries")
        .insert({
            "engineer_initials": engineer_initials,
            "project_number":    project_number,
            "intake_id":         intake_id,
            "phase_code":        phase_code,
            "entry_date":        entry_date,
            "hours":             hours,
            "notes":             notes,
            "created_at":        now,
            "updated_at":        now,
        })
        .execute()
    )
    return int(resp.data[0]["id"])


def list_time_entries(
    *,
    start: Optional[str] = None,
    end: Optional[str] = None,
    engineer: Optional[str] = None,
) -> list[dict[str, Any]]:
    q = (
        _client()
        .table("time_entries")
        .select("*")
        .order("entry_date", desc=False)
        .order("id", desc=False)
    )
    if start:
        q = q.gte("entry_date", start)
    if end:
        q = q.lte("entry_date", end)
    if engineer:
        q = q.eq("engineer_initials", engineer)
    return q.execute().data or []


def update_time_entry(entry_id: int, *, hours: float, notes: Optional[str]) -> None:
    (
        _client()
        .table("time_entries")
        .update({"hours": hours, "notes": notes, "updated_at": _utc_now_iso()})
        .eq("id", entry_id)
        .execute()
    )


def delete_time_entry(entry_id: int) -> None:
    _client().table("time_entries").delete().eq("id", entry_id).execute()


def get_time_entry(entry_id: int) -> Optional[dict[str, Any]]:
    resp = (
        _client()
        .table("time_entries")
        .select("*")
        .eq("id", entry_id)
        .maybe_single()
        .execute()
    )
    return resp.data


def count_timesheet_period_entries(start: str, end: str) -> int:
    resp = (
        _client()
        .table("time_entries")
        .select("id", count="exact")
        .gte("entry_date", start)
        .lte("entry_date", end)
        .execute()
    )
    return resp.count or 0


# ── Active Projects (for timesheet dropdown) ─────────────────────────────────

def list_active_projects(engineer: Optional[str] = None) -> list[dict[str, Any]]:
    from datetime import date as _date
    today = _date.today().isoformat()
    resp = (
        _client()
        .table("calendar_events")
        .select("*")
        .gte("end_date", today + "T00:00:00Z")
        .eq("is_ooo", False)
        .execute()
    )
    events = resp.data or []

    if engineer:
        events = [e for e in events if engineer in (e.get("team") or [])]

    project_numbers = list({e["project_number"] for e in events if e.get("project_number")})
    if not project_numbers:
        return []

    intake_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name")
        .in_("project_number", project_numbers)
        .execute()
    )
    intake_by_pn: dict[str, dict] = {i["project_number"]: i for i in (intake_resp.data or [])}
    intake_ids = [i["id"] for i in (intake_resp.data or [])]

    phases_by_intake: dict[int, list[str]] = {}
    if intake_ids:
        pb_resp = (
            _client()
            .table("phase_budgets")
            .select("intake_id,phase_code")
            .in_("intake_id", intake_ids)
            .execute()
        )
        for pb in (pb_resp.data or []):
            iid = int(pb["intake_id"])
            phases_by_intake.setdefault(iid, [])
            if pb["phase_code"] not in phases_by_intake[iid]:
                phases_by_intake[iid].append(pb["phase_code"])

    results = []
    for pn in sorted(project_numbers):
        intake = intake_by_pn.get(pn, {})
        iid = intake.get("id")
        phases = phases_by_intake.get(int(iid), []) if iid else []
        if not phases:
            phases = sorted({e["phase"] for e in events if e.get("project_number") == pn and e.get("phase")})
        cal_ev = next((e for e in events if e.get("project_number") == pn), {})
        results.append({
            "intake_id":    iid,
            "project_number": pn,
            "client":       intake.get("client_name") or cal_ev.get("client") or "",
            "project_name": intake.get("project_name") or cal_ev.get("client") or pn,
            "phases":       phases,
        })
    return results


# ── Payroll Export ───────────────────────────────────────────────────────────

def get_payroll_data(start: str, end: str) -> dict[str, Any]:
    entries = list_time_entries(start=start, end=end)
    if not entries:
        return {
            "entries": [],
            "summary_rows": [],
            "kpis": {"total_hours": 0.0, "engineers_active": 0, "projects_billed": 0},
            "start": start,
            "end": end,
        }

    project_numbers = list({e["project_number"] for e in entries})
    intake_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name")
        .in_("project_number", project_numbers)
        .execute()
    )
    intake_by_pn: dict[str, dict] = {i["project_number"]: i for i in (intake_resp.data or [])}
    intake_ids = [i["id"] for i in (intake_resp.data or [])]

    budgets_by_key: dict[tuple, float] = {}
    if intake_ids:
        pb_resp = (
            _client()
            .table("phase_budgets")
            .select("intake_id,phase_code,budgeted_hours")
            .in_("intake_id", intake_ids)
            .execute()
        )
        for pb in (pb_resp.data or []):
            budgets_by_key[(int(pb["intake_id"]), pb["phase_code"])] = float(pb["budgeted_hours"])

    # cumulative hours per (project_number, phase_code) across all time
    all_te_resp = (
        _client()
        .table("time_entries")
        .select("project_number,phase_code,hours")
        .in_("project_number", project_numbers)
        .execute()
    )
    cumulative: dict[tuple, float] = {}
    for e in (all_te_resp.data or []):
        key = (e["project_number"], e["phase_code"])
        cumulative[key] = cumulative.get(key, 0.0) + float(e["hours"])

    # Aggregate this-period hours: engineer → pn → phase → hours
    agg: dict[str, dict[str, dict[str, float]]] = {}
    for e in entries:
        eng, pn, ph = e["engineer_initials"], e["project_number"], e["phase_code"]
        agg.setdefault(eng, {}).setdefault(pn, {}).setdefault(ph, 0.0)
        agg[eng][pn][ph] += float(e["hours"])

    summary_rows: list[dict[str, Any]] = []
    for eng in sorted(agg):
        eng_total = 0.0
        detail_rows: list[dict[str, Any]] = []
        for pn in sorted(agg[eng]):
            intake = intake_by_pn.get(pn, {})
            iid = intake.get("id")
            for ph in sorted(agg[eng][pn]):
                hrs = round(agg[eng][pn][ph], 2)
                eng_total += hrs
                budget = budgets_by_key.get((int(iid), ph), 0.0) if iid else 0.0
                cum_hrs = round(cumulative.get((pn, ph), 0.0), 2)
                pct = round(cum_hrs / budget * 100, 1) if budget > 0 else None
                detail_rows.append({
                    "engineer":       eng,
                    "project_number": pn,
                    "client":         intake.get("client_name") or "",
                    "project_name":   intake.get("project_name") or "",
                    "phase":          ph,
                    "hours":          hrs,
                    "budget":         budget,
                    "pct_budget":     pct,
                    "cum_hours":      cum_hrs,
                    "is_subtotal":    False,
                })
        summary_rows.append({
            "engineer": eng, "project_number": None, "client": "", "project_name": "",
            "phase": "", "hours": round(eng_total, 2), "budget": None, "pct_budget": None,
            "cum_hours": None, "is_subtotal": True,
        })
        summary_rows.extend(detail_rows)

    kpis = {
        "total_hours":       round(sum(float(e["hours"]) for e in entries), 2),
        "engineers_active":  len({e["engineer_initials"] for e in entries}),
        "projects_billed":   len({e["project_number"] for e in entries}),
    }
    return {
        "entries":      entries,
        "summary_rows": summary_rows,
        "kpis":         kpis,
        "start":        start,
        "end":          end,
        "intake_by_pn": intake_by_pn,
    }


# ── Pipeline / Billing Phase constants ───────────────────────────────────────

PRODUCTION_PHASE_ORDER: list[str] = [
    "RFP", "SD", "DD", "50%", "75%", "90%", "CD", "IFP", "CA", "REV"
]

BILLING_PHASE_ORDER: list[str] = ["retainer", "SD", "DD", "CD", "CA"]

# exiting this production phase triggers this billing gate
BILLING_TRIGGER_MAP: dict[str, str] = {
    "SD":  "SD",
    "DD":  "DD",
    "IFP": "CD",
    "CA":  "CA",
}

# production phases that count toward each billing gate's hours budget
BILLING_TO_PRODUCTION: dict[str, list[str]] = {
    "SD":  ["SD"],
    "DD":  ["DD"],
    "CD":  ["50%", "75%", "90%", "CD", "IFP"],
    "CA":  ["CA"],
}

BILLING_PHASE_LABELS: dict[str, str] = {
    "retainer": "Retainer",
    "SD":        "Schematic Design",
    "DD":        "Design Development",
    "CD":        "Construction Documents",
    "CA":        "Construction Admin",
}


# ── Billing Phase Definitions (firm-level) ───────────────────────────────────

def get_billing_phase_definitions() -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("billing_phase_definitions")
        .select("*")
        .order("sequence_order")
        .execute()
    )
    return resp.data or []


def update_billing_phase_definition(code: str, default_pct: float) -> None:
    (
        _client()
        .table("billing_phase_definitions")
        .update({"default_pct": default_pct})
        .eq("code", code)
        .execute()
    )


# ── Project Billing Phases ────────────────────────────────────────────────────

def create_billing_phases_for_project(intake_id: int, approved_fee: float) -> None:
    """Idempotent — creates one billing phase row per definition if not already present."""
    defs = get_billing_phase_definitions()
    now = _utc_now_iso()
    rows = []
    for d in defs:
        fee_amount = round(approved_fee * float(d["default_pct"]), 2)
        status = "complete_pending_approval" if d["code"] == "retainer" else "pending"
        rows.append({
            "intake_id":           intake_id,
            "billing_phase_code":  d["code"],
            "fee_amount":          fee_amount,
            "fee_pct":             float(d["default_pct"]),
            "status":              status,
            "created_at":          now,
            "updated_at":          now,
        })
    (
        _client()
        .table("project_billing_phases")
        .upsert(rows, on_conflict="intake_id,billing_phase_code", ignore_duplicates=True)
        .execute()
    )
    # Mark project as pipeline-active
    (
        _client()
        .table("intakes")
        .update({
            "pipeline_active":         1,
            "current_billing_phase":   "retainer",
            "current_production_phase": "SD",
            "updated_at":              now,
        })
        .eq("id", intake_id)
        .execute()
    )


def get_project_billing_phases(intake_id: int) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("project_billing_phases")
        .select("*")
        .eq("intake_id", intake_id)
        .order("id")
        .execute()
    )
    rows = resp.data or []
    # sort by billing phase order
    order_map = {c: i for i, c in enumerate(BILLING_PHASE_ORDER)}
    rows.sort(key=lambda r: order_map.get(r["billing_phase_code"], 99))
    return rows


def get_pending_invoice_approvals() -> list[dict[str, Any]]:
    """Returns billing phases awaiting Mo's invoice approval, enriched with intake data."""
    pbp_resp = (
        _client()
        .table("project_billing_phases")
        .select("*")
        .eq("status", "complete_pending_approval")
        .order("updated_at")
        .execute()
    )
    rows = pbp_resp.data or []
    if not rows:
        return []

    intake_ids = list({r["intake_id"] for r in rows})
    intake_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name,location_region")
        .in_("id", intake_ids)
        .execute()
    )
    intake_map = {i["id"]: i for i in (intake_resp.data or [])}

    result = []
    for r in rows:
        intake = intake_map.get(r["intake_id"], {})
        # hours check for this billing gate
        hours = check_phase_hours_vs_budget(r["intake_id"], r["billing_phase_code"])
        result.append({**r, **{"intake": intake, "hours_check": hours}})
    return result


def count_pending_invoice_approvals() -> int:
    resp = (
        _client()
        .table("project_billing_phases")
        .select("id", count="exact")
        .eq("status", "complete_pending_approval")
        .execute()
    )
    return resp.count or 0


def approve_invoice(
    intake_id: int,
    billing_phase_code: str,
    approved_by: str,
    fee_override: Optional[float],
    note: Optional[str],
) -> dict[str, Any]:
    now = _utc_now_iso()
    update_data: dict[str, Any] = {
        "status":               "invoice_approved",
        "invoice_approved_by":  approved_by,
        "invoice_approved_at":  now,
        "invoice_approved_note": note,
        "updated_at":           now,
    }
    if fee_override is not None:
        update_data["invoice_fee_override"] = fee_override

    (
        _client()
        .table("project_billing_phases")
        .update(update_data)
        .eq("intake_id", intake_id)
        .eq("billing_phase_code", billing_phase_code)
        .execute()
    )

    # advance current_billing_phase
    try:
        idx = BILLING_PHASE_ORDER.index(billing_phase_code)
        next_billing = BILLING_PHASE_ORDER[idx + 1] if idx + 1 < len(BILLING_PHASE_ORDER) else "complete"
    except ValueError:
        next_billing = "complete"

    (
        _client()
        .table("intakes")
        .update({"current_billing_phase": next_billing, "updated_at": now})
        .eq("id", intake_id)
        .execute()
    )

    # phase history
    (
        _client()
        .table("project_phase_history")
        .insert({
            "intake_id":  intake_id,
            "phase_type": "billing",
            "phase_code": billing_phase_code,
            "moved_by":   approved_by,
            "note":       f"Invoice approved. Next: {next_billing}",
            "exited_at":  now,
        })
        .execute()
    )
    return {"success": True, "next_billing_phase": next_billing}


def decline_invoice(
    intake_id: int,
    billing_phase_code: str,
    declined_by: str,
    reason: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    # fetch existing note to append
    existing_resp = (
        _client()
        .table("project_billing_phases")
        .select("phase_completed_note")
        .eq("intake_id", intake_id)
        .eq("billing_phase_code", billing_phase_code)
        .maybe_single()
        .execute()
    )
    prev_note = (existing_resp.data or {}).get("phase_completed_note") or ""
    combined_note = (prev_note + f"\n[DECLINED by {declined_by}: {reason}]").strip()

    (
        _client()
        .table("project_billing_phases")
        .update({
            "status":              "pending",
            "phase_completed_note": combined_note,
            "updated_at":          now,
        })
        .eq("intake_id", intake_id)
        .eq("billing_phase_code", billing_phase_code)
        .execute()
    )
    return {"success": True}


def set_change_order(intake_id: int, pending: bool, note: Optional[str]) -> None:
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({
            "change_order_pending": 1 if pending else 0,
            "change_order_note":    note,
            "updated_at":           now,
        })
        .eq("id", intake_id)
        .execute()
    )


# ── Phase Advancement ─────────────────────────────────────────────────────────

def check_phase_hours_vs_budget(intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    production_phases = BILLING_TO_PRODUCTION.get(billing_phase_code, [])
    if not production_phases:
        return {"over_budget": False, "budgeted": 0.0, "actual": 0.0, "variance": 0.0}

    pb_resp = (
        _client()
        .table("phase_budgets")
        .select("phase_code,budgeted_hours")
        .eq("intake_id", intake_id)
        .in_("phase_code", production_phases)
        .execute()
    )
    budgeted = sum(float(r["budgeted_hours"]) for r in (pb_resp.data or []))

    te_resp = (
        _client()
        .table("time_entries")
        .select("hours")
        .eq("intake_id", intake_id)
        .in_("phase_code", production_phases)
        .execute()
    )
    actual = sum(float(r["hours"]) for r in (te_resp.data or []))

    return {
        "over_budget": actual > budgeted if budgeted > 0 else False,
        "budgeted":    round(budgeted, 2),
        "actual":      round(actual, 2),
        "variance":    round(actual - budgeted, 2),
    }


def advance_production_phase(
    intake_id: int,
    to_phase: str,
    completed_by: str,
    note: str,
) -> dict[str, Any]:
    """Advances the production phase, triggers billing gate if applicable."""
    intake_resp = (
        _client()
        .table("intakes")
        .select("current_production_phase,change_order_pending")
        .eq("id", intake_id)
        .maybe_single()
        .execute()
    )
    if not intake_resp.data:
        return {"success": False, "error": "Intake not found"}

    from_phase = intake_resp.data.get("current_production_phase") or "SD"
    now = _utc_now_iso()

    # 1. Update current production phase
    (
        _client()
        .table("intakes")
        .update({"current_production_phase": to_phase, "updated_at": now})
        .eq("id", intake_id)
        .execute()
    )

    # 2. Write history — close previous, open new
    (
        _client()
        .table("project_phase_history")
        .insert({
            "intake_id":  intake_id,
            "phase_type": "production",
            "phase_code": from_phase,
            "moved_by":   completed_by,
            "note":       note,
            "exited_at":  now,
        })
        .execute()
    )
    (
        _client()
        .table("project_phase_history")
        .insert({
            "intake_id":  intake_id,
            "phase_type": "production",
            "phase_code": to_phase,
            "moved_by":   completed_by,
            "note":       f"Entered from {from_phase}",
        })
        .execute()
    )

    # 3. Check billing trigger
    triggered_billing = BILLING_TRIGGER_MAP.get(from_phase)
    billing_alert: Optional[dict[str, Any]] = None

    if triggered_billing:
        co_pending = int(intake_resp.data.get("change_order_pending") or 0)
        if co_pending:
            billing_alert = {
                "type": "co_blocked",
                "message": f"Phase advanced, but {triggered_billing} invoice is blocked — resolve the change order first.",
            }
        else:
            pbp_resp = (
                _client()
                .table("project_billing_phases")
                .select("*")
                .eq("intake_id", intake_id)
                .eq("billing_phase_code", triggered_billing)
                .maybe_single()
                .execute()
            )
            pbp = pbp_resp.data
            if pbp and pbp["status"] == "pending":
                hours = check_phase_hours_vs_budget(intake_id, triggered_billing)
                (
                    _client()
                    .table("project_billing_phases")
                    .update({
                        "status":               "complete_pending_approval",
                        "phase_completed_by":   completed_by,
                        "phase_completed_at":   now,
                        "phase_completed_note": note,
                        "updated_at":           now,
                    })
                    .eq("intake_id", intake_id)
                    .eq("billing_phase_code", triggered_billing)
                    .execute()
                )
                billing_alert = {
                    "type":            "invoice_queued",
                    "billing_phase":   triggered_billing,
                    "fee_amount":      float(pbp["fee_amount"]),
                    "hours_over_budget": hours["over_budget"],
                    "message": (
                        f"{triggered_billing} invoice draft queued for Mo's approval "
                        f"(${float(pbp['fee_amount']):,.0f})"
                    ),
                }

    return {"success": True, "from_phase": from_phase, "to_phase": to_phase, "billing_alert": billing_alert}


# ── Pipeline Board Data ───────────────────────────────────────────────────────

def get_pipeline_data() -> dict[str, Any]:
    """Returns all pipeline_active projects grouped by current_billing_phase."""
    intakes_resp = (
        _client()
        .table("intakes")
        .select(
            "id,project_number,project_name,client_name,location_region,"
            "current_production_phase,current_billing_phase,pipeline_active,"
            "change_order_pending,change_order_note,mo_fee_override"
        )
        .eq("pipeline_active", 1)
        .order("project_number")
        .execute()
    )
    intakes = intakes_resp.data or []
    if not intakes:
        return _empty_pipeline()

    intake_ids = [i["id"] for i in intakes]

    # billing phases
    pbp_resp = (
        _client()
        .table("project_billing_phases")
        .select("intake_id,billing_phase_code,fee_amount,fee_pct,status,change_order_pending,invoice_fee_override")
        .in_("intake_id", intake_ids)
        .execute()
    )
    pbp_by_intake: dict[int, list[dict]] = {}
    for row in (pbp_resp.data or []):
        pbp_by_intake.setdefault(int(row["intake_id"]), []).append(row)

    # phase budgets — total budgeted hours per project
    pb_resp = (
        _client()
        .table("phase_budgets")
        .select("intake_id,budgeted_hours")
        .in_("intake_id", intake_ids)
        .execute()
    )
    budget_by_intake: dict[int, float] = {}
    for row in (pb_resp.data or []):
        iid = int(row["intake_id"])
        budget_by_intake[iid] = budget_by_intake.get(iid, 0.0) + float(row["budgeted_hours"])

    # actual hours per project
    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id,hours")
        .in_("intake_id", intake_ids)
        .execute()
    )
    actual_by_intake: dict[int, float] = {}
    for row in (te_resp.data or []):
        iid = int(row["intake_id"])
        actual_by_intake[iid] = actual_by_intake.get(iid, 0.0) + float(row["hours"])

    # calendar team members per project
    cal_resp = (
        _client()
        .table("calendar_events")
        .select("project_number,team")
        .in_("project_number", [i["project_number"] for i in intakes if i.get("project_number")])
        .execute()
    )
    team_by_pn: dict[str, list[str]] = {}
    for ev in (cal_resp.data or []):
        pn = ev.get("project_number") or ""
        team = ev.get("team") or []
        if isinstance(team, list):
            existing = team_by_pn.get(pn, [])
            for m in team:
                if m not in existing:
                    existing.append(m)
            team_by_pn[pn] = existing

    columns: dict[str, dict] = {
        "retainer": {"label": "Retainer",              "projects": []},
        "SD":       {"label": "Schematic Design",      "projects": []},
        "DD":       {"label": "Design Development",    "projects": []},
        "CD":       {"label": "Construction Documents","projects": []},
        "CA":       {"label": "Construction Admin",    "projects": []},
        "complete": {"label": "Complete",              "projects": []},
    }

    for intake in intakes:
        iid = intake["id"]
        billing_col = intake.get("current_billing_phase") or "retainer"
        if billing_col not in columns:
            billing_col = "complete"

        billing_phases = pbp_by_intake.get(iid, [])
        current_pbp = next(
            (p for p in billing_phases if p["billing_phase_code"] == billing_col), None
        )
        invoice_status = current_pbp["status"] if current_pbp else "pending"
        billing_fee = float(current_pbp["fee_amount"]) if current_pbp else 0.0
        billing_fee_pct = float(current_pbp["fee_pct"]) if current_pbp else 0.0

        budgeted = round(budget_by_intake.get(iid, 0.0), 1)
        actual   = round(actual_by_intake.get(iid, 0.0), 1)
        pn = intake.get("project_number") or ""

        project = {
            "intake_id":              iid,
            "project_number":         pn,
            "project_name":           intake["project_name"],
            "client":                 intake.get("client_name") or "",
            "current_production_phase": intake.get("current_production_phase") or "SD",
            "current_billing_phase":  billing_col,
            "billing_fee":            billing_fee,
            "billing_fee_pct":        billing_fee_pct,
            "team":                   team_by_pn.get(pn, []),
            "budgeted_hours":         budgeted,
            "actual_hours":           actual,
            "change_order_pending":   bool(int(intake.get("change_order_pending") or 0)),
            "invoice_status":         invoice_status,
            "can_advance":            invoice_status not in ("complete_pending_approval", "invoice_approved"),
            "approved_fee":           float(intake.get("mo_fee_override") or 0),
        }
        columns[billing_col]["projects"].append(project)

    total_fee = sum(
        float(i.get("mo_fee_override") or 0) for i in intakes
    )
    pending_invoices = sum(
        1 for col in columns.values()
        for p in col["projects"] if p["invoice_status"] == "complete_pending_approval"
    )

    return {
        "columns": columns,
        "stats": {
            "active_projects":   len(intakes),
            "pending_invoices":  pending_invoices,
            "total_pipeline":    round(total_fee, 2),
        },
    }


def _empty_pipeline() -> dict[str, Any]:
    return {
        "columns": {
            "retainer": {"label": "Retainer",              "projects": []},
            "SD":       {"label": "Schematic Design",      "projects": []},
            "DD":       {"label": "Design Development",    "projects": []},
            "CD":       {"label": "Construction Documents","projects": []},
            "CA":       {"label": "Construction Admin",    "projects": []},
            "complete": {"label": "Complete",              "projects": []},
        },
        "stats": {"active_projects": 0, "pending_invoices": 0, "total_pipeline": 0},
    }
