from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
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
    proposed_start_date: Optional[str] = None
    proposed_end_date: Optional[str] = None
    assigned_engineers: Optional[str] = None  # JSON array string e.g. '["JW","MK"]'
    mo_decision_notes: Optional[str] = None
    proposal_sent_date: Optional[str] = None
    follow_up_count: int = 0
    win_probability: int = 50

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
            proposed_start_date=d.get("proposed_start_date"),
            proposed_end_date=d.get("proposed_end_date"),
            assigned_engineers=d.get("assigned_engineers"),
            mo_decision_notes=d.get("mo_decision_notes"),
            proposal_sent_date=d.get("proposal_sent_date"),
            follow_up_count=int(d.get("follow_up_count") or 0),
            win_probability=int(d.get("win_probability") or 50),
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
    if resp is None or not resp.data:
        return None
    return IntakeRow.from_dict(resp.data)


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
    _client().table("calendar_events").delete().eq("intake_id", intake_id).execute()
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


def _business_days_since(dt_str: str) -> int:
    """Return the number of Mon–Fri business days between dt_str (UTC ISO) and today."""
    try:
        sent = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date()
    except Exception:
        return 0
    today = date.today()
    if today <= sent:
        return 0
    count = 0
    cur = sent + timedelta(days=1)
    while cur <= today:
        if cur.weekday() < 5:
            count += 1
        cur += timedelta(days=1)
    return count


def mark_proposal_sent(intake_id: int) -> None:
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({"status": "PROPOSAL_OUT", "proposal_sent_date": now, "updated_at": now})
        .eq("id", intake_id)
        .execute()
    )


def mark_project_won(intake_id: int, win_probability: int = 100) -> None:
    now = _utc_now_iso()
    # Read current state to avoid overwriting fields already set by create_billing_phases_for_project
    current = (
        _client()
        .table("intakes")
        .select("current_billing_phase,current_production_phase")
        .eq("id", intake_id)
        .maybe_single()
        .execute()
    ).data or {}
    update: dict[str, Any] = {
        "status":          "ACTIVE_PROJECT",
        "win_probability": win_probability,
        "pipeline_active": 1,
        "updated_at":      now,
    }
    if not current.get("current_billing_phase"):
        update["current_billing_phase"]    = "retainer"
    if not current.get("current_production_phase"):
        update["current_production_phase"] = "SD"
    (
        _client()
        .table("intakes")
        .update(update)
        .eq("id", intake_id)
        .execute()
    )


def increment_follow_up(intake_id: int) -> int:
    """Bump follow_up_count by 1 and return the new count."""
    intake = get_intake(intake_id)
    new_count = (intake.follow_up_count if intake else 0) + 1
    now = _utc_now_iso()
    (
        _client()
        .table("intakes")
        .update({"follow_up_count": new_count, "updated_at": now})
        .eq("id", intake_id)
        .execute()
    )
    return new_count


def get_active_bids() -> list[dict[str, Any]]:
    """Return all PROPOSAL_OUT intakes with staleness metadata.
    Returns [] if migration 007 (proposal_sent_date column) hasn't run yet."""
    try:
        resp = (
            _client()
            .table("intakes")
            .select(
                "id,project_name,client_name,location_region,lead_contact,"
                "proposal_sent_date,follow_up_count,win_probability,answers_json,"
                "mo_fee_override,ifp_due_date,inquiry_date"
            )
            .eq("status", "PROPOSAL_OUT")
            .order("proposal_sent_date", desc=False)
            .execute()
        )
    except Exception:
        return []
    rows = resp.data or []
    result = []
    for r in rows:
        bdays = _business_days_since(r.get("proposal_sent_date") or "")
        stale = bdays > 5
        warn  = 3 <= bdays <= 5
        try:
            answers = json.loads(r.get("answers_json") or "{}")
        except Exception:
            answers = {}
        try:
            approved_fee = float(r.get("mo_fee_override") or 0)
        except (ValueError, TypeError):
            approved_fee = 0.0
        result.append({
            "intake_id":         r["id"],
            "project_name":      r.get("project_name") or "",
            "client_name":       r.get("client_name") or "",
            "location":          r.get("location_region") or "",
            "lead_contact":      r.get("lead_contact") or "",
            "proposal_sent_date": r.get("proposal_sent_date") or "",
            "business_days_out": bdays,
            "is_stale":          stale,
            "is_warn":           warn,
            "follow_up_count":   int(r.get("follow_up_count") or 0),
            "win_probability":   int(r.get("win_probability") or 50),
            "approved_fee":      approved_fee,
            "ifp_due_date":      r.get("ifp_due_date") or "",
            "inquiry_date":      r.get("inquiry_date") or "",
            "approx_sf":         answers.get("approx_sf") or "",
            "project_type":      answers.get("project_type") or "",
        })
    return result


def set_mo_review(
    intake_id: int,
    *,
    mo_decision: str,
    mo_notes: Optional[str],
    mo_conditions: Optional[str],
    mo_fee_decision: Optional[str],
    mo_fee_override: Optional[str],
    status: str,
    proposed_start_date: Optional[str] = None,
    proposed_end_date: Optional[str] = None,
    assigned_engineers: Optional[str] = None,
    mo_decision_notes: Optional[str] = None,
) -> None:
    now = _utc_now_iso()
    payload: dict[str, Any] = {
        "updated_at":      now,
        "mo_decision":     mo_decision,
        "mo_notes":        mo_notes,
        "mo_conditions":   mo_conditions,
        "mo_reviewed_at":  now,
        "mo_fee_decision": mo_fee_decision,
        "mo_fee_override": mo_fee_override,
        "status":          status,
    }
    if proposed_start_date is not None:
        payload["proposed_start_date"] = proposed_start_date
    if proposed_end_date is not None:
        payload["proposed_end_date"] = proposed_end_date
    if assigned_engineers is not None:
        payload["assigned_engineers"] = assigned_engineers
    if mo_decision_notes is not None:
        payload["mo_decision_notes"] = mo_decision_notes
    (
        _client()
        .table("intakes")
        .update(payload)
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
    "CD":  "#f59e0b",   # amber-400 (clearly distinct from IFP red)
    "IFP": "#dc2626",   # red-600   (milestone red)
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
    "MK": "President",
    "NK": "Principal",
    "RS": "CAD/BIM Manager",
    "RO": "Project Manager",
    "SW": "Senior CAD Designer",
    "JP": "CAD Designer",
    "JW": "Project Engineer",
    "JR": "Engineer in Training (EIT)",
    "RK": "Engineer in Training (EIT)",
    "NS": "Office Manager",
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
    is_legacy: bool = True      # False = weu_hours-based event from generate_phase_calendar_events
    weu_hours: float = 0.0      # pre-calculated WEU hours for non-legacy events

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
            "is_legacy": self.is_legacy,
            "weu_hours": self.weu_hours,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "title": self.title,
            "color": self.color,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CalendarEventRow":
        is_legacy = d.get("is_legacy")
        if is_legacy is None:
            is_legacy = True  # rows created before this field existed are legacy
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
            is_legacy=bool(is_legacy),
            weu_hours=float(d.get("weu_hours") or 0.0),
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
TARGET_EFFICIENCY_RATIO: float = 200.0  # $/hr — CLAUDE.md V4.0 default

DEFAULT_PHASE_SPLITS: dict[str, float] = {
    "SD":  0.05,
    "50%": 0.20,
    "75%": 0.20,
    "90%": 0.15,
    "IFP": 0.25,
    "CA":  0.10,
    "REV": 0.05,
}

PHASE_ORDER: list[str] = ["SD", "50%", "75%", "90%", "IFP", "CA", "REV"]

PHASE_LABELS: dict[str, str] = {
    "SD":  "Schematic Design",
    "50%": "50% DD",
    "75%": "75% DD",
    "90%": "90% CD",
    "IFP": "Issued for Permit",
    "CA":  "Construction Admin",
    "REV": "Revisions",
}

TEAM_FULL_NAMES: dict[str, str] = {
    "MK": "Mo Kateeb",
    "NK": "Nathan Kline",
    "RS": "Randall Smith",
    "RO": "Ryan Olson",
    "SW": "Scott Webb",
    "JP": "Jesus Prado",
    "JW": "Jacob Wadman",
    "JR": "Josh Robinder",
    "RK": "Rajul Kanth",
    "NS": "Natalie Songco",
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
    rows: list[dict] = []
    budget_per_phase: dict[str, float] = {}
    for phase_code, split in DEFAULT_PHASE_SPLITS.items():
        total_hours = round((approved_fee * split) / BILLING_RATE, 2)
        dollar_budget = round(total_hours * TARGET_EFFICIENCY_RATIO, 2)
        budget_per_phase[phase_code] = dollar_budget
        rows.append({
            "intake_id":    intake_id,
            "project_number": project_number,
            "phase_code":   phase_code,
            "budgeted_hours": total_hours,
            "approved_fee": approved_fee,
            "billing_rate": BILLING_RATE,
            "bucket_allocation": {
                "senior":     round(total_hours * _BUCKET_SPLIT["senior"],     2),
                "production": round(total_hours * _BUCKET_SPLIT["production"], 2),
            },
            "created_at":   now,
            "updated_at":   now,
        })
    (
        _client()
        .table("phase_budgets")
        .upsert(rows, on_conflict="intake_id,phase_code")
        .execute()
    )
    # Denormalize budget_per_phase onto the intake row for fast lookup
    try:
        (
            _client()
            .table("intakes")
            .update({"budget_per_phase": budget_per_phase, "updated_at": now})
            .eq("id", intake_id)
            .execute()
        )
    except Exception:
        pass  # Column requires SQL migration: ALTER TABLE intakes ADD COLUMN budget_per_phase jsonb;


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
        .select("phase_code,hours,engineer_initials")
        .eq("intake_id", intake_id)
        .execute()
    )
    hours_by_phase: dict[str, float] = {}
    bucket_by_phase: dict[str, dict[str, float]] = {}
    for e in (te_resp.data or []):
        p  = e["phase_code"]
        h  = float(e["hours"] or 0)
        rt = e.get("role_type") or _ROLE_BUCKET.get(e.get("engineer_initials", ""), "senior")
        hours_by_phase[p] = hours_by_phase.get(p, 0.0) + h
        bucket_by_phase.setdefault(p, {"senior": 0.0, "production": 0.0})
        bucket_by_phase[p][rt] = bucket_by_phase[p].get(rt, 0.0) + h

    phase_order = list(DEFAULT_PHASE_SPLITS.keys())

    def _sort_key(b: dict) -> int:
        try:
            return phase_order.index(b["phase_code"])
        except ValueError:
            return 99

    result = []
    for b in sorted(budgets, key=_sort_key):
        phase    = b["phase_code"]
        budgeted = float(b["budgeted_hours"])
        used     = round(hours_by_phase.get(phase, 0.0), 2)
        remaining = round(budgeted - used, 2)
        pct      = round((used / budgeted * 100) if budgeted > 0 else 0.0, 1)

        raw_alloc = b.get("bucket_allocation")
        if raw_alloc and isinstance(raw_alloc, dict):
            bucket_alloc = {k: float(v) for k, v in raw_alloc.items()}
        else:
            bucket_alloc = {
                "senior":     round(budgeted * _BUCKET_SPLIT["senior"],     2),
                "production": round(budgeted * _BUCKET_SPLIT["production"], 2),
            }

        phase_spent = bucket_by_phase.get(phase, {"senior": 0.0, "production": 0.0})
        bucket_remaining = {
            role: round(alloc - phase_spent.get(role, 0.0), 2)
            for role, alloc in bucket_alloc.items()
        }

        result.append({
            "id":               b["id"],
            "intake_id":        b["intake_id"],
            "project_number":   b["project_number"],
            "phase_code":       phase,
            "budgeted_hours":   budgeted,
            "approved_fee":     float(b["approved_fee"]),
            "billing_rate":     float(b["billing_rate"]),
            "hours_used":       used,
            "remaining":        remaining,
            "pct_used":         pct,
            "dollar_budget":    round(budgeted * TARGET_EFFICIENCY_RATIO, 2),
            "dollar_burn":      round(used     * TARGET_EFFICIENCY_RATIO, 2),
            "dollar_remaining": round((budgeted - used) * TARGET_EFFICIENCY_RATIO, 2),
            "dollar_pct":       round((used / budgeted * 100) if budgeted > 0 else 0.0, 1),
            "over_budget":      used > budgeted,
            "ready_for_invoicing": (used / budgeted >= 1.0) if budgeted > 0 else False,
            "bucket_allocation":    bucket_alloc,
            "bucket_spent":         {k: round(v, 2) for k, v in phase_spent.items()},
            "bucket_remaining":     bucket_remaining,
        })
    return result


def _sync_budget_per_phase(intake_id: int) -> None:
    """Recompute and write budget_per_phase jsonb to intakes after a phase edit."""
    resp = (
        _client()
        .table("phase_budgets")
        .select("phase_code,budgeted_hours")
        .eq("intake_id", intake_id)
        .execute()
    )
    bpp = {
        row["phase_code"]: round(float(row["budgeted_hours"]) * TARGET_EFFICIENCY_RATIO, 2)
        for row in (resp.data or [])
    }
    try:
        (
            _client()
            .table("intakes")
            .update({"budget_per_phase": bpp, "updated_at": _utc_now_iso()})
            .eq("id", intake_id)
            .execute()
        )
    except Exception:
        pass


def update_phase_budget(intake_id: int, phase_code: str, budgeted_hours: float) -> None:
    (
        _client()
        .table("phase_budgets")
        .update({"budgeted_hours": budgeted_hours, "updated_at": _utc_now_iso()})
        .eq("intake_id", intake_id)
        .eq("phase_code", phase_code)
        .execute()
    )
    _sync_budget_per_phase(intake_id)


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
    role_type: Optional[str] = None,
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


# ── Capacity: intake-based pseudo-events ─────────────────────────────────────

_VALID_PHASE_COEFF_KEYS = frozenset(
    ["50%", "75%", "90%", "IFP", "RFP", "DD", "CA", "CD", "REV", "SD"]
)


def get_active_intake_pseudo_events(covered_project_numbers: set) -> list[dict[str, Any]]:
    """
    Return WEU-compatible event dicts for active intakes that have assigned_engineers
    set but no corresponding calendar event (to avoid double-counting).
    """
    from datetime import date, timedelta

    try:
        resp = (
            _client()
            .table("intakes")
            .select(
                "id,project_number,project_name,client_name,location_region,"
                "assigned_engineers,current_phase,proposed_start_date,proposed_end_date,"
                "mo_fee_override"
            )
            .eq("status", "active")
            .not_.is_("assigned_engineers", "null")
            .execute()
        )
        intakes = resp.data or []
    except Exception:
        return []

    today = date.today().isoformat()
    far_future = (date.today() + timedelta(days=90)).isoformat()
    result = []

    for intake in intakes:
        pn = intake.get("project_number") or ""
        if pn and pn in covered_project_numbers:
            continue  # already represented by a real calendar event

        assigned_raw = intake.get("assigned_engineers")
        if not assigned_raw:
            continue
        try:
            team = json.loads(assigned_raw) if isinstance(assigned_raw, str) else assigned_raw
        except Exception:
            continue
        if not isinstance(team, list) or not team:
            continue

        # Derive tier from approved fee
        fee = float(intake.get("mo_fee_override") or 0)
        if fee >= 100_000:
            tier = 5
        elif fee >= 60_000:
            tier = 4
        elif fee >= 30_000:
            tier = 3
        elif fee >= 15_000:
            tier = 2
        else:
            tier = 3  # safe default

        phase = intake.get("current_phase") or "IFP"
        if phase not in _VALID_PHASE_COEFF_KEYS:
            phase = "IFP"

        start_dt = intake.get("proposed_start_date") or (today + "T00:00:00Z")
        end_dt   = intake.get("proposed_end_date")   or (far_future + "T23:59:59Z")
        if not start_dt.endswith("Z"):
            start_dt += "T00:00:00Z"
        if not end_dt.endswith("Z"):
            end_dt += "T23:59:59Z"

        result.append({
            "id":             f"intake-{intake['id']}",
            "project_number": pn,
            "client":         intake.get("client_name") or "",
            "location":       intake.get("location_region") or "",
            "phase":          phase,
            "team":           team,
            "project_type":   "",
            "tier":           tier,
            "phase_jump":     False,
            "start_date":     start_dt,
            "end_date":       end_dt,
            "is_ooo":         False,
            "title":          f"{pn or '?'} – {intake.get('project_name') or intake.get('client_name', '')}",
        })

    return result


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


# ── Timesheet Submissions ─────────────────────────────────────────────────────

def get_submission(engineer: str, period_start: str) -> Optional[dict[str, Any]]:
    try:
        resp = (
            _client()
            .table("timesheet_submissions")
            .select("*")
            .eq("engineer_initials", engineer)
            .eq("period_start", period_start)
            .maybe_single()
            .execute()
        )
        return resp.data if resp is not None else None
    except Exception:
        return None


def get_or_create_submission(engineer: str, period_start: str, period_end: str) -> dict[str, Any]:
    existing = get_submission(engineer, period_start)
    if existing:
        return existing
    now = _utc_now_iso()
    resp = (
        _client()
        .table("timesheet_submissions")
        .insert({
            "engineer_initials": engineer,
            "period_start":      period_start,
            "period_end":        period_end,
            "status":            "DRAFT",
            "created_at":        now,
            "updated_at":        now,
        })
        .execute()
    )
    return resp.data[0]


_PERIOD_LOCK_SENTINEL = "__PERIOD_LOCK__"


def is_period_globally_locked(period_start: str, period_end: str) -> bool:
    """Return True if an admin has explicitly locked this pay period firm-wide."""
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("id")
        .eq("engineer_initials", _PERIOD_LOCK_SENTINEL)
        .eq("period_start", period_start)
        .limit(1)
        .execute()
    )
    return bool(resp.data)


def lock_pay_period(period_start: str, period_end: str, locked_by: str) -> None:
    """Firm-wide pay period lock. Stored as a sentinel row in timesheet_submissions."""
    now = _utc_now_iso()
    _client().table("timesheet_submissions").upsert(
        {
            "engineer_initials": _PERIOD_LOCK_SENTINEL,
            "period_start":      period_start,
            "period_end":        period_end,
            "status":            "APPROVED",
            "reviewed_by":       locked_by,
            "reviewed_at":       now,
            "total_hours":       0,
        },
        on_conflict="engineer_initials,period_start",
    ).execute()


def is_period_locked(engineer: str, entry_date: str) -> bool:
    """Return True if the pay period covering entry_date is locked for this engineer or globally by admin."""
    # Per-engineer submission lock
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("id")
        .eq("engineer_initials", engineer)
        .lte("period_start", entry_date)
        .gte("period_end", entry_date)
        .in_("status", ["SUBMITTED", "APPROVED"])
        .limit(1)
        .execute()
    )
    if resp.data:
        return True
    # Global period lock (admin-set via Lock Pay Period button)
    resp2 = (
        _client()
        .table("timesheet_submissions")
        .select("id")
        .eq("engineer_initials", _PERIOD_LOCK_SENTINEL)
        .lte("period_start", entry_date)
        .gte("period_end", entry_date)
        .limit(1)
        .execute()
    )
    return bool(resp2.data)


# ── Project Expenses ──────────────────────────────────────────────────────────

def create_expense(
    intake_id: int,
    engineer_initials: str,
    phase: Optional[str],
    amount: float,
    category: str,
    description: Optional[str],
    receipt_url: Optional[str],
    is_reimbursable: bool,
) -> dict[str, Any]:
    now = _utc_now_iso()
    resp = (
        _client()
        .table("project_expenses")
        .insert({
            "intake_id":         intake_id,
            "engineer_initials": engineer_initials,
            "phase":             phase or None,
            "amount":            round(float(amount), 2),
            "category":          category,
            "description":       description or None,
            "receipt_url":       receipt_url or None,
            "status":            "pending",
            "is_reimbursable":   bool(is_reimbursable),
            "created_at":        now,
            "updated_at":        now,
        })
        .execute()
    )
    return resp.data[0] if resp.data else {}


def get_expenses_for_project(intake_id: int) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("project_expenses")
        .select("*")
        .eq("intake_id", intake_id)
        .order("created_at", desc=True)
        .execute()
    )
    return resp.data or []


def get_expenses_for_engineer(engineer_initials: str, limit: int = 20) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("project_expenses")
        .select("*, intakes(project_number, project_name)")
        .eq("engineer_initials", engineer_initials)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    rows = resp.data or []
    for row in rows:
        intake = row.pop("intakes", None) or {}
        row["project_number"] = intake.get("project_number") or "—"
        row["project_name"]   = intake.get("project_name") or "—"
    return rows


def update_expense(expense_id: str, updates: dict[str, Any]) -> dict[str, Any]:
    updates = {**updates, "updated_at": _utc_now_iso()}
    resp = (
        _client()
        .table("project_expenses")
        .update(updates)
        .eq("id", expense_id)
        .execute()
    )
    return resp.data[0] if resp.data else {}


def get_pending_reimbursable_expenses() -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("project_expenses")
        .select("*, intakes(project_number, project_name, client_name)")
        .eq("is_reimbursable", True)
        .in_("status", ["pending", "approved"])
        .order("created_at", desc=True)
        .execute()
    )
    rows = resp.data or []
    result = []
    for row in rows:
        intake = row.pop("intakes", None) or {}
        row["project_number"] = intake.get("project_number") or "—"
        row["project_name"]   = intake.get("project_name") or "—"
        row["client_name"]    = intake.get("client_name") or "—"
        result.append(row)
    return result


def submit_period(engineer: str, period_start: str, period_end: str, total_hours: float) -> dict[str, Any]:
    now = _utc_now_iso()
    existing = get_submission(engineer, period_start)
    if existing:
        resp = (
            _client()
            .table("timesheet_submissions")
            .update({
                "status":       "SUBMITTED",
                "total_hours":  total_hours,
                "submitted_at": now,
                "updated_at":   now,
            })
            .eq("id", existing["id"])
            .execute()
        )
        if resp.data:
            return resp.data[0]
        # Supabase may omit RETURNING rows in some RLS configs — re-fetch
        result = get_submission(engineer, period_start)
        if result:
            return result
        return {**existing, "status": "SUBMITTED", "total_hours": total_hours, "submitted_at": now}
    resp = (
        _client()
        .table("timesheet_submissions")
        .insert({
            "engineer_initials": engineer,
            "period_start":      period_start,
            "period_end":        period_end,
            "status":            "SUBMITTED",
            "total_hours":       total_hours,
            "submitted_at":      now,
            "created_at":        now,
            "updated_at":        now,
        })
        .execute()
    )
    if resp.data:
        return resp.data[0]
    # Re-fetch in case RETURNING was suppressed
    result = get_submission(engineer, period_start)
    if result:
        return result
    raise RuntimeError(f"Failed to create submission for {engineer}/{period_start}")


def review_submission(submission_id: int, action: str, reviewer_notes: Optional[str] = None) -> dict[str, Any]:
    """action must be 'approve' or 'reject'."""
    status = "APPROVED" if action == "approve" else "REJECTED"
    now = _utc_now_iso()
    resp = (
        _client()
        .table("timesheet_submissions")
        .update({
            "status":          status,
            "reviewed_at":     now,
            "reviewer_notes":  reviewer_notes,
            "updated_at":      now,
        })
        .eq("id", submission_id)
        .execute()
    )
    return resp.data[0]


def get_review_queue() -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("*")
        .eq("status", "SUBMITTED")
        .order("submitted_at", desc=False)
        .execute()
    )
    return resp.data or []


def count_pending_review() -> int:
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("id", count="exact")
        .eq("status", "SUBMITTED")
        .execute()
    )
    return resp.count or 0


def get_approved_engineers_for_period(period_start: str, period_end: str) -> set[str]:
    """Return set of engineer_initials with APPROVED submissions covering this period."""
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("engineer_initials")
        .eq("status", "APPROVED")
        .eq("period_start", period_start)
        .execute()
    )
    return {row["engineer_initials"] for row in (resp.data or [])}


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
        status = "pending"
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


def auto_trigger_billing_phase(intake_id: int, billing_phase_code: str) -> bool:
    """Marks a billing phase as complete_pending_approval when hours reach budget.
    Returns True if newly triggered, False if already triggered or not applicable."""
    pbp_resp = (
        _client()
        .table("project_billing_phases")
        .select("id,status")
        .eq("intake_id", intake_id)
        .eq("billing_phase_code", billing_phase_code)
        .maybe_single()
        .execute()
    )
    pbp = pbp_resp.data
    if not pbp or pbp.get("status") != "pending":
        return False
    now = _utc_now_iso()
    (
        _client()
        .table("project_billing_phases")
        .update({
            "status":               "complete_pending_approval",
            "phase_completed_at":   now,
            "phase_completed_note": "Auto-triggered: phase hours reached budget",
            "updated_at":           now,
        })
        .eq("intake_id", intake_id)
        .eq("billing_phase_code", billing_phase_code)
        .execute()
    )
    return True


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
    """Returns all pipeline_active projects grouped by current_billing_phase, plus PROPOSAL_OUT bids."""
    intakes_resp = (
        _client()
        .table("intakes")
        .select(
            "id,project_number,project_name,client_name,location_region,"
            "current_production_phase,current_billing_phase,pipeline_active,"
            "change_order_pending,change_order_note,mo_fee_override"
        )
        .eq("pipeline_active", 1)
        .neq("status", "PROPOSAL_OUT")   # proposals belong in the Proposal Submitted column only
        .order("project_number")
        .execute()
    )
    intakes = intakes_resp.data or []

    # Fetch active bids (PROPOSAL_OUT) separately for the proposals column
    active_bids = get_active_bids()

    if not intakes and not active_bids:
        return _empty_pipeline()

    intake_ids = [i["id"] for i in intakes]
    pbp_by_intake: dict[int, list[dict]]   = {}
    budget_by_intake: dict[int, float]     = {}
    budget_by_phase: dict[tuple, float]    = {}
    actual_by_intake: dict[int, float]     = {}
    actual_by_phase: dict[tuple, float]    = {}
    team_by_pn: dict[str, list[str]]       = {}

    if intake_ids:
        # billing phases
        pbp_resp = (
            _client()
            .table("project_billing_phases")
            .select("intake_id,billing_phase_code,fee_amount,fee_pct,status,change_order_pending,invoice_fee_override")
            .in_("intake_id", intake_ids)
            .execute()
        )
        for row in (pbp_resp.data or []):
            pbp_by_intake.setdefault(int(row["intake_id"]), []).append(row)

        # phase budgets — total and per-phase budgeted hours
        pb_resp = (
            _client()
            .table("phase_budgets")
            .select("intake_id,phase_code,budgeted_hours")
            .in_("intake_id", intake_ids)
            .execute()
        )
        for row in (pb_resp.data or []):
            iid = int(row["intake_id"])
            hrs = float(row["budgeted_hours"])
            budget_by_intake[iid] = budget_by_intake.get(iid, 0.0) + hrs
            budget_by_phase[(iid, row["phase_code"])] = hrs

        # actual hours per project and per phase
        te_resp = (
            _client()
            .table("time_entries")
            .select("intake_id,hours,phase_code")
            .in_("intake_id", intake_ids)
            .execute()
        )
        for row in (te_resp.data or []):
            iid = int(row["intake_id"])
            hrs = float(row["hours"])
            actual_by_intake[iid] = actual_by_intake.get(iid, 0.0) + hrs
            pc = row.get("phase_code") or ""
            actual_by_phase[(iid, pc)] = actual_by_phase.get((iid, pc), 0.0) + hrs

        # calendar team members per project
        project_numbers = [i["project_number"] for i in intakes if i.get("project_number")]
        if project_numbers:
            cal_resp = (
                _client()
                .table("calendar_events")
                .select("project_number,team")
                .in_("project_number", project_numbers)
                .execute()
            )
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
        "proposals": {"label": "Proposal Submitted", "projects": [], "is_proposals": True},
        "retainer":  {"label": "Retainer",              "projects": []},
        "SD":        {"label": "Schematic Design",      "projects": []},
        "DD":        {"label": "Design Development",    "projects": []},
        "CD":        {"label": "Construction Documents","projects": []},
        "CA":        {"label": "Construction Admin",    "projects": []},
        "complete":  {"label": "Complete",              "projects": []},
    }

    # Populate proposal cards
    for bid in active_bids:
        columns["proposals"]["projects"].append(bid)

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
        cur_prod = intake.get("current_production_phase") or "SD"
        # Aggregate budget/actual by billing column, not by raw production phase code,
        # because phase_budgets uses milestone codes (50%, 75%, etc.) that don't match
        # the production phase codes stored in current_production_phase.
        _budget_map: dict[str, list[str]] = {
            "SD": ["SD"],
            "DD": ["50%", "75%"],
            "CD": ["90%", "IFP"],
            "CA": ["CA", "REV"],
        }
        _actual_map: dict[str, list[str]] = {
            "SD": ["SD", "RFP"],
            "DD": ["DD", "50%", "75%"],
            "CD": ["90%", "CD", "IFP"],
            "CA": ["CA", "REV"],
        }
        budget_codes = _budget_map.get(billing_col, [])
        actual_codes = _actual_map.get(billing_col, [])
        phase_budgeted = round(sum(budget_by_phase.get((iid, pc), 0.0) for pc in budget_codes), 1)
        phase_actual   = round(sum(actual_by_phase.get((iid, pc), 0.0) for pc in actual_codes), 1)

        project = {
            "intake_id":              iid,
            "project_number":         pn,
            "project_name":           intake["project_name"],
            "client":                 intake.get("client_name") or "",
            "current_production_phase": cur_prod,
            "current_billing_phase":  billing_col,
            "billing_fee":            billing_fee,
            "billing_fee_pct":        billing_fee_pct,
            "team":                   team_by_pn.get(pn, []),
            "budgeted_hours":         budgeted,
            "actual_hours":           actual,
            "phase_budgeted_hours":   phase_budgeted,
            "phase_actual_hours":     phase_actual,
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
        for p in col["projects"] if p.get("invoice_status") == "complete_pending_approval"
    )

    return {
        "columns": columns,
        "stats": {
            "active_projects":   len(intakes),
            "pending_invoices":  pending_invoices,
            "total_pipeline":    round(total_fee, 2),
            "active_bids":       len(active_bids),
            "stale_bids":        sum(1 for b in active_bids if b.get("is_stale")),
        },
    }


def _empty_pipeline() -> dict[str, Any]:
    return {
        "columns": {
            "proposals": {"label": "Proposal Submitted", "projects": [], "is_proposals": True},
            "retainer":  {"label": "Retainer",              "projects": []},
            "SD":        {"label": "Schematic Design",      "projects": []},
            "DD":        {"label": "Design Development",    "projects": []},
            "CD":        {"label": "Construction Documents","projects": []},
            "CA":        {"label": "Construction Admin",    "projects": []},
            "complete":  {"label": "Complete",              "projects": []},
        },
        "stats": {"active_projects": 0, "pending_invoices": 0, "total_pipeline": 0, "active_bids": 0, "stale_bids": 0},
    }


# ── Time-Off CRUD ─────────────────────────────────────────────────────────────

TIME_OFF_REASONS = ["Vacation", "Sick Leave", "Company Holiday", "Conference/PD", "Personal", "Other"]


def create_time_off(
    *,
    engineer_initials: str,
    start_date: str,
    end_date: str,
    reason: str = "Vacation",
    notes: Optional[str] = None,
    created_by: Optional[str] = None,
) -> int:
    now = _utc_now_iso()
    resp = (
        _client()
        .table("time_off")
        .insert({
            "engineer_initials": engineer_initials.strip().upper(),
            "start_date":        start_date,
            "end_date":          end_date,
            "reason":            reason,
            "notes":             notes,
            "created_by":        created_by,
            "created_at":        now,
            "updated_at":        now,
        })
        .execute()
    )
    return int(resp.data[0]["id"])


def list_time_off(
    *,
    engineer: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> list[dict[str, Any]]:
    q = (
        _client()
        .table("time_off")
        .select("*")
        .order("start_date", desc=False)
        .order("id", desc=False)
    )
    if engineer:
        q = q.eq("engineer_initials", engineer.strip().upper())
    if start:
        q = q.gte("end_date", start)
    if end:
        q = q.lte("start_date", end)
    resp = q.execute()
    rows = resp.data or []
    # Annotate with working_days count
    for r in rows:
        try:
            s = date.fromisoformat(r["start_date"])
            e = date.fromisoformat(r["end_date"])
            r["working_days"] = count_working_days(s, e)
        except (ValueError, KeyError):
            r["working_days"] = 0
    return rows


def delete_time_off(time_off_id: int) -> None:
    _client().table("time_off").delete().eq("id", time_off_id).execute()


def count_upcoming_ooo(days: int = 30) -> int:
    today = date.today()
    future = today + timedelta(days=days)
    resp = (
        _client()
        .table("time_off")
        .select("id", count="exact")
        .lte("start_date", future.isoformat())
        .gte("end_date", today.isoformat())
        .execute()
    )
    return resp.count or 0


# ── Projected Capacity Engine ─────────────────────────────────────────────────

# WEU constants (mirrors weu.py to avoid circular import)
_PHASE_COEFF: dict[str, float] = {
    "50%": 1.0, "75%": 1.2, "90%": 1.5, "IFP": 0.4,
    "RFP": 0.3, "DD": 0.6, "CA": 0.8, "CD": 1.0, "REV": 0.5, "SD": 0.2,
}
_CAPACITY_BASE = 10.0
_TEAM_MULTIPLIER: dict[str, float] = {
    "MK": 0.2, "NK": 1.0, "RO": 1.0, "JW": 1.0,
    "RS": 1.0, "SW": 1.0, "JP": 0.8, "JR": 0.8, "RK": 0.8,
}
ENGINEERING_POOL = ["MK", "NK", "RO", "JW", "JR", "RK"]
DRAFTING_POOL    = ["RS", "SW", "JP"]

# Role bucket — mirrors weu.ROLE_BUCKETS; defined here to avoid circular import
_ROLE_BUCKET: dict[str, str] = {
    **{eng: "senior"     for eng in ENGINEERING_POOL},
    **{eng: "production" for eng in DRAFTING_POOL},
}
_BUCKET_SPLIT: dict[str, float] = {"senior": 0.40, "production": 0.60}


def count_working_days(start: date, end: date) -> int:
    total = 0
    current = start
    while current <= end:
        if current.weekday() < 5:
            total += 1
        current += timedelta(days=1)
    return total


def infer_tier_from_intake(intake: "IntakeRow") -> int:
    """
    Derive a WEU tier (1–5) from the approved fee and complexity signals.

    Tier scale:
      1 – Minor assessment / sub-$15k
      2 – Small project  $15k–$30k
      3 – Standard       $30k–$60k   (most TI and new-construction)
      4 – Large          $60k–$100k
      5 – Major          $100k+
    """
    fee = float(getattr(intake, "mo_fee_override", None) or 0)
    if fee >= 100_000:
        tier = 5
    elif fee >= 60_000:
        tier = 4
    elif fee >= 30_000:
        tier = 3
    elif fee >= 15_000:
        tier = 2
    else:
        tier = 2  # minimum for any approved project

    # Bump by one for explicitly high-complexity flags (cap at 5)
    try:
        answers = intake.answers  # parsed property
        complexity = str(answers.get("complexity_estimate") or "").lower()
        project_type = str(answers.get("project_type") or "").lower()
        if "high" in complexity or "complex" in complexity:
            tier = min(tier + 1, 5)
        if project_type in ("healthcare", "historic", "high_rise"):
            tier = min(tier + 1, 5)
    except Exception:
        pass

    return tier


def generate_phase_calendar_events(
    intake_id: int,
    project_number: str,
    start_date: str,
    ifp_date: str,
    team: list[str],
    weu_hours: float = 40.0,
    replace_existing: bool = True,
    tier: Optional[int] = None,
) -> list[dict]:
    """
    Generate one calendar_events row per phase for a project.
    IFP phase ends exactly on ifp_date. Pre-IFP phases are distributed
    backward from ifp_date; CA and REV are distributed forward from it.
    Returns the list of generated event dicts.
    """
    start = date.fromisoformat(start_date)
    ifp   = date.fromisoformat(ifp_date)
    total_days = (ifp - start).days
    if total_days <= 0:
        raise ValueError(f"start_date {start_date} must be before ifp_date {ifp_date}")

    pre_ifp  = ["SD", "50%", "75%", "90%", "IFP"]
    post_ifp = ["CA", "REV"]

    pre_total  = sum(DEFAULT_PHASE_SPLITS[p] for p in pre_ifp)
    post_total = sum(DEFAULT_PHASE_SPLITS[p] for p in post_ifp)
    post_days  = int(total_days * (post_total / pre_total))

    now = _utc_now_iso()
    events: list[dict] = []
    cursor = start

    for i, phase in enumerate(pre_ifp):
        split = DEFAULT_PHASE_SPLITS[phase] / pre_total
        if i == len(pre_ifp) - 1:
            phase_start, phase_end = cursor, ifp
        else:
            phase_days  = int(total_days * split)
            phase_start = cursor
            phase_end   = cursor + timedelta(days=max(phase_days - 1, 0))

        events.append({
            "intake_id":      intake_id,
            "project_number": project_number,
            "phase_code":     phase,
            "phase_label":    PHASE_LABELS[phase],
            "phase":          phase,
            "start_date":     phase_start.isoformat() + "T00:00:00Z",
            "end_date":       phase_end.isoformat()   + "T23:59:59Z",
            "weu_hours":      round(weu_hours * DEFAULT_PHASE_SPLITS[phase], 1),
            "tier":           tier,
            "team":           team,
            "is_legacy":      False,
            "is_ooo":         False,
            "client":         "",
            "location":       "",
            "project_type":   "",
            "created_at":     now,
            "updated_at":     now,
        })

        if i < len(pre_ifp) - 1:
            cursor = phase_end + timedelta(days=1)

    cursor = ifp + timedelta(days=1)
    for phase in post_ifp:
        split      = DEFAULT_PHASE_SPLITS[phase] / post_total
        phase_days = int(post_days * split)
        phase_start = cursor
        phase_end   = cursor + timedelta(days=max(phase_days - 1, 0))

        events.append({
            "intake_id":      intake_id,
            "project_number": project_number,
            "phase_code":     phase,
            "phase_label":    PHASE_LABELS[phase],
            "phase":          phase,
            "start_date":     phase_start.isoformat() + "T00:00:00Z",
            "end_date":       phase_end.isoformat()   + "T23:59:59Z",
            "weu_hours":      round(weu_hours * DEFAULT_PHASE_SPLITS[phase], 1),
            "tier":           tier,
            "team":           team,
            "is_legacy":      False,
            "is_ooo":         False,
            "client":         "",
            "location":       "",
            "project_type":   "",
            "created_at":     now,
            "updated_at":     now,
        })
        cursor = phase_end + timedelta(days=1)

    if replace_existing:
        _client().table("calendar_events") \
            .delete() \
            .eq("intake_id", intake_id) \
            .eq("is_legacy", False) \
            .execute()

    if events:
        _client().table("calendar_events").insert(events).execute()

    return events


def list_phase_calendar_events(
    year: int,
    month: int,
) -> list[dict]:
    """
    Return non-legacy phase-span events that overlap the 3-month window
    centred on (year, month) — for the Gantt calendar view.
    """
    from calendar import monthrange as _mr
    first = date(year, month, 1)
    # previous month start
    if month > 1:
        prev_start = date(year, month - 1, 1)
    else:
        prev_start = date(year - 1, 12, 1)
    # next month end
    if month < 12:
        next_end = date(year, month + 1, _mr(year, month + 1)[1])
    else:
        next_end = date(year + 1, 1, 31)

    resp = (
        _client()
        .table("calendar_events")
        .select("id,intake_id,project_number,phase_code,phase_label,start_date,end_date,weu_hours,team")
        .eq("is_legacy", False)
        .eq("is_ooo", False)
        .gte("end_date",   prev_start.isoformat())
        .lte("start_date", next_end.isoformat())
        .order("start_date")
        .execute()
    )
    rows = []
    for r in (resp.data or []):
        team = r.get("team") or []
        if isinstance(team, str):
            try:
                team = json.loads(team)
            except Exception:
                team = [t.strip() for t in team.split(",") if t.strip()]
        rows.append({
            "id":             r["id"],
            "intake_id":      r.get("intake_id"),
            "project_number": r.get("project_number") or "",
            "phase_code":     r.get("phase_code") or "",
            "phase_label":    r.get("phase_label") or r.get("phase_code") or "",
            "start_date":     (r.get("start_date") or "")[:10],
            "end_date":       (r.get("end_date")   or "")[:10],
            "weu_hours":      r.get("weu_hours") or 0.0,
            "team":           team,
        })
    return rows


def _count_ooo_days(engineer_initials: str, window_start: date, window_end: date) -> int:
    resp = (
        _client()
        .table("time_off")
        .select("start_date,end_date")
        .eq("engineer_initials", engineer_initials)
        .lte("start_date", window_end.isoformat())
        .gte("end_date", window_start.isoformat())
        .execute()
    )
    ooo_days = 0
    for entry in (resp.data or []):
        try:
            overlap_start = max(date.fromisoformat(entry["start_date"]), window_start)
            overlap_end   = min(date.fromisoformat(entry["end_date"]),   window_end)
            ooo_days += count_working_days(overlap_start, overlap_end)
        except (ValueError, KeyError):
            pass
    return ooo_days


def _get_existing_load_hours(engineer_initials: str, window_start: date, window_end: date) -> float:
    resp = (
        _client()
        .table("calendar_events")
        .select("start_date,end_date,phase,tier,team,phase_jump,is_ooo,is_legacy,weu_hours")
        .lte("start_date", window_end.isoformat() + "T23:59:59Z")
        .gte("end_date",   window_start.isoformat())
        .execute()
    )
    total = 0.0
    for event in (resp.data or []):
        if event.get("is_ooo"):
            continue
        team = event.get("team") or []
        if isinstance(team, str):
            try:
                team = json.loads(team)
            except Exception:
                team = [t.strip() for t in team.split(",")]
        if engineer_initials not in team:
            continue
        try:
            ev_start = date.fromisoformat(event["start_date"][:10])
            ev_end   = date.fromisoformat(event["end_date"][:10])
        except (ValueError, KeyError):
            continue
        overlap_start = max(ev_start, window_start)
        overlap_end   = min(ev_end,   window_end)
        ev_working   = max(count_working_days(ev_start, ev_end), 1)
        ov_working   = count_working_days(overlap_start, overlap_end)
        if ov_working <= 0:
            continue

        is_legacy = event.get("is_legacy")
        if is_legacy is None:
            is_legacy = True  # treat unknown rows as legacy

        if not is_legacy:
            # New phase-span events: weu_hours is total team hours for the phase.
            # Attribute each engineer's share pro-rated over the overlap window.
            weu = float(event.get("weu_hours") or 0.0)
            n_team = max(len(team), 1)
            total += (weu / n_team) * (ov_working / ev_working)
        else:
            # Legacy events: tier-based WEU formula
            tier = event.get("tier") or 0
            phase_coeff = _PHASE_COEFF.get(event.get("phase") or "", 0.5)
            person_mult = _TEAM_MULTIPLIER.get(engineer_initials, 1.0)
            qa = 1.15 if event.get("phase_jump") else 1.0
            weu_rate = tier * phase_coeff * person_mult * qa / _CAPACITY_BASE
            total += weu_rate * ov_working * 8.0
    return total


def get_remaining_resourced_hours(project_number: str, from_date: date) -> float:
    """
    Total team-hours from future calendar events for this project, pro-rated from from_date.
    Handles both legacy (tier-based) and new phase-span (weu_hours-based) events.
    """
    resp = (
        _client()
        .table("calendar_events")
        .select("start_date,end_date,phase,tier,team,phase_jump,is_ooo,is_legacy,weu_hours")
        .eq("project_number", project_number)
        .gte("end_date", from_date.isoformat())
        .execute()
    )
    total = 0.0
    for event in (resp.data or []):
        if event.get("is_ooo"):
            continue
        try:
            ev_start = date.fromisoformat(event["start_date"][:10])
            ev_end   = date.fromisoformat(event["end_date"][:10])
        except (ValueError, KeyError):
            continue
        effective_start = max(ev_start, from_date)
        if effective_start > ev_end:
            continue
        ev_working  = max(count_working_days(ev_start, ev_end), 1)
        rem_working = count_working_days(effective_start, ev_end)
        if rem_working <= 0:
            continue

        is_legacy = event.get("is_legacy")
        if is_legacy is None:
            is_legacy = True

        if not is_legacy:
            weu = float(event.get("weu_hours") or 0.0)
            total += weu * (rem_working / ev_working)
        else:
            tier = event.get("tier") or 0
            if not tier:
                continue
            phase_coeff = _PHASE_COEFF.get(event.get("phase") or "", 0.5)
            qa = 1.15 if event.get("phase_jump") else 1.0
            team = event.get("team") or []
            if isinstance(team, str):
                try:
                    team = json.loads(team)
                except Exception:
                    team = [t.strip() for t in team.split(",") if t.strip()]
            for eng in team:
                person_mult = _TEAM_MULTIPLIER.get(eng, 1.0)
                weu_rate = tier * phase_coeff * person_mult * qa / _CAPACITY_BASE
                total += weu_rate * rem_working * 8.0
    return total


def get_projected_capacity(
    engineer_initials: str, window_start: date, window_end: date
) -> dict[str, Any]:
    working_days  = count_working_days(window_start, window_end)
    ooo_days      = _count_ooo_days(engineer_initials, window_start, window_end)
    available_days = max(working_days - ooo_days, 0)
    available_hours = available_days * 8.0
    committed_hours = _get_existing_load_hours(engineer_initials, window_start, window_end)
    if available_hours > 0:
        utilization_pct = round(committed_hours / available_hours * 100, 1)
    else:
        utilization_pct = 100.0
    ooo_bar_pct = round(ooo_days / working_days * 100, 1) if working_days > 0 else 0.0
    return {
        "engineer_initials": engineer_initials,
        "window_start":      window_start.isoformat(),
        "window_end":        window_end.isoformat(),
        "working_days":      working_days,
        "ooo_days":          ooo_days,
        "available_days":    available_days,
        "available_hours":   available_hours,
        "committed_hours":   round(committed_hours, 1),
        "utilization_pct":   utilization_pct,
        "has_ooo":           ooo_days > 0,
        "ooo_bar_pct":       ooo_bar_pct,
        "role":              {
            "MK": "President, PE", "NK": "PE",            "RO": "Eng/PM, PE",
            "JW": "Project Eng",   "RS": "CAD Mgr",       "SW": "Sr CAD",
            "JP": "CAD Designer",  "JR": "EIT",           "RK": "EIT",
        }.get(engineer_initials, ""),
    }


def get_all_projected_capacity(window_start: date, window_end: date) -> dict[str, Any]:
    return {
        "window_start":      window_start.isoformat(),
        "window_end":        window_end.isoformat(),
        "window_days":       count_working_days(window_start, window_end),
        "engineering_pool":  [get_projected_capacity(m, window_start, window_end) for m in ENGINEERING_POOL],
        "drafting_pool":     [get_projected_capacity(m, window_start, window_end) for m in DRAFTING_POOL],
    }


# ── Schedule Generator ────────────────────────────────────────────────────────

PHASE_WEIGHTS: dict[str, dict[str, float]] = {
    "new_construction":      {"SD": 0.15, "DD": 0.20, "IFP": 0.40, "CA": 0.25},
    "tenant_improvement":    {"SD": 0.10, "DD": 0.20, "IFP": 0.50, "CA": 0.20},
    "addition_expansion":    {"SD": 0.15, "DD": 0.20, "IFP": 0.40, "CA": 0.25},
    "build_to_suit_retrofit": {"SD": 0.10, "DD": 0.25, "IFP": 0.45, "CA": 0.20},
    "repeating_program":     {"SD": 0.10, "DD": 0.20, "IFP": 0.50, "CA": 0.20},
    "one_off_unique":        {"SD": 0.15, "DD": 0.25, "IFP": 0.40, "CA": 0.20},
}
DEFAULT_PHASE_WEIGHTS: dict[str, float] = {"SD": 0.15, "DD": 0.20, "IFP": 0.45, "CA": 0.20}


def calculate_phase_schedule(
    project_start: date, project_end: date, project_type: str
) -> list[dict[str, Any]]:
    weights = PHASE_WEIGHTS.get(project_type, DEFAULT_PHASE_WEIGHTS)
    total_days = (project_end - project_start).days
    if total_days <= 0:
        raise ValueError("project_end must be after project_start")
    phases = []
    current = project_start
    phase_list = list(weights.items())
    for i, (code, weight) in enumerate(phase_list):
        is_last = i == len(phase_list) - 1
        if is_last:
            phase_end = project_end
        else:
            duration = max(round(total_days * weight), 1)
            phase_end = current + timedelta(days=duration - 1)
            # Ensure remaining phases each get at least 1 day
            remaining = len(phase_list) - i - 1
            phase_end = min(phase_end, project_end - timedelta(days=remaining))
        phases.append({
            "phase_code":    code,
            "start_date":    current.isoformat(),
            "end_date":      phase_end.isoformat(),
            "duration_days": (phase_end - current).days + 1,
        })
        current = phase_end + timedelta(days=1)
    return phases


# ── Burn Health Batch Engine ──────────────────────────────────────────────────

_TERMINAL_STATUSES = {"DECLINED", "COMPLETED", "ARCHIVED"}


def get_burn_health_data(today: date) -> list[dict[str, Any]]:
    """
    Batch projected burn for all active projects.
    Makes 4 Supabase calls (vs N per-project calls) by fetching all data up front.
    Returns list sorted by projected_burn_pct descending (highest risk first).
    """
    all_intakes = list_intakes()
    active = [
        i for i in all_intakes
        if i.project_number and i.status not in _TERMINAL_STATUSES
    ]
    if not active:
        return []

    intake_ids  = [i.id           for i in active]
    project_nums = [i.project_number for i in active]

    # Approved fee — first phase_budget row per intake (same fee on all rows)
    pb_resp = (
        _client()
        .table("phase_budgets")
        .select("intake_id,approved_fee")
        .in_("intake_id", intake_ids)
        .execute()
    )
    approved_fee_by_intake: dict[int, float] = {}
    for row in (pb_resp.data or []):
        iid = row["intake_id"]
        if iid not in approved_fee_by_intake:
            approved_fee_by_intake[iid] = float(row["approved_fee"] or 0)

    # Keep only intakes that have a positive approved fee
    active = [i for i in active if approved_fee_by_intake.get(i.id, 0) > 0]
    if not active:
        return []

    intake_ids   = [i.id             for i in active]
    project_nums = [i.project_number for i in active]

    # Potential burn hours: only count entries from SUBMITTED or APPROVED periods
    submitted_periods = _get_submitted_periods()
    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id,hours,engineer_initials,entry_date")
        .in_("intake_id", intake_ids)
        .execute()
    )
    hours_by_intake: dict[int, float] = {}
    for e in (te_resp.data or []):
        iid = e["intake_id"]
        if _entry_in_submitted_period(e.get("entry_date", ""), e.get("engineer_initials", ""), submitted_periods):
            hours_by_intake[iid] = hours_by_intake.get(iid, 0.0) + float(e["hours"] or 0)

    # Future calendar events for all project numbers — handles both legacy and phase-span events
    ce_resp = (
        _client()
        .table("calendar_events")
        .select("project_number,start_date,end_date,phase,tier,team,phase_jump,is_ooo,is_legacy,weu_hours")
        .in_("project_number", project_nums)
        .gte("end_date", today.isoformat())
        .execute()
    )
    remaining_by_project: dict[str, float] = {}
    for event in (ce_resp.data or []):
        if event.get("is_ooo"):
            continue
        pnum = event.get("project_number")
        if not pnum:
            continue
        try:
            ev_start = date.fromisoformat(event["start_date"][:10])
            ev_end   = date.fromisoformat(event["end_date"][:10])
        except (ValueError, KeyError):
            continue
        effective_start = max(ev_start, today)
        if effective_start > ev_end:
            continue
        ev_working  = max(count_working_days(ev_start, ev_end), 1)
        rem_working = count_working_days(effective_start, ev_end)
        if rem_working <= 0:
            continue

        is_legacy = event.get("is_legacy")
        if is_legacy is None:
            is_legacy = True

        if not is_legacy:
            weu = float(event.get("weu_hours") or 0.0)
            remaining_by_project[pnum] = (
                remaining_by_project.get(pnum, 0.0) + weu * (rem_working / ev_working)
            )
        else:
            tier = event.get("tier") or 0
            if not tier:
                continue
            phase_coeff = _PHASE_COEFF.get(event.get("phase") or "", 0.5)
            qa = 1.15 if event.get("phase_jump") else 1.0
            team = event.get("team") or []
            if isinstance(team, str):
                try:
                    team = json.loads(team)
                except Exception:
                    team = [t.strip() for t in team.split(",") if t.strip()]
            for eng in team:
                person_mult = _TEAM_MULTIPLIER.get(eng, 1.0)
                weu_rate = tier * phase_coeff * person_mult * qa / _CAPACITY_BASE
                remaining_by_project[pnum] = (
                    remaining_by_project.get(pnum, 0.0) + weu_rate * rem_working * 8.0
                )

    results: list[dict[str, Any]] = []
    for i in active:
        approved_fee    = approved_fee_by_intake.get(i.id, 0.0)
        current_hours   = hours_by_intake.get(i.id, 0.0)
        remaining_hours = remaining_by_project.get(i.project_number, 0.0)

        current_burn_val   = round(current_hours   * TARGET_EFFICIENCY_RATIO, 2)
        remaining_val      = round(remaining_hours * TARGET_EFFICIENCY_RATIO, 2)
        projected_burn_val = round(current_burn_val + remaining_val, 2)

        def _pct(v: float, total: float = approved_fee) -> float:
            return round(v / total * 100, 1) if total > 0 else 0.0

        current_pct   = _pct(current_burn_val)
        projected_pct = _pct(projected_burn_val)

        if projected_pct > 100:
            risk = "over_budget"
        elif projected_pct >= 85:
            risk = "at_risk"
        elif projected_pct >= 70:
            risk = "watch"
        else:
            risk = "on_track"

        days_remaining = None
        if i.proposed_end_date:
            try:
                end = date.fromisoformat(i.proposed_end_date[:10])
                days_remaining = (end - today).days
            except ValueError:
                pass

        results.append({
            "intake_id":                 i.id,
            "project_number":            i.project_number,
            "project_name":              i.project_name,
            "client":                    i.client_name,
            "status":                    i.status,
            "approved_fee":              approved_fee,
            "current_burn_hours":        round(current_hours, 1),
            "current_burn_value":        current_burn_val,
            "current_burn_pct":          current_pct,
            "remaining_resourced_hours": round(remaining_hours, 1),
            "projected_burn_value":      projected_burn_val,
            "projected_burn_pct":        projected_pct,
            "remaining_budget":          round(approved_fee - projected_burn_val, 2),
            "risk":                      risk,
            "days_remaining":            days_remaining,
        })

    results.sort(key=lambda x: x["projected_burn_pct"], reverse=True)
    return results


def count_burn_at_risk(today: date) -> int:
    """
    Lightweight proxy for the nav badge: count active projects where current
    logged burn already exceeds 70% of approved fee, without the WEU calendar scan.
    """
    all_intakes = list_intakes()
    active = [
        i for i in all_intakes
        if i.project_number and i.status not in _TERMINAL_STATUSES
    ]
    if not active:
        return 0

    intake_ids = [i.id for i in active]

    pb_resp = (
        _client()
        .table("phase_budgets")
        .select("intake_id,approved_fee")
        .in_("intake_id", intake_ids)
        .execute()
    )
    approved_fee_by_intake: dict[int, float] = {}
    for row in (pb_resp.data or []):
        iid = row["intake_id"]
        if iid not in approved_fee_by_intake:
            approved_fee_by_intake[iid] = float(row["approved_fee"] or 0)

    active = [i for i in active if approved_fee_by_intake.get(i.id, 0) > 0]
    if not active:
        return 0

    intake_ids = [i.id for i in active]

    submitted_periods = _get_submitted_periods()
    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id,hours,engineer_initials,entry_date")
        .in_("intake_id", intake_ids)
        .execute()
    )
    hours_by_intake: dict[int, float] = {}
    for e in (te_resp.data or []):
        iid = e["intake_id"]
        if _entry_in_submitted_period(e.get("entry_date", ""), e.get("engineer_initials", ""), submitted_periods):
            hours_by_intake[iid] = hours_by_intake.get(iid, 0.0) + float(e["hours"] or 0)

    count = 0
    for i in active:
        fee = approved_fee_by_intake.get(i.id, 0.0)
        burn = hours_by_intake.get(i.id, 0.0) * TARGET_EFFICIENCY_RATIO
        if fee > 0 and burn >= fee * 0.70:
            count += 1
    return count


# ── Engineer Bucket View (Launch Page) ───────────────────────────────────────

def get_engineer_bucket_view(engineer: str) -> dict[str, Any]:
    """
    Batch-fetch the data needed for the Engineer Launch Page.
    Returns, per active project, each phase's bucket allocation and
    how much has been spent by role, so the engineer can see exactly
    how many hours remain in their Senior or Production bucket.

    Makes 4 Supabase calls regardless of project count.
    """
    role = _ROLE_BUCKET.get(engineer, "senior")
    projects = list_active_projects(engineer)
    if not projects:
        return {
            "engineer": engineer,
            "name":     TEAM_FULL_NAMES.get(engineer, engineer),
            "role":     role,
            "projects": [],
        }

    intake_ids   = [p["intake_id"]      for p in projects if p.get("intake_id")]
    project_nums = [p["project_number"] for p in projects if p.get("project_number")]

    # Batch 1: phase budgets (with bucket_allocation)
    pb_resp = (
        _client()
        .table("phase_budgets")
        .select("*")
        .in_("intake_id", intake_ids)
        .execute()
    )
    budgets_by_intake: dict[int, list[dict]] = {}
    for pb in (pb_resp.data or []):
        iid = int(pb["intake_id"])
        budgets_by_intake.setdefault(iid, [])
        budgets_by_intake[iid].append(pb)

    # Batch 2: all time entries for these projects
    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id,phase_code,hours,engineer_initials")
        .in_("intake_id", intake_ids)
        .execute()
    )
    # (intake_id, phase_code, role_type) → total hours
    bucket_spent_map: dict[tuple[int, str, str], float] = {}
    # (intake_id, phase_code) → hours logged by THIS engineer
    own_spent_map: dict[tuple[int, str], float] = {}
    for e in (te_resp.data or []):
        iid   = int(e["intake_id"])
        phase = e.get("phase_code", "")
        h     = float(e.get("hours") or 0)
        rt    = e.get("role_type") or _ROLE_BUCKET.get(e.get("engineer_initials", ""), "senior")
        key   = (iid, phase, rt)
        bucket_spent_map[key] = bucket_spent_map.get(key, 0.0) + h
        if e.get("engineer_initials") == engineer:
            ok = (iid, phase)
            own_spent_map[ok] = own_spent_map.get(ok, 0.0) + h

    # Batch 3: current/upcoming calendar events to determine active phase
    today_iso = date.today().isoformat() + "T00:00:00Z"
    ce_resp = (
        _client()
        .table("calendar_events")
        .select("project_number,phase,start_date,end_date")
        .in_("project_number", project_nums)
        .gte("end_date", today_iso)
        .neq("is_ooo", True)
        .order("start_date", desc=False)
        .execute()
    )
    current_phase_by_pn: dict[str, str] = {}
    for ev in (ce_resp.data or []):
        pn = ev.get("project_number", "")
        if pn and pn not in current_phase_by_pn:
            current_phase_by_pn[pn] = ev.get("phase", "")

    # Build per-project result
    result_projects: list[dict] = []
    for proj in projects:
        iid = proj.get("intake_id")
        if not iid:
            continue
        pn            = proj.get("project_number", "")
        current_phase = current_phase_by_pn.get(pn, "")
        budgets       = budgets_by_intake.get(iid, [])

        def _sort_phase(b: dict) -> int:
            try:
                return PHASE_ORDER.index(b["phase_code"])
            except ValueError:
                return 99

        phases: list[dict] = []
        for pb in sorted(budgets, key=_sort_phase):
            phase        = pb["phase_code"]
            total_budget = float(pb.get("budgeted_hours") or 0)

            raw_alloc = pb.get("bucket_allocation")
            if raw_alloc and isinstance(raw_alloc, dict):
                bucket_alloc = {k: float(v) for k, v in raw_alloc.items()}
            else:
                bucket_alloc = {
                    "senior":     round(total_budget * _BUCKET_SPLIT["senior"],     2),
                    "production": round(total_budget * _BUCKET_SPLIT["production"], 2),
                }

            my_budget    = bucket_alloc.get(role, 0.0)
            my_spent     = bucket_spent_map.get((iid, phase, role), 0.0)
            my_remaining = round(my_budget - my_spent, 2)
            own_h        = own_spent_map.get((iid, phase), 0.0)
            pct_used     = round((my_spent / my_budget * 100) if my_budget > 0 else 0.0, 1)
            status       = "good" if pct_used < 75 else "warn" if pct_used < 90 else "critical"

            phases.append({
                "phase_code":       phase,
                "phase_label":      PHASE_LABELS.get(phase, phase),
                "is_current":       phase == current_phase,
                "bucket_total":     round(my_budget, 2),
                "bucket_spent":     round(my_spent,  2),
                "bucket_remaining": my_remaining,
                "pct_used":         pct_used,
                "status":           status,
                "own_hours":        round(own_h, 2),
            })

        total_own = sum(own_spent_map.get((iid, pb["phase_code"]), 0.0) for pb in budgets)
        current_phase_data = next((p for p in phases if p["is_current"]), None)

        result_projects.append({
            **proj,
            "current_phase":       current_phase,
            "current_phase_label": PHASE_LABELS.get(current_phase, current_phase or "—"),
            "current_phase_data":  current_phase_data,
            "role":                role,
            "phases":              phases,
            "total_own_hours":     round(total_own, 2),
        })

    return {
        "engineer": engineer,
        "name":     TEAM_FULL_NAMES.get(engineer, engineer),
        "role":     role,
        "projects": result_projects,
    }


# ── Engineer Dashboard ────────────────────────────────────────────────────────

# EIT → mentor mapping (initials of senior engineer assigned as mentor)
_MENTOR_MAP: dict[str, str] = {
    "JR": "NK",  # Josh Robinder → Nathan Kline
    "RK": "NK",  # Rajul Kanth   → Nathan Kline
}


def get_engineer_dashboard_data(engineer: str) -> dict[str, Any]:
    """
    Aggregate the data needed for the Engineer Dashboard page in a single call.
    Returns weekly capacity, active project burn data, 7-day milestones, and
    soft blockers drawn from each project's red flags.
    """
    from datetime import date as _date, timedelta as _td

    today     = _date.today()
    week_start = today - _td(days=today.weekday())   # Monday
    week_end   = week_start + _td(days=4)             # Friday

    # ── Engineer identity ──────────────────────────────────────────────────────
    color      = TEAM_COLORS.get(engineer, "#888")
    name       = TEAM_FULL_NAMES.get(engineer, engineer)
    role_title = ENGINEER_ROLES.get(engineer, "")
    is_eit     = "EIT" in role_title
    mentor_ini = _MENTOR_MAP.get(engineer)
    mentor_name = TEAM_FULL_NAMES.get(mentor_ini, "") if mentor_ini else None

    # ── Project bucket view (phases + own hours) ───────────────────────────────
    bucket_view = get_engineer_bucket_view(engineer)

    # ── This-week logged hours ─────────────────────────────────────────────────
    te_resp = (
        _client()
        .table("time_entries")
        .select("hours")
        .eq("engineer_initials", engineer)
        .gte("entry_date", week_start.isoformat())
        .lte("entry_date", week_end.isoformat())
        .execute()
    )
    week_logged = round(sum(float(e.get("hours") or 0) for e in (te_resp.data or [])), 1)

    # ── Weekly capacity ────────────────────────────────────────────────────────
    cap = get_projected_capacity(engineer, week_start, week_end)
    capacity_hours  = cap["available_hours"]  # 8h × available working days
    utilization_pct = round((week_logged / capacity_hours * 100) if capacity_hours > 0 else 0.0, 1)

    # ── 7-day milestones for this engineer ────────────────────────────────────
    horizon = today + _td(days=7)
    ce_resp = (
        _client()
        .table("calendar_events")
        .select("project_number,phase,start_date,end_date,client,team")
        .gte("end_date", today.isoformat() + "T00:00:00Z")
        .lte("start_date", horizon.isoformat() + "T23:59:59Z")
        .neq("is_ooo", True)
        .order("end_date", desc=False)
        .execute()
    )
    pn_to_name = {p["project_number"]: p.get("project_name", p["project_number"])
                  for p in bucket_view["projects"]}
    milestones: list[dict] = []
    for ev in (ce_resp.data or []):
        if engineer not in (ev.get("team") or []):
            continue
        raw_end = (ev.get("end_date") or "")[:10]
        if not raw_end:
            continue
        try:
            days_away = (_date.fromisoformat(raw_end) - today).days
        except Exception:
            continue
        pn = ev.get("project_number", "")
        milestones.append({
            "project_number": pn,
            "project_name":   pn_to_name.get(pn, pn),
            "phase":          ev.get("phase", ""),
            "date":           raw_end,
            "days_away":      days_away,
        })

    # ── Soft blockers from project red flags ──────────────────────────────────
    intake_ids = [p["intake_id"] for p in bucket_view["projects"] if p.get("intake_id")]
    blockers_by_iid: dict[int, list[str]] = {}
    if intake_ids:
        rf_resp = (
            _client()
            .table("intakes")
            .select("id,red_flags_json")
            .in_("id", intake_ids)
            .execute()
        )
        for row in (rf_resp.data or []):
            flags = json.loads(row.get("red_flags_json") or "[]")
            soft  = [
                f.get("title", "Issue")
                for f in flags
                if f.get("severity", "").lower() in ("low", "medium")
            ]
            if soft:
                blockers_by_iid[row["id"]] = soft

    # ── Attach blockers to projects + compute personal burn pulse ─────────────
    enriched_projects: list[dict] = []
    for proj in bucket_view["projects"]:
        iid = proj.get("intake_id")
        cpd = proj.get("current_phase_data") or {}
        own   = cpd.get("own_hours",    0.0)
        total = cpd.get("bucket_total", 0.0)
        if total > 0:
            own_pct = own / total * 100
        else:
            own_pct = 0.0
        burn_pulse = "critical" if own > total else ("warn" if own_pct >= 80 else "good")
        enriched_projects.append({
            **proj,
            "soft_blockers": blockers_by_iid.get(iid, []),
            "burn_pulse":    burn_pulse,
            "own_pct":       round(own_pct, 1),
        })

    return {
        "engineer":        engineer,
        "name":            name,
        "role_title":      role_title,
        "color":           color,
        "is_eit":          is_eit,
        "mentor_initials": mentor_ini,
        "mentor_name":     mentor_name,
        "week": {
            "start":           week_start.isoformat(),
            "end":             week_end.isoformat(),
            "logged_hours":    week_logged,
            "capacity_hours":  capacity_hours,
            "utilization_pct": utilization_pct,
            "ooo_days":        cap["ooo_days"],
        },
        "projects":   enriched_projects,
        "milestones": milestones[:10],
    }


# ── Potential Burn helpers ────────────────────────────────────────────────────

def _get_submitted_periods() -> list[tuple[str, str, str]]:
    """Return (engineer_initials, period_start, period_end) for SUBMITTED + APPROVED periods."""
    try:
        resp = (
            _client()
            .table("timesheet_submissions")
            .select("engineer_initials,period_start,period_end")
            .in_("status", ["SUBMITTED", "APPROVED"])
            .execute()
        )
        return [(r["engineer_initials"], r["period_start"], r["period_end"]) for r in (resp.data or [])]
    except Exception:
        return []


def _entry_in_submitted_period(entry_date: str, engineer: str, periods: list[tuple[str, str, str]]) -> bool:
    for eng, pstart, pend in periods:
        if eng == engineer and pstart <= entry_date <= pend:
            return True
    return False


def get_potential_hours_for_intake(intake_id: int) -> dict[str, Any]:
    """
    Returns potential burn hours for a single project, including per-bucket breakdown.
    potential = hours from SUBMITTED or APPROVED periods (visible to burn tracking).
    total     = all logged hours regardless of submission state.
    bucket_potential = potential hours split by senior / production role.
    """
    entries = list_time_entries_for_intake(intake_id)
    if not entries:
        return {
            "potential": 0.0, "total": 0.0, "draft": 0.0,
            "bucket_potential": {"senior": 0.0, "production": 0.0},
            "bucket_total":     {"senior": 0.0, "production": 0.0},
        }
    submitted_periods = _get_submitted_periods()
    potential = 0.0
    total     = 0.0
    bucket_potential: dict[str, float] = {"senior": 0.0, "production": 0.0}
    bucket_total:     dict[str, float] = {"senior": 0.0, "production": 0.0}
    for e in entries:
        h  = float(e.get("hours") or 0)
        rt = e.get("role_type") or _ROLE_BUCKET.get(e.get("engineer_initials", ""), "senior")
        total += h
        bucket_total[rt] = bucket_total.get(rt, 0.0) + h
        if _entry_in_submitted_period(e.get("entry_date", ""), e.get("engineer_initials", ""), submitted_periods):
            potential += h
            bucket_potential[rt] = bucket_potential.get(rt, 0.0) + h
    return {
        "potential":         round(potential, 2),
        "total":             round(total, 2),
        "draft":             round(total - potential, 2),
        "bucket_potential":  {k: round(v, 2) for k, v in bucket_potential.items()},
        "bucket_total":      {k: round(v, 2) for k, v in bucket_total.items()},
    }


def get_enriched_review_queue() -> list[dict[str, Any]]:
    """SUBMITTED timesheet submissions enriched with full engineer name and projects."""
    rows = get_review_queue()
    result = []
    for r in rows:
        eng = r.get("engineer_initials", "")
        period_start = r.get("period_start", "")
        period_end   = r.get("period_end", "")
        projects: list[dict] = []
        try:
            te_resp = (
                _client()
                .table("time_entries")
                .select("intake_id")
                .eq("engineer_initials", eng)
                .gte("entry_date", period_start)
                .lte("entry_date", period_end)
                .not_.is_("intake_id", "null")
                .execute()
            )
            intake_ids = list({e["intake_id"] for e in (te_resp.data or [])})
            if intake_ids:
                pr = (
                    _client()
                    .table("intakes")
                    .select("id,project_number,project_name")
                    .in_("id", intake_ids)
                    .execute()
                )
                projects = pr.data or []
        except Exception:
            pass
        result.append({
            **r,
            "engineer_name":  TEAM_FULL_NAMES.get(eng, eng),
            "engineer_color": TEAM_COLORS.get(eng, "#888"),
            "projects":       projects,
        })
    return result


def get_all_recent_submissions(limit: int = 30) -> list[dict[str, Any]]:
    """All submissions (any status), newest first, enriched."""
    resp = (
        _client()
        .table("timesheet_submissions")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    rows = resp.data or []
    result = []
    for r in rows:
        eng = r.get("engineer_initials", "")
        result.append({
            **r,
            "engineer_name": TEAM_FULL_NAMES.get(eng, eng),
            "engineer_color": TEAM_COLORS.get(eng, "#888"),
        })
    return result


# ── Profiles (RBAC) ───────────────────────────────────────────────────────────

VALID_ROLES = {"admin", "billing", "engineer", "drafter"}


def list_profiles() -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("profiles")
        .select("*")
        .order("initials")
        .execute()
    )
    return resp.data or []


def get_profile(initials: str) -> Optional[dict[str, Any]]:
    resp = (
        _client()
        .table("profiles")
        .select("*")
        .eq("initials", initials.upper())
        .maybe_single()
        .execute()
    )
    return resp.data


def get_profile_role(initials: str) -> str:
    """Returns the role string for a team member, falling back to 'engineer'."""
    p = get_profile(initials)
    return p["role"] if p else "engineer"


def upsert_profile(
    initials: str,
    *,
    full_name: Optional[str] = None,
    email: Optional[str] = None,
    role: Optional[str] = None,
    color: Optional[str] = None,
    pool: Optional[str] = None,
    capacity_multiplier: Optional[float] = None,
) -> None:
    now = _utc_now_iso()
    row: dict[str, Any] = {"initials": initials.upper(), "updated_at": now}
    if full_name is not None:
        row["full_name"] = full_name
    if email is not None:
        row["email"] = email
    if role is not None:
        row["role"] = role
    if color is not None:
        row["color"] = color
    if pool is not None:
        row["pool"] = pool
    if capacity_multiplier is not None:
        row["capacity_multiplier"] = capacity_multiplier
    (
        _client()
        .table("profiles")
        .upsert(row, on_conflict="initials")
        .execute()
    )


def list_profiles_by_role(role: str) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("profiles")
        .select("*")
        .eq("role", role)
        .order("initials")
        .execute()
    )
    return resp.data or []


# ── Billing Queue ─────────────────────────────────────────────────────────────

def list_billing_queue(
    *,
    status: Optional[str] = None,
    assigned_to: Optional[str] = None,
) -> list[dict[str, Any]]:
    q = _client().table("billing_queue").select("*")
    if status:
        q = q.eq("status", status)
    if assigned_to:
        q = q.eq("assigned_to", assigned_to.upper())
    resp = q.order("created_at").execute()
    return resp.data or []


def get_billing_queue_item(item_id: int) -> Optional[dict[str, Any]]:
    resp = (
        _client()
        .table("billing_queue")
        .select("*")
        .eq("id", item_id)
        .maybe_single()
        .execute()
    )
    return resp.data


def get_billing_queue_for_intake(intake_id: int) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("billing_queue")
        .select("*")
        .eq("intake_id", intake_id)
        .order("created_at")
        .execute()
    )
    return resp.data or []


def create_billing_queue_item(
    intake_id: int,
    billing_phase_code: str,
    *,
    project_number: Optional[str] = None,
    client_name: Optional[str] = None,
    amount: Optional[float] = None,
    assigned_to: Optional[str] = None,
    invoice_date: Optional[str] = None,
    due_date: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    now = _utc_now_iso()
    row: dict[str, Any] = {
        "intake_id":          intake_id,
        "billing_phase_code": billing_phase_code,
        "status":             "pending",
        "created_at":         now,
        "updated_at":         now,
    }
    if project_number is not None:
        row["project_number"] = project_number
    if client_name is not None:
        row["client_name"] = client_name
    if amount is not None:
        row["amount"] = amount
    if assigned_to is not None:
        row["assigned_to"] = assigned_to.upper()
    if invoice_date is not None:
        row["invoice_date"] = invoice_date
    if due_date is not None:
        row["due_date"] = due_date
    if notes is not None:
        row["notes"] = notes
    resp = _client().table("billing_queue").insert(row).execute()
    return (resp.data or [{}])[0].get("id", -1)


def update_billing_queue_item(
    item_id: int,
    *,
    status: Optional[str] = None,
    invoice_number: Optional[str] = None,
    invoice_date: Optional[str] = None,
    due_date: Optional[str] = None,
    amount: Optional[float] = None,
    paid_date: Optional[str] = None,
    paid_amount: Optional[float] = None,
    assigned_to: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    patch: dict[str, Any] = {"updated_at": _utc_now_iso()}
    if status is not None:
        patch["status"] = status
    if invoice_number is not None:
        patch["invoice_number"] = invoice_number
    if invoice_date is not None:
        patch["invoice_date"] = invoice_date
    if due_date is not None:
        patch["due_date"] = due_date
    if amount is not None:
        patch["amount"] = amount
    if paid_date is not None:
        patch["paid_date"] = paid_date
    if paid_amount is not None:
        patch["paid_amount"] = paid_amount
    if assigned_to is not None:
        patch["assigned_to"] = assigned_to.upper()
    if notes is not None:
        patch["notes"] = notes
    (
        _client()
        .table("billing_queue")
        .update(patch)
        .eq("id", item_id)
        .execute()
    )


def count_billing_queue_pending(assigned_to: Optional[str] = None) -> int:
    q = (
        _client()
        .table("billing_queue")
        .select("id", count="exact")
        .in_("status", ["pending", "sent", "overdue"])
    )
    if assigned_to:
        q = q.eq("assigned_to", assigned_to.upper())
    resp = q.execute()
    return resp.count or 0


# ── Natalie's Dashboard Data ──────────────────────────────────────────────────

_ALL_STAFF: list[str] = ["MK", "NK", "RO", "JW", "JR", "RK", "RS", "SW", "JP"]


def get_firm_timecard_summary(period_start: str, period_end: str) -> list[dict[str, Any]]:
    """Per-engineer hours + submission status for the given pay period."""
    te_resp = (
        _client()
        .table("time_entries")
        .select("engineer_initials,hours,entry_date")
        .gte("entry_date", period_start)
        .lte("entry_date", period_end)
        .execute()
    )
    entries = te_resp.data or []

    sub_resp = (
        _client()
        .table("timesheet_submissions")
        .select("engineer_initials,status,submitted_at,period_start")
        .eq("period_start", period_start)
        .execute()
    )
    subs = {r["engineer_initials"]: r for r in (sub_resp.data or [])}

    today = datetime.now(timezone.utc).date().isoformat()
    hours_by_eng: dict[str, float] = {}
    today_by_eng: dict[str, float] = {}
    for e in entries:
        ini = e["engineer_initials"]
        h = float(e["hours"] or 0)
        hours_by_eng[ini] = hours_by_eng.get(ini, 0.0) + h
        if e["entry_date"] == today:
            today_by_eng[ini] = today_by_eng.get(ini, 0.0) + h

    result = []
    for ini in _ALL_STAFF:
        sub = subs.get(ini, {})
        result.append({
            "initials":          ini,
            "role":              ENGINEER_ROLES.get(ini, ""),
            "color":             TEAM_COLORS.get(ini, "#888"),
            "period_hours":      round(hours_by_eng.get(ini, 0.0), 1),
            "today_hours":       round(today_by_eng.get(ini, 0.0), 1),
            "submission_status": sub.get("status", "NOT_STARTED"),
            "submitted_at":      sub.get("submitted_at"),
        })
    return result


def list_time_entries_today() -> list[dict[str, Any]]:
    """All time entries for today, enriched with project name."""
    today = datetime.now(timezone.utc).date().isoformat()
    te_resp = (
        _client()
        .table("time_entries")
        .select("*")
        .eq("entry_date", today)
        .order("id", desc=True)
        .execute()
    )
    entries = te_resp.data or []
    if not entries:
        return []

    intake_ids = list({e["intake_id"] for e in entries if e.get("intake_id")})
    if intake_ids:
        intake_resp = (
            _client()
            .table("intakes")
            .select("id,project_name,project_number")
            .in_("id", intake_ids)
            .execute()
        )
        intake_map = {i["id"]: i for i in (intake_resp.data or [])}
    else:
        intake_map = {}

    for e in entries:
        intake = intake_map.get(e.get("intake_id"), {})
        e["project_name"]   = intake.get("project_name", "—")
        e["project_number"] = e.get("project_number") or intake.get("project_number", "—")
    return entries


# ── Natalie Dashboard helpers ─────────────────────────────────────────────────

def update_billing_phase_fee(intake_id: int, billing_phase_code: str, fee_amount: float) -> None:
    now = _utc_now_iso()
    _client().table("project_billing_phases").update({
        "fee_amount": fee_amount,
        "updated_at": now,
    }).eq("intake_id", intake_id).eq("billing_phase_code", billing_phase_code).execute()


def mark_billing_phase_invoiced(intake_id: int, billing_phase_code: str, invoiced_by: str) -> None:
    now = _utc_now_iso()
    _client().table("project_billing_phases").update({
        "status":      "invoiced",
        "invoiced_at": now,
        "invoiced_by": invoiced_by,
        "updated_at":  now,
    }).eq("intake_id", intake_id).eq("billing_phase_code", billing_phase_code).execute()


def get_cash_flow_forecast() -> dict[str, Any]:
    resp = (
        _client()
        .table("project_billing_phases")
        .select("billing_phase_code,fee_amount,status,intake_id")
        .in_("status", ["complete_pending_approval", "invoice_approved"])
        .execute()
    )
    rows = resp.data or []
    total = sum(float(r.get("fee_amount") or 0) for r in rows)
    return {"total": round(total, 2), "count": len(rows)}


def get_stale_projects(days: int = 14) -> list[dict[str, Any]]:
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    intakes_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name,current_phase")
        .eq("pipeline_active", 1)
        .eq("status", "ACTIVE_PROJECT")
        .execute()
    )
    intakes = intakes_resp.data or []
    if not intakes:
        return []
    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id")
        .gte("entry_date", cutoff)
        .not_.is_("intake_id", "null")
        .execute()
    )
    recent_ids = {e["intake_id"] for e in (te_resp.data or [])}
    return [i for i in intakes if i["id"] not in recent_ids]


def get_utilization_summary(period_start: str, period_end: str) -> dict[str, Any]:
    te_resp = (
        _client()
        .table("time_entries")
        .select("hours,intake_id")
        .gte("entry_date", period_start)
        .lte("entry_date", period_end)
        .execute()
    )
    entries = te_resp.data or []
    total    = sum(float(e["hours"] or 0) for e in entries)
    billable = sum(float(e["hours"] or 0) for e in entries if e.get("intake_id"))
    admin    = total - billable
    pct      = round(billable / total * 100, 1) if total > 0 else 0.0
    return {
        "total":        round(total, 1),
        "billable":     round(billable, 1),
        "admin":        round(admin, 1),
        "billable_pct": pct,
    }


def get_payroll_audit(period_start: str, period_end: str) -> list[dict[str, Any]]:
    te_resp = (
        _client()
        .table("time_entries")
        .select("engineer_initials,hours,intake_id,project_number")
        .gte("entry_date", period_start)
        .lte("entry_date", period_end)
        .execute()
    )
    entries = te_resp.data or []
    sub_resp = (
        _client()
        .table("timesheet_submissions")
        .select("engineer_initials,status,total_hours")
        .eq("period_start", period_start)
        .neq("engineer_initials", _PERIOD_LOCK_SENTINEL)
        .execute()
    )
    subs = {r["engineer_initials"]: r for r in (sub_resp.data or [])}

    # Build a set of project_numbers that exist in intakes so entries logged
    # with a valid project but intake_id=null still count as billable.
    pns_missing_id = {
        e["project_number"] for e in entries
        if not e.get("intake_id") and e.get("project_number")
    }
    known_project_numbers: set[str] = set()
    if pns_missing_id:
        pn_resp = (
            _client()
            .table("intakes")
            .select("project_number")
            .in_("project_number", list(pns_missing_id))
            .execute()
        )
        known_project_numbers = {r["project_number"] for r in (pn_resp.data or [])}

    total_by_eng: dict[str, float] = {}
    billable_by_eng: dict[str, float] = {}
    for e in entries:
        ini = e["engineer_initials"]
        h   = float(e["hours"] or 0)
        total_by_eng[ini] = total_by_eng.get(ini, 0.0) + h
        is_billable = bool(e.get("intake_id")) or (
            e.get("project_number") in known_project_numbers
        )
        if is_billable:
            billable_by_eng[ini] = billable_by_eng.get(ini, 0.0) + h

    EXPECTED = 80.0
    result = []
    for ini in _ALL_STAFF:
        total    = round(total_by_eng.get(ini, 0.0), 1)
        billable = round(billable_by_eng.get(ini, 0.0), 1)
        pct      = round(billable / total * 100, 1) if total > 0 else 0.0
        sub      = subs.get(ini, {})
        discrepancy = total > EXPECTED * 1.15 or (
            sub.get("status") == "APPROVED" and total < EXPECTED * 0.5
        )
        result.append({
            "initials":       ini,
            "role":           ENGINEER_ROLES.get(ini, ""),
            "color":          TEAM_COLORS.get(ini, "#888"),
            "total_hours":    total,
            "billable_hours": billable,
            "billable_pct":   pct,
            "status":         sub.get("status", "NOT_STARTED"),
            "discrepancy":    discrepancy,
        })
    return result


def get_engineer_project_hours() -> list[dict[str, Any]]:
    """Per-engineer breakdown across ALL active projects, including 0-hour entries."""
    # Fetch all active projects
    proj_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name")
        .eq("pipeline_active", 1)
        .neq("status", "PROPOSAL_OUT")
        .order("project_number")
        .execute()
    )
    projects = proj_resp.data or []
    if not projects:
        return []

    intake_ids = [p["id"] for p in projects]
    proj_map = {p["id"]: p for p in projects}

    # Fetch all time entries for those projects
    te_resp = (
        _client()
        .table("time_entries")
        .select("engineer_initials,intake_id,hours")
        .in_("intake_id", intake_ids)
        .execute()
    )
    entries = te_resp.data or []

    # Build (engineer, intake_id) → hours map
    hours_map: dict[tuple, float] = {}
    for e in entries:
        key = (e["engineer_initials"], int(e["intake_id"]))
        hours_map[key] = hours_map.get(key, 0.0) + float(e["hours"] or 0)

    # Build per-engineer list — only include projects the engineer has logged hours on
    result = []
    for ini in _ALL_STAFF:
        project_rows = []
        total_h = 0.0
        for p in projects:
            h = round(hours_map.get((ini, p["id"]), 0.0), 1)
            if h > 0:
                total_h += h
                project_rows.append({
                    "intake_id":      p["id"],
                    "project_number": p.get("project_number") or "—",
                    "project_name":   p.get("project_name") or "",
                    "client_name":    p.get("client_name") or "",
                    "hours":          h,
                })
        if project_rows:
            result.append({
                "initials":     ini,
                "role":         ENGINEER_ROLES.get(ini, ""),
                "color":        TEAM_COLORS.get(ini, "#888"),
                "full_name":    TEAM_FULL_NAMES.get(ini, ini),
                "total_hours":  round(total_h, 1),
                "projects":     project_rows,
            })
    return result


# ── Invoice System ────────────────────────────────────────────────────────────

PHASE_FEE_PCT: dict[str, float] = {
    "retainer": 0.15, "SD": 0.20, "DD": 0.30, "CD": 0.30, "CA": 0.10,
}


def _next_invoice_number() -> str:
    year = datetime.now().year
    resp = (
        _client()
        .table("invoices")
        .select("invoice_number")
        .ilike("invoice_number", f"AVS-{year}-%")
        .order("invoice_number", desc=True)
        .limit(1)
        .execute()
    )
    rows = resp.data or []
    if rows:
        try:
            seq = int(rows[0]["invoice_number"].split("-")[-1]) + 1
        except (ValueError, IndexError):
            seq = 1
    else:
        seq = 1
    return f"AVS-{year}-{seq:03d}"


def get_pending_billables() -> list[dict[str, Any]]:
    """Phases approved by Mo (invoice_approved) awaiting formal invoice generation."""
    pbp_resp = (
        _client()
        .table("project_billing_phases")
        .select("*")
        .eq("status", "invoice_approved")
        .order("invoice_approved_at", desc=True)
        .execute()
    )
    rows = pbp_resp.data or []
    if not rows:
        return []

    intake_ids = list({r["intake_id"] for r in rows})
    intake_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name,location_region,mo_fee_override")
        .in_("id", intake_ids)
        .execute()
    )
    intake_map = {i["id"]: i for i in (intake_resp.data or [])}

    all_phases_resp = (
        _client()
        .table("project_billing_phases")
        .select("intake_id,billing_phase_code,fee_amount,status")
        .in_("intake_id", intake_ids)
        .execute()
    )
    phases_by_intake: dict[int, list] = {}
    for p in (all_phases_resp.data or []):
        phases_by_intake.setdefault(p["intake_id"], []).append(p)

    inv_resp = (
        _client()
        .table("invoices")
        .select("intake_id,amount")
        .in_("intake_id", intake_ids)
        .execute()
    )
    invoiced_by_intake: dict[int, float] = {}
    for inv in (inv_resp.data or []):
        iid = inv["intake_id"]
        invoiced_by_intake[iid] = invoiced_by_intake.get(iid, 0.0) + float(inv["amount"] or 0)

    result = []
    for r in rows:
        intake = intake_map.get(r["intake_id"], {})
        phases = phases_by_intake.get(r["intake_id"], [])
        total_contract = sum(float(p.get("fee_amount") or 0) for p in phases)
        prev_billed = invoiced_by_intake.get(r["intake_id"], 0.0)
        amount_due = float(r.get("invoice_fee_override") or r.get("fee_amount") or 0)
        balance = max(0.0, total_contract - prev_billed - amount_due)
        result.append({
            **r,
            "intake": intake,
            "total_contract": round(total_contract, 2),
            "prev_billed": round(prev_billed, 2),
            "amount_due": round(amount_due, 2),
            "balance_to_finish": round(balance, 2),
        })
    return result


def get_invoice_preview(intake_id: int, phase_code: str) -> dict[str, Any]:
    """Full breakdown for the confirmation modal and PDF."""
    pbp_resp = (
        _client()
        .table("project_billing_phases")
        .select("billing_phase_code,fee_amount,fee_pct,invoice_fee_override")
        .eq("intake_id", intake_id)
        .execute()
    )
    phases = pbp_resp.data or []
    total_contract = sum(float(p.get("fee_amount") or 0) for p in phases)
    this_phase = next((p for p in phases if p["billing_phase_code"] == phase_code), {})
    amount_due = float(this_phase.get("invoice_fee_override") or this_phase.get("fee_amount") or 0)
    fee_pct = float(this_phase.get("fee_pct") or PHASE_FEE_PCT.get(phase_code, 0))

    prev_billed = 0.0
    try:
        inv_resp = (
            _client()
            .table("invoices")
            .select("amount")
            .eq("intake_id", intake_id)
            .execute()
        )
        prev_billed = sum(float(i.get("amount") or 0) for i in (inv_resp.data or []))
    except Exception:
        pass

    intake_resp = (
        _client()
        .table("intakes")
        .select("project_number,project_name,client_name,location_region")
        .eq("id", intake_id)
        .maybe_single()
        .execute()
    )
    intake = intake_resp.data or {}

    order_map = {c: i for i, c in enumerate(BILLING_PHASE_ORDER)}
    phase_rows = sorted([{
        "code": p["billing_phase_code"],
        "label": BILLING_PHASE_LABELS.get(p["billing_phase_code"], p["billing_phase_code"]),
        "phase_fee": round(float(p.get("fee_amount") or 0), 2),
        "pct_of_contract": round(float(p.get("fee_pct") or 0) * 100, 0),
        "is_current": p["billing_phase_code"] == phase_code,
    } for p in phases], key=lambda x: order_map.get(x["code"], 99))

    return {
        "intake": intake,
        "intake_id": intake_id,
        "phase_code": phase_code,
        "phase_label": BILLING_PHASE_LABELS.get(phase_code, phase_code),
        "total_contract": round(total_contract, 2),
        "prev_billed": round(prev_billed, 2),
        "amount_due": round(amount_due, 2),
        "balance_to_finish": round(max(0.0, total_contract - prev_billed - amount_due), 2),
        "fee_pct": round(fee_pct * 100, 0),
        "phase_rows": phase_rows,
    }


def create_invoice(
    intake_id: int, phase_code: str, amount: float, created_by: str, notes: str = ""
) -> dict[str, Any]:
    invoice_number = _next_invoice_number()
    now = _utc_now_iso()
    resp = (
        _client()
        .table("invoices")
        .insert({
            "invoice_number": invoice_number,
            "intake_id":      intake_id,
            "phase_code":     phase_code,
            "amount":         round(amount, 2),
            "status":         "draft",
            "created_by":     created_by,
            "notes":          notes,
            "created_at":     now,
        })
        .execute()
    )
    invoice_id = (resp.data or [{}])[0].get("id")
    mark_billing_phase_invoiced(intake_id, phase_code, created_by)
    try:
        _client().table("intakes").update(
            {"last_invoiced_at": now, "updated_at": now}
        ).eq("id", intake_id).execute()
    except Exception:
        pass
    return {"invoice_number": invoice_number, "invoice_id": invoice_id, "amount": amount}


def get_all_invoices(limit: int = 50) -> list[dict[str, Any]]:
    resp = (
        _client()
        .table("invoices")
        .select("*")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    rows = resp.data or []
    if not rows:
        return []
    intake_ids = list({r["intake_id"] for r in rows})
    intake_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name")
        .in_("id", intake_ids)
        .execute()
    )
    intake_map = {i["id"]: i for i in (intake_resp.data or [])}
    return [{
        **r,
        "intake": intake_map.get(r["intake_id"], {}),
        "phase_label": BILLING_PHASE_LABELS.get(r.get("phase_code", ""), r.get("phase_code", "")),
    } for r in rows]


def update_invoice_status(invoice_id: int, status: str, updated_by: str) -> None:
    now = _utc_now_iso()
    patch: dict[str, Any] = {"status": status}
    if status == "sent":
        patch["sent_at"] = now
    elif status == "paid":
        patch["paid_at"] = now
    _client().table("invoices").update(patch).eq("id", invoice_id).execute()


def get_burn_vs_bill() -> list[dict[str, Any]]:
    """Burn (hours × rate) vs invoiced amount per active project."""
    intakes_resp = (
        _client()
        .table("intakes")
        .select("id,project_number,project_name,client_name")
        .eq("pipeline_active", 1)
        .eq("status", "ACTIVE_PROJECT")
        .execute()
    )
    intakes = intakes_resp.data or []
    if not intakes:
        return []
    intake_ids = [i["id"] for i in intakes]

    te_resp = (
        _client()
        .table("time_entries")
        .select("intake_id,hours")
        .in_("intake_id", intake_ids)
        .execute()
    )
    hours_by: dict[int, float] = {}
    for e in (te_resp.data or []):
        iid = e["intake_id"]
        hours_by[iid] = hours_by.get(iid, 0.0) + float(e["hours"] or 0)

    inv_resp = (
        _client()
        .table("invoices")
        .select("intake_id,amount")
        .in_("intake_id", intake_ids)
        .execute()
    )
    billed_by: dict[int, float] = {}
    for inv in (inv_resp.data or []):
        iid = inv["intake_id"]
        billed_by[iid] = billed_by.get(iid, 0.0) + float(inv["amount"] or 0)

    result = []
    for i in intakes:
        iid = i["id"]
        burn = round(hours_by.get(iid, 0.0) * TARGET_EFFICIENCY_RATIO, 2)
        billed = round(billed_by.get(iid, 0.0), 2)
        ratio = round(burn / billed, 3) if billed > 0 else None
        result.append({
            "intake_id":      iid,
            "project_number": i.get("project_number") or "—",
            "project_name":   i.get("project_name") or "—",
            "client_name":    i.get("client_name") or "—",
            "burn":           burn,
            "billed":         billed,
            "ratio":          ratio,
            "profit_risk":    ratio is not None and ratio > 1.0,
        })
    result.sort(key=lambda x: (x["ratio"] or 0), reverse=True)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# §10.6 Staffing mitigations — persistence layer
# ─────────────────────────────────────────────────────────────────────────────
#
# Required Supabase table (run once in SQL editor):
#
#   CREATE TABLE staffing_mitigations (
#     id           BIGSERIAL PRIMARY KEY,
#     pattern      TEXT    NOT NULL,
#     week         DATE    NOT NULL,           -- ISO Monday
#     from_person  TEXT    NOT NULL,
#     to_person    TEXT    NOT NULL,
#     hours_delta  NUMERIC NOT NULL,
#     rationale    TEXT,
#     applied_by   TEXT,
#     applied_at   TIMESTAMPTZ DEFAULT NOW(),
#     reverted_at  TIMESTAMPTZ
#   );
#   CREATE INDEX idx_staffing_mitigations_active
#     ON staffing_mitigations (week) WHERE reverted_at IS NULL;
#
# Until that runs, the functions below will return [] / 0 / silently fail,
# matching the graceful-degrade pattern used by get_active_bids().

def record_applied_mitigation(
    *,
    pattern: str,
    week: str,
    from_person: str,
    to_person: str,
    hours_delta: float,
    rationale: str,
    applied_by: str = "",
) -> Optional[dict[str, Any]]:
    """Insert a mitigation row. Returns the inserted row dict, or None on failure."""
    try:
        resp = (
            _client()
            .table("staffing_mitigations")
            .insert({
                "pattern":     pattern,
                "week":        week,
                "from_person": from_person,
                "to_person":   to_person,
                "hours_delta": float(hours_delta),
                "rationale":   rationale,
                "applied_by":  applied_by,
                "applied_at":  _utc_now_iso(),
            })
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception:
        return None


def list_applied_mitigations(
    *,
    week: Optional[str] = None,
    include_reverted: bool = False,
) -> list[dict[str, Any]]:
    """List applied mitigations, optionally filtered to a specific week."""
    try:
        q = _client().table("staffing_mitigations").select("*")
        if week:
            q = q.eq("week", week)
        if not include_reverted:
            q = q.is_("reverted_at", "null")
        resp = q.order("applied_at", desc=True).execute()
        return resp.data or []
    except Exception:
        return []


def revert_applied_mitigation(mitigation_id: int) -> bool:
    """Soft-revert a mitigation. Returns True on success."""
    try:
        (
            _client()
            .table("staffing_mitigations")
            .update({"reverted_at": _utc_now_iso()})
            .eq("id", mitigation_id)
            .is_("reverted_at", "null")
            .execute()
        )
        return True
    except Exception:
        return False


def get_applied_mitigation(mitigation_id: int) -> Optional[dict[str, Any]]:
    try:
        resp = (
            _client()
            .table("staffing_mitigations")
            .select("*")
            .eq("id", mitigation_id)
            .limit(1)
            .execute()
        )
        rows = resp.data or []
        return rows[0] if rows else None
    except Exception:
        return None
