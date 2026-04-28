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
    "CD":  "#f43f5e",   # rose-500
    "IFP": "#f59e0b",   # amber-500
    "REV": "#a855f7",   # purple-500
    "SD":  "#06b6d4",   # cyan-500
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
