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
        os.environ["SUPABASE_KEY"],
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
