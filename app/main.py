from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import csv
import io
import json as _json
from datetime import date, timedelta

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from fastapi import UploadFile, File

from . import calendar_sync
from . import weu as weu_engine
from . import db
from . import document_extractor
from . import project_search
from . import proposal_generator
from .decision import compute_decision, complexity_estimate
from .fee_estimator import cognasync_estimate_from_answers, check_fee_review_required


APP_DIR = Path(__file__).resolve().parent

templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def _status_from_recommendation(recommendation: str) -> str:
    if recommendation == "PROCEED_TO_PROPOSAL":
        return "PROCEED_TO_PROPOSAL"
    if recommendation == "CLARIFY_FIRST":
        return "NEEDS_INFO"
    return "PENDING_MO_REVIEW"


def _now_local_iso() -> str:
    # Display-only; stored timestamps are UTC in db layer.
    return datetime.now().replace(microsecond=0).isoformat()


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str if value_str else None


async def _parse_intake_form(request: Request) -> dict[str, Any]:
    form = await request.form()

    def checkbox(name: str) -> bool:
        return form.get(name) == "on"

    def getlist(name: str) -> list[str]:
        try:
            return [str(v) for v in form.getlist(name) if str(v).strip()]
        except Exception:
            v = form.get(name)
            return [str(v)] if v else []

    answers: dict[str, Any] = {
        "project_type": _as_str(form.get("project_type")) or "unknown",
        "building_type": _as_str(form.get("building_type")) or "other",
        "building_type_experience": _as_str(form.get("building_type_experience")) or "unknown",
        "architect_status": _as_str(form.get("architect_status")) or "unknown",
        "architect_responsiveness": _as_str(form.get("architect_responsiveness")) or "unknown",
        "decision_maker_clarity": _as_str(form.get("decision_maker_clarity")) or "unclear",
        "city": _as_str(form.get("city")),
        "state": _as_str(form.get("state")),
        "relationship_type": _as_str(form.get("relationship_type")) or "new",
        "inquiry_source": _as_str(form.get("inquiry_source")) or "cold_inquiry",
        "primary_material": _as_str(form.get("primary_material")) or "steel",
        "scope_definition": _as_str(form.get("scope_definition")) or "unknown",
        "scope_risk_type": _as_str(form.get("scope_risk_type")) or "standard",
        "specialist_support": _as_str(form.get("specialist_support")) or "unknown",
        "scope_creep_likelihood": _as_str(form.get("scope_creep_likelihood")) or "unknown",
        "schedule_realism": _as_str(form.get("schedule_realism")) or "unknown",
        "approx_sf": _as_str(form.get("approx_sf")),
        "est_construction_cost": _as_str(form.get("est_construction_cost")),
        "weeks_to_permit_submission": _as_str(form.get("weeks_to_permit_submission")),
        "hard_stop_deadlines": getlist("hard_stop_deadlines"),
        "site_access": _as_str(form.get("site_access")) or "unknown",
        "docs_commitment": _as_str(form.get("docs_commitment")) or "unknown",
        "capacity_available": _as_str(form.get("capacity_available")) or "unknown",
        "quick_flags": getlist("quick_flags"),
        # Docs (booleans)
        "doc_geotech": checkbox("doc_geotech"),
        "doc_grading_plan": checkbox("doc_grading_plan"),
        "doc_arch_drawings": checkbox("doc_arch_drawings"),
        "doc_existing_struct_drawings": checkbox("doc_existing_struct_drawings"),
        "doc_site_photos": checkbox("doc_site_photos"),
        "doc_rfp_program": checkbox("doc_rfp_program"),
        "doc_site_plan": checkbox("doc_site_plan"),
        "doc_prelim_schedule": checkbox("doc_prelim_schedule"),
        # Notes / meta
        "info_gaps": getlist("info_gaps"),
        "decline_reasons": getlist("decline_reasons"),
        "notes": _as_str(form.get("notes")),
    }

    return answers


def _badge_class(value: str) -> str:
    v = (value or "").lower()
    return {
        "critical": "badge badge-critical",
        "high": "badge badge-high",
        "medium": "badge badge-medium",
        "low": "badge badge-low",
    }.get(v, "badge")


def _days_ago(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        return (datetime.now().date() - d).days
    except Exception:
        return None


def _current_pay_period() -> tuple[str, str]:
    """Return (start, end) ISO strings for the current 2-week pay period."""
    today = date.today()
    days_since_monday = today.weekday()
    monday = today - timedelta(days=days_since_monday)
    reference = date(2020, 1, 6)  # known Monday
    weeks_elapsed = (monday - reference).days // 7
    period_start = reference + timedelta(weeks=(weeks_elapsed // 2) * 2)
    period_end = period_start + timedelta(days=13)
    return period_start.isoformat(), period_end.isoformat()


templates.env.filters["badge_class"] = _badge_class
templates.env.filters["days_ago"] = _days_ago
templates.env.globals["pending_mo_count"] = lambda: len(db.list_pending_mo())


def _timesheet_period_count() -> int:
    try:
        start, end = _current_pay_period()
        return db.count_timesheet_period_entries(start, end)
    except Exception:
        return 0


templates.env.globals["timesheet_period_count"] = _timesheet_period_count


def _pending_invoice_count() -> int:
    try:
        return db.count_pending_invoice_approvals()
    except Exception:
        return 0


templates.env.globals["pending_invoice_count"] = _pending_invoice_count


def _upcoming_ooo_count() -> int:
    try:
        return db.count_upcoming_ooo(30)
    except Exception:
        return 0


templates.env.globals["upcoming_ooo_count"] = _upcoming_ooo_count


app = FastAPI(title="AVS Intake Gate")
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")


@app.on_event("startup")
def _startup() -> None:
    db.init_db()


@app.get("/", response_class=HTMLResponse)
def launch(request: Request) -> HTMLResponse:
    now = datetime.now()
    hour = now.hour
    time_of_day = "morning" if hour < 12 else "afternoon" if hour < 17 else "evening"

    all_intakes = db.list_intakes()
    proceed_count  = sum(1 for i in all_intakes if i.status == "PROCEED_TO_PROPOSAL")
    declined_count = sum(1 for i in all_intakes if i.status == "DECLINED")

    mo_queue_raw = db.list_pending_mo()[:3]
    mo_queue_intakes = []
    for intake in mo_queue_raw:
        flags = []
        try:
            flags = _json.loads(intake.red_flags or "[]") if intake.red_flags else []
        except Exception:
            pass
        high_count     = sum(1 for f in flags if (f.get("severity") or "").lower() == "high")
        critical_count = sum(1 for f in flags if (f.get("severity") or "").lower() == "critical")
        mo_queue_intakes.append({
            "id": intake.id,
            "project_name": intake.project_name,
            "recommendation": intake.recommendation,
            "red_flag_high_count": high_count,
            "red_flag_critical_count": critical_count,
        })

    # Calendar stats
    active_project_count = 0
    ifp_conflict_count   = 0
    ooo_this_week        = 0
    try:
        today = date.today()
        week_start = (today - timedelta(days=today.weekday())).isoformat()
        week_end   = (today + timedelta(days=6 - today.weekday())).isoformat()
        cal_events = db._client().table("calendar_events") \
            .select("id,phase,is_ooo,start_date,end_date") \
            .gte("end_date", (today.replace(day=1)).isoformat()) \
            .execute()
        rows = cal_events.data or []
        active_project_count = len(rows)
        ifp_days: dict[str, int] = {}
        for ev in rows:
            if ev.get("phase") == "IFP" and not ev.get("is_ooo"):
                s = (ev.get("start_date") or "")[:10]
                e = (ev.get("end_date") or "")[:10]
                if s and e:
                    cur = date.fromisoformat(s)
                    end = date.fromisoformat(e)
                    while cur <= end:
                        k = cur.isoformat()
                        ifp_days[k] = ifp_days.get(k, 0) + 1
                        cur += timedelta(days=1)
            if ev.get("is_ooo"):
                s = (ev.get("start_date") or "")[:10]
                e = (ev.get("end_date") or "")[:10]
                if s and e and s <= week_end and e >= week_start:
                    ooo_this_week += 1
        ifp_conflict_count = sum(1 for v in ifp_days.values() if v >= 2)
    except Exception:
        pass

    return templates.TemplateResponse(
        "launch.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "time_of_day": time_of_day,
            "total_intakes": len(all_intakes),
            "proceed_count": proceed_count,
            "declined_count": declined_count,
            "mo_queue_intakes": mo_queue_intakes,
            "active_project_count": active_project_count,
            "ifp_conflict_count": ifp_conflict_count,
            "ooo_this_week": ooo_this_week,
        },
    )


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, status: Optional[str] = None) -> HTMLResponse:
    intakes = db.list_intakes(status=status)
    counts: dict[str, int] = {}
    for row in intakes:
        counts[row.status] = counts.get(row.status, 0) + 1
    all_counts = {}
    for row in db.list_intakes():
        all_counts[row.status] = all_counts.get(row.status, 0) + 1

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "intakes": intakes,
            "status_filter": status,
            "counts": all_counts,
            "now_local": _now_local_iso(),
        },
    )


@app.get("/intakes/new", response_class=HTMLResponse)
def intake_new(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "intake_form.html",
        {
            "request": request,
            "mode": "new",
            "intake": None,
            "answers": {},
            "now_local": _now_local_iso(),
            "project_templates": db.list_templates(),
        },
    )


@app.get("/api/templates")
def api_list_templates() -> list[dict[str, Any]]:
    return [{"id": t.id, "name": t.name, "description": t.description, "answers": t.answers}
            for t in db.list_templates()]


@app.post("/api/templates")
async def api_create_template(request: Request) -> dict[str, Any]:
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Template name is required.")
    template_id = db.create_template(
        name=name,
        description=(body.get("description") or "").strip(),
        answers=body.get("answers") or {},
    )
    return {"template_id": template_id}


@app.delete("/api/templates/{template_id}")
def api_delete_template(template_id: int) -> dict[str, Any]:
    db.delete_template(template_id)
    return {"deleted": template_id}


@app.delete("/api/intakes/{intake_id}")
def api_delete_intake(intake_id: int) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    db.delete_intake(intake_id)
    return {"deleted": intake_id}


@app.post("/intakes")
async def intake_create(request: Request) -> RedirectResponse:
    form = await request.form()
    project_name = _as_str(form.get("project_name"))
    if not project_name:
        raise HTTPException(status_code=400, detail="Project name is required.")

    inquiry_date = _as_str(form.get("inquiry_date"))
    ifp_due_date = _as_str(form.get("ifp_due_date"))
    client_name = _as_str(form.get("client_name"))
    architect_name = _as_str(form.get("architect_name"))
    lead_contact = _as_str(form.get("lead_contact"))
    submitted_by = _as_str(form.get("submitted_by"))

    answers = await _parse_intake_form(request)
    city = answers.get("city") or ""
    state = answers.get("state") or ""
    location_region = f"{city}, {state}".strip(", ") or None

    decision = compute_decision(answers)
    status = _status_from_recommendation(decision["recommendation"])

    intake_id = db.create_intake(
        inquiry_date=inquiry_date,
        ifp_due_date=ifp_due_date,
        project_name=project_name,
        client_name=client_name,
        architect_name=architect_name,
        lead_contact=lead_contact,
        location_region=location_region,
        submitted_by=submitted_by,
        status=status,
        recommendation=decision["recommendation"],
        recommendation_reason=decision["reason"],
        red_flags=decision["red_flags"],
        red_flag_counts=decision["counts"],
        answers=answers,
    )

    return RedirectResponse(url=f"/intakes/{intake_id}", status_code=303)


@app.get("/intakes/{intake_id}", response_class=HTMLResponse)
def intake_view(request: Request, intake_id: int) -> HTMLResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    decision = compute_decision(intake.answers)
    # Inject complexity so fee_estimator doesn't need to import decision.py
    enriched_answers = {**intake.answers, "_complexity": decision["complexity_estimate"]}
    cognasync_estimate = cognasync_estimate_from_answers(
        intake.project_name, enriched_answers
    )
    # Default fee for proposal generation form
    proposal_fee_default: Optional[float] = None
    approved_fee: Optional[float] = None
    if intake.mo_fee_decision == "OVERRIDE" and intake.mo_fee_override:
        try:
            proposal_fee_default = float(intake.mo_fee_override)
            approved_fee = proposal_fee_default
        except (ValueError, TypeError):
            pass
    elif (
        intake.mo_fee_decision == "ACCEPTED"
        and cognasync_estimate
        and not cognasync_estimate.get("needs_manual_review")
    ):
        fee_range = cognasync_estimate.get("suggested_fee_range") or {}
        lo = fee_range.get("low") or 0
        hi = fee_range.get("high") or 0
        if lo and hi:
            proposal_fee_default = round((lo + hi) / 2 / 500) * 500
            approved_fee = proposal_fee_default

    # Command header: hours burned % from phase budgets
    phase_budgets = db.list_phase_budgets(intake_id) if intake.project_number else []
    total_budget = sum(b["budgeted_hours"] for b in phase_budgets)
    total_used = sum(b["hours_used"] for b in phase_budgets)
    hours_burned_pct: Optional[float] = (
        round(total_used / total_budget * 100, 1) if total_budget > 0 else None
    )

    # Days to IFP and schedule data
    days_to_ifp: Optional[int] = None
    schedule_data: dict = {}
    today_d = date.today()
    if intake.ifp_due_date:
        try:
            ifp_d = date.fromisoformat(intake.ifp_due_date)
            days_to_ifp = (ifp_d - today_d).days
            if intake.inquiry_date:
                start_d = date.fromisoformat(intake.inquiry_date)
                total_days = (ifp_d - start_d).days
                elapsed = (today_d - start_d).days
                pct = max(0, min(100, round(elapsed / total_days * 100, 1))) if total_days > 0 else 100
                schedule_data = {
                    "start": intake.inquiry_date,
                    "end": intake.ifp_due_date,
                    "today": today_d.isoformat(),
                    "total_days": total_days,
                    "elapsed_days": elapsed,
                    "days_remaining": days_to_ifp,
                    "pct": pct,
                }
        except (ValueError, AttributeError):
            pass

    # Default open accordion sections
    c = intake.red_flag_counts
    default_open: set[str] = set()
    if not intake.mo_decision:
        default_open.add("summary")
    if c.get("critical", 0) > 0 or c.get("high", 0) > 0:
        default_open.add("flags")
    if intake.mo_decision:
        default_open.add("mo")
    if intake.status in ("PROCEED_TO_PROPOSAL", "PROCEED_WITH_CONDITIONS"):
        default_open.add("proposal")
        if intake.project_number:
            default_open.add("budget")
    if not default_open:
        default_open.add("summary")

    return templates.TemplateResponse(
        "intake_view.html",
        {
            "request": request,
            "intake": intake,
            "answers": intake.answers,
            "complexity_estimate": decision["complexity_estimate"],
            "fee_range_estimate": decision["fee_range_estimate"],
            "fast_track": decision["fast_track"],
            "cognasync_estimate": cognasync_estimate,
            "now_local": _now_local_iso(),
            "mo_passcode_enabled": bool(os.environ.get("AVS_MO_PASSCODE")),
            "proposal_checklist": intake.proposal_checklist,
            "checklist_keys": CHECKLIST_KEYS,
            "proposal_completed_at": intake.proposal_completed_at,
            "proposal_fee_default": proposal_fee_default,
            "approved_fee": approved_fee,
            "phase_budgets": phase_budgets,
            "hours_burned_pct": hours_burned_pct,
            "days_to_ifp": days_to_ifp,
            "schedule_data": schedule_data,
            "default_open": default_open,
            "db_team_colors": db.TEAM_COLORS,
        },
    )


@app.get("/intakes/{intake_id}/edit", response_class=HTMLResponse)
def intake_edit(request: Request, intake_id: int) -> HTMLResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    return templates.TemplateResponse(
        "intake_form.html",
        {
            "request": request,
            "mode": "edit",
            "intake": intake,
            "answers": intake.answers,
            "now_local": _now_local_iso(),
        },
    )


@app.post("/intakes/{intake_id}")
async def intake_update(request: Request, intake_id: int) -> RedirectResponse:
    existing = db.get_intake(intake_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Not found.")

    form = await request.form()
    project_name = _as_str(form.get("project_name"))
    if not project_name:
        raise HTTPException(status_code=400, detail="Project name is required.")

    inquiry_date = _as_str(form.get("inquiry_date"))
    ifp_due_date = _as_str(form.get("ifp_due_date"))
    client_name = _as_str(form.get("client_name"))
    architect_name = _as_str(form.get("architect_name"))
    lead_contact = _as_str(form.get("lead_contact"))
    submitted_by = _as_str(form.get("submitted_by"))

    answers = await _parse_intake_form(request)
    city = answers.get("city") or ""
    state = answers.get("state") or ""
    location_region = f"{city}, {state}".strip(", ") or None

    decision = compute_decision(answers)
    status = _status_from_recommendation(decision["recommendation"])

    # Keep final outcomes if Mo already decided.
    if existing.mo_decision:
        status = existing.status

    db.update_intake(
        intake_id,
        inquiry_date=inquiry_date,
        ifp_due_date=ifp_due_date,
        project_name=project_name,
        client_name=client_name,
        architect_name=architect_name,
        lead_contact=lead_contact,
        location_region=location_region,
        submitted_by=submitted_by,
        status=status,
        recommendation=decision["recommendation"],
        recommendation_reason=decision["reason"],
        red_flags=decision["red_flags"],
        red_flag_counts=decision["counts"],
        answers=answers,
    )
    return RedirectResponse(url=f"/intakes/{intake_id}", status_code=303)


@app.post("/intakes/{intake_id}/push-to-mo")
def push_to_mo_queue(intake_id: int) -> RedirectResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    if not intake.mo_decision:
        db.set_status(intake_id, "PENDING_MO_REVIEW")
    return RedirectResponse(url=f"/intakes/{intake_id}", status_code=303)


# ── Document upload + AI extraction ──────────────────────────────────────────

@app.get("/intake/upload", response_class=HTMLResponse)
def intake_upload_get(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "upload_intake.html",
        {"request": request, "now_local": _now_local_iso(), "error": None},
    )


@app.post("/intake/upload", response_class=HTMLResponse)
async def intake_upload_post(
    request: Request,
    file: Optional[UploadFile] = File(None),
    paste_text: Optional[str] = Form(None),
) -> HTMLResponse:
    import types

    error: Optional[str] = None
    prefill: dict[str, Any] = {}
    source_label = "uploaded document"

    try:
        paste_clean = (paste_text or "").strip()
        if paste_clean:
            text = paste_clean
            source_label = "pasted text"
        elif file and file.filename:
            raw = await file.read()
            if not raw:
                raise ValueError("Uploaded file is empty.")
            text = document_extractor.extract_text(file.filename, raw)
            if text.startswith("[") and "error" in text.lower():
                raise ValueError(text)
            source_label = file.filename
        else:
            raise ValueError("Please upload a file or paste document text.")

        prefill = document_extractor.extract_intake_fields(text)
    except Exception as exc:
        error = str(exc)
        return templates.TemplateResponse(
            "upload_intake.html",
            {"request": request, "now_local": _now_local_iso(), "error": error},
        )

    # Render empty form — JS populateForm() handles all field mapping + confidence indicators
    fake_intake = types.SimpleNamespace(
        id=None, project_name="", client_name="", architect_name="",
        lead_contact="", location_region="", submitted_by="",
        inquiry_date="", ifp_due_date="", status="",
    )

    return templates.TemplateResponse(
        "intake_form.html",
        {
            "request": request,
            "mode": "new",
            "intake": fake_intake,
            "answers": {},
            "now_local": _now_local_iso(),
            "project_templates": db.list_templates(),
            "prefill_data": prefill,
            "prefill_filename": source_label,
        },
    )


# ── Proposal generation ───────────────────────────────────────────────────────

@app.post("/intakes/{intake_id}/generate-proposal")
def generate_proposal_route(
    intake_id: int,
    fee_amount: float = Form(...),
    structural_system: Optional[str] = Form(None),
) -> RedirectResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    decision = compute_decision(intake.answers)
    enriched = {**intake.answers, "_complexity": decision["complexity_estimate"]}
    if structural_system:
        enriched["structural_system"] = structural_system

    try:
        text = proposal_generator.generate_proposal(
            project_name=intake.project_name or "Project",
            project_type=intake.answers.get("project_type") or "new_construction",
            location=intake.location_region or "",
            building_type=intake.answers.get("building_type") or "retail",
            approx_sf=int(intake.answers["approx_sf"]) if intake.answers.get("approx_sf") else None,
            structural_system=enriched.get("structural_system") or "",
            scope_description=enriched.get("scope_description") or "",
            architect_name=intake.architect_name or "",
            architect_firm=enriched.get("architect_firm") or "",
            architect_email=intake.lead_contact or "",
            fee_amount=fee_amount,
            complexity=decision["complexity_estimate"],
            mo_conditions=intake.mo_conditions or "",
            mo_notes=intake.mo_notes or "",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    db.save_proposal(intake_id, text)
    return RedirectResponse(url=f"/intakes/{intake_id}#proposal-section", status_code=303)


def _require_mo_passcode_if_configured(passcode: Optional[str]) -> None:
    expected = os.environ.get("AVS_MO_PASSCODE")
    if not expected:
        return
    if (passcode or "") != expected:
        raise HTTPException(status_code=403, detail="Invalid Mo passcode.")


@app.get("/mo-queue", response_class=HTMLResponse)
def mo_queue(request: Request) -> HTMLResponse:
    import json as _json
    intakes = db.list_pending_mo()
    pending_invoices = db.get_pending_invoice_approvals()
    return templates.TemplateResponse(
        "mo_queue.html",
        {
            "request":            request,
            "intakes":            intakes,
            "pending_invoices":   pending_invoices,
            "now_local":          _now_local_iso(),
            "valid_phases":       db.VALID_PHASES,
            "team_colors_json":   _json.dumps(db.TEAM_COLORS),
            "phase_colors_json":  _json.dumps(db.PHASE_COLORS),
            "billing_labels_json": _json.dumps(db.BILLING_PHASE_LABELS),
            "billing_labels":     db.BILLING_PHASE_LABELS,
        },
    )


@app.post("/api/intakes/{intake_id}/mo-review")
async def api_mo_review_json(request: Request, intake_id: int) -> dict:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()
    mo_decision = (body.get("mo_decision") or "").strip().upper()
    status = {
        "PROCEED":                 "PROCEED_TO_PROPOSAL",
        "PROCEED_WITH_CONDITIONS": "PROCEED_WITH_CONDITIONS",
        "REQUEST_CLARIFICATION":   "NEEDS_INFO",
        "DECLINE":                 "DECLINED",
    }.get(mo_decision)
    if not status:
        raise HTTPException(status_code=400, detail="Invalid decision.")
    mo_fee_decision = (body.get("mo_fee_decision") or "").strip().upper() or None
    fee_override = _as_str(str(body.get("mo_fee_override") or "")) if mo_fee_decision == "OVERRIDE" else None
    db.set_mo_review(
        intake_id,
        mo_decision=mo_decision,
        mo_notes=_as_str(str(body.get("mo_notes") or "")),
        mo_conditions=_as_str(str(body.get("mo_conditions") or "")),
        mo_fee_decision=mo_fee_decision,
        mo_fee_override=fee_override,
        status=status,
    )

    project_number: Optional[str] = intake.project_number
    if mo_decision == "PROCEED" and not project_number:
        project_number = db.assign_next_project_number()
        db.set_intake_project_number(intake_id, project_number)
        # Auto-generate phase budgets from approved fee
        approved_fee: Optional[float] = None
        if mo_fee_decision == "OVERRIDE" and fee_override:
            try:
                approved_fee = float(fee_override)
            except (ValueError, TypeError):
                pass
        if approved_fee and approved_fee > 0:
            try:
                db.generate_phase_budgets(intake_id, project_number, approved_fee)
            except Exception:
                pass  # non-fatal: budgets can be generated later
            try:
                db.create_billing_phases_for_project(intake_id, approved_fee)
            except Exception:
                pass  # non-fatal: billing phases can be created later

    return {"success": True, "status": status, "intake_id": intake_id, "project_number": project_number}


@app.post("/api/intakes/{intake_id}/generate-proposal")
async def api_generate_proposal_json(request: Request, intake_id: int) -> dict:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()
    fee_amount = float(body.get("fee_amount") or 0)
    structural_system = str(body.get("structural_system") or "")
    decision = compute_decision(intake.answers)
    enriched = {**intake.answers, "_complexity": decision["complexity_estimate"]}
    if structural_system:
        enriched["structural_system"] = structural_system
    try:
        text = proposal_generator.generate_proposal(
            project_name=intake.project_name or "Project",
            project_type=intake.answers.get("project_type") or "new_construction",
            location=intake.location_region or "",
            building_type=intake.answers.get("building_type") or "retail",
            approx_sf=int(intake.answers["approx_sf"]) if intake.answers.get("approx_sf") else None,
            structural_system=enriched.get("structural_system") or "",
            scope_description=enriched.get("scope_description") or "",
            architect_name=intake.architect_name or "",
            architect_firm=enriched.get("architect_firm") or "",
            architect_email=intake.lead_contact or "",
            fee_amount=fee_amount,
            complexity=decision["complexity_estimate"],
            mo_conditions=intake.mo_conditions or "",
            mo_notes=intake.mo_notes or "",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    db.save_proposal(intake_id, text)
    return {"proposal_text": text, "intake_id": intake_id}


@app.get("/intakes/{intake_id}/mo-review", response_class=HTMLResponse)
def mo_review_get(request: Request, intake_id: int) -> HTMLResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    _mo_decision = compute_decision(intake.answers)
    _mo_answers  = {**intake.answers, "_complexity": _mo_decision["complexity_estimate"]}
    fee_estimate = cognasync_estimate_from_answers(intake.project_name, _mo_answers)
    return templates.TemplateResponse(
        "mo_review.html",
        {
            "request": request,
            "intake": intake,
            "answers": intake.answers,
            "fee_estimate": fee_estimate,
            "now_local": _now_local_iso(),
            "mo_passcode_enabled": bool(os.environ.get("AVS_MO_PASSCODE")),
        },
    )


@app.post("/intakes/{intake_id}/mo-review")
def mo_review_post(
    intake_id: int,
    mo_decision: str = Form(...),
    mo_notes: Optional[str] = Form(None),
    mo_conditions: Optional[str] = Form(None),
    mo_fee_decision: Optional[str] = Form(None),
    mo_fee_override: Optional[str] = Form(None),
    mo_passcode: Optional[str] = Form(None),
    redirect_after: Optional[str] = Form(None),
) -> RedirectResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    _require_mo_passcode_if_configured(mo_passcode)

    decision_norm = mo_decision.strip().upper()
    status = {
        "PROCEED": "PROCEED_TO_PROPOSAL",
        "PROCEED_WITH_CONDITIONS": "PROCEED_WITH_CONDITIONS",
        "REQUEST_CLARIFICATION": "NEEDS_INFO",
        "DECLINE": "DECLINED",
    }.get(decision_norm)
    if not status:
        raise HTTPException(status_code=400, detail="Invalid decision.")

    fee_decision_norm = _as_str(mo_fee_decision)
    if fee_decision_norm:
        fee_decision_norm = fee_decision_norm.upper()
    fee_override = _as_str(mo_fee_override) if fee_decision_norm == "OVERRIDE" else None

    db.set_mo_review(
        intake_id,
        mo_decision=decision_norm,
        mo_notes=_as_str(mo_notes),
        mo_conditions=_as_str(mo_conditions),
        mo_fee_decision=fee_decision_norm,
        mo_fee_override=fee_override,
        status=status,
    )

    destination = redirect_after if redirect_after else f"/intakes/{intake_id}"
    return RedirectResponse(url=destination, status_code=303)


CHECKLIST_KEYS = db.CHECKLIST_KEYS


@app.post("/intakes/{intake_id}/proposal-checklist")
async def proposal_checklist_update(request: Request, intake_id: int) -> RedirectResponse:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    form = await request.form()
    checklist = {key: form.get(key) == "on" for key in CHECKLIST_KEYS}
    db.set_proposal_checklist(intake_id, checklist)
    return RedirectResponse(url=f"/intakes/{intake_id}#proposal-prep", status_code=303)


@app.get("/reports", response_class=HTMLResponse)
def reports(request: Request) -> HTMLResponse:
    all_intakes = db.list_intakes()
    now = datetime.now()

    # 1. Total intakes
    total_intakes = len(all_intakes)

    # 2. Intakes submitted this month (based on inquiry_date or created_at)
    this_month_count = 0
    for row in all_intakes:
        date_str = row.inquiry_date or row.created_at[:10]
        try:
            d = datetime.strptime(date_str[:7], "%Y-%m")
            if d.year == now.year and d.month == now.month:
                this_month_count += 1
        except Exception:
            pass

    # 3 & 4. Conversion/decline rates (only intakes with mo_decision)
    decided = [r for r in all_intakes if r.mo_decision]
    proceed_statuses = {"PROCEED_TO_PROPOSAL", "PROCEED_WITH_CONDITIONS"}
    proceed_count = sum(1 for r in decided if r.status in proceed_statuses)
    decline_count = sum(1 for r in decided if r.status == "DECLINED")
    if decided:
        conversion_rate = f"{round(proceed_count / len(decided) * 100)}%"
        decline_rate = f"{round(decline_count / len(decided) * 100)}%"
    else:
        conversion_rate = "—"
        decline_rate = "—"

    # 5. Average days from inquiry_date to mo_reviewed_at
    day_diffs = []
    for row in all_intakes:
        if row.inquiry_date and row.mo_reviewed_at:
            try:
                d1 = datetime.strptime(row.inquiry_date[:10], "%Y-%m-%d")
                d2 = datetime.strptime(row.mo_reviewed_at[:10], "%Y-%m-%d")
                diff = (d2 - d1).days
                if diff >= 0:
                    day_diffs.append(diff)
            except Exception:
                pass
    avg_days = f"{sum(day_diffs) / len(day_diffs):.1f} days" if day_diffs else "—"

    # 6. Average days from mo_reviewed_at to proposal_completed_at
    mo_to_proposal_diffs = []
    for row in all_intakes:
        if row.mo_reviewed_at and row.proposal_completed_at:
            try:
                d1 = datetime.strptime(row.mo_reviewed_at[:10], "%Y-%m-%d")
                d2 = datetime.strptime(row.proposal_completed_at[:10], "%Y-%m-%d")
                diff = (d2 - d1).days
                if diff >= 0:
                    mo_to_proposal_diffs.append(diff)
            except Exception:
                pass
    avg_mo_to_proposal = (
        f"{sum(mo_to_proposal_diffs) / len(mo_to_proposal_diffs):.1f} days"
        if mo_to_proposal_diffs
        else "—"
    )

    # 7. Most common red flag
    flag_title_counter: Counter = Counter()
    flag_severity_by_title: dict[str, Counter] = defaultdict(Counter)
    for row in all_intakes:
        for flag in row.red_flags:
            title = flag.get("title", "Unknown")
            severity = flag.get("severity", "low")
            flag_title_counter[title] += 1
            flag_severity_by_title[title][severity] += 1

    if flag_title_counter:
        top_title, top_count = flag_title_counter.most_common(1)[0]
        most_common_red_flag = f"{top_title} ({top_count}×)"
    else:
        most_common_red_flag = "—"

    # Table 1: Red Flag Frequency
    SEVERITY_RANK = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    red_flag_table = []
    for title, count in flag_title_counter.most_common():
        sev_counter = flag_severity_by_title[title]
        top_sev = max(sev_counter, key=lambda s: SEVERITY_RANK.get(s, 0))
        red_flag_table.append({"title": title, "count": count, "top_severity": top_sev})

    # Table 2: Intakes by Project Type
    pt_counts: dict[str, dict[str, int]] = defaultdict(lambda: {"total": 0, "proceed": 0, "decline": 0})
    for row in all_intakes:
        pt = row.answers.get("project_type") or "unknown"
        pt_counts[pt]["total"] += 1
        if row.status in proceed_statuses:
            pt_counts[pt]["proceed"] += 1
        elif row.status == "DECLINED":
            pt_counts[pt]["decline"] += 1
    project_type_table = sorted(
        [{"type": k, **v} for k, v in pt_counts.items()],
        key=lambda x: x["total"],
        reverse=True,
    )

    # Table 3: Architect Intelligence
    arch_data: dict[str, dict[str, Any]] = {}
    for row in all_intakes:
        name = (row.architect_name or "").strip()
        if not name:
            continue
        if name not in arch_data:
            arch_data[name] = {
                "total": 0,
                "proceed": 0,
                "decline": 0,
                "total_red_flags": 0,
                "flag_titles": Counter(),
            }
        entry = arch_data[name]
        entry["total"] += 1
        if row.status in proceed_statuses:
            entry["proceed"] += 1
        elif row.status == "DECLINED":
            entry["decline"] += 1
        entry["total_red_flags"] += row.red_flag_counts.get("total", 0)
        for flag in row.red_flags:
            title = flag.get("title", "Unknown")
            entry["flag_titles"][title] += 1

    architect_table = []
    for name, entry in arch_data.items():
        if entry["total"] < 2:
            continue
        avg_flags = round(entry["total_red_flags"] / entry["total"], 1)
        top_flag = entry["flag_titles"].most_common(1)[0][0] if entry["flag_titles"] else "—"
        architect_table.append({
            "name": name,
            "total": entry["total"],
            "proceed": entry["proceed"],
            "decline": entry["decline"],
            "avg_red_flags": avg_flags,
            "top_flag": top_flag,
        })
    architect_table.sort(key=lambda x: x["total"], reverse=True)

    # ── Chart data ──────────────────────────────────────────────────────────
    # Status donut
    _STATUS_LABELS = {
        "PROCEED_TO_PROPOSAL":     "Proceed",
        "PENDING_MO_REVIEW":       "Mo Review",
        "NEEDS_INFO":              "Needs Info",
        "PROCEED_WITH_CONDITIONS": "With Conditions",
        "DECLINED":                "Declined",
    }
    _raw_status = Counter(r.status for r in all_intakes)
    status_chart_labels = list(_STATUS_LABELS.values())
    status_chart_values = [_raw_status.get(k, 0) for k in _STATUS_LABELS]

    # Monthly intake trend
    _monthly_raw: dict[str, int] = defaultdict(int)
    for _r in all_intakes:
        _src = (_r.inquiry_date or _r.created_at or "")[:7]
        if len(_src) == 7:
            _monthly_raw[_src] += 1
    _sorted_months = sorted(_monthly_raw)
    monthly_labels = _sorted_months
    monthly_values = [_monthly_raw[m] for m in _sorted_months]

    # Red flag frequency (top 10, longest-title truncated to 32 chars)
    rf_labels   = [r["title"][:32] for r in red_flag_table[:10]]
    rf_values   = [r["count"]      for r in red_flag_table[:10]]
    rf_sevs     = [r["top_severity"] for r in red_flag_table[:10]]

    # Project type stacked bar
    pt_labels  = [r["type"]    for r in project_type_table]
    pt_proceed = [r["proceed"] for r in project_type_table]
    pt_decline = [r["decline"] for r in project_type_table]
    pt_pending = [r["total"] - r["proceed"] - r["decline"] for r in project_type_table]

    # Severity breakdown donut
    _sev_raw: Counter = Counter()
    for _r in all_intakes:
        for _flag in _r.red_flags:
            _sev_raw[_flag.get("severity", "low")] += 1
    severity_labels = ["Critical", "High", "Medium", "Low"]
    severity_values = [_sev_raw.get(s, 0) for s in ("critical", "high", "medium", "low")]

    # Approved fee pipeline
    _approved_statuses = {"PROCEED_TO_PROPOSAL", "PROCEED_WITH_CONDITIONS"}
    approved_rows: list[dict[str, Any]] = []
    approved_fee_total = 0.0
    for _r in all_intakes:
        if _r.status not in _approved_statuses:
            continue
        fee_amount: Optional[float] = None
        fee_source = "—"
        if _r.mo_fee_decision == "OVERRIDE" and _r.mo_fee_override:
            try:
                fee_amount = float(_r.mo_fee_override)
                fee_source = "Mo override"
            except (ValueError, TypeError):
                pass
        elif _r.mo_fee_decision in {"ACCEPTED", None}:
            est = cognasync_estimate_from_answers(_r.project_name, _r.answers)
            fee_range = (est or {}).get("suggested_fee_range")
            if fee_range:
                low  = fee_range.get("low") or 0
                high = fee_range.get("high") or 0
                if low and high:
                    fee_amount = (low + high) / 2
                    fee_source = "Estimate midpoint" if _r.mo_fee_decision == "ACCEPTED" else "Auto estimate"
        if fee_amount is not None:
            approved_fee_total += fee_amount
        approved_rows.append({
            "id":           _r.id,
            "project_name": _r.project_name,
            "client_name":  _r.client_name or "—",
            "status":       _r.status,
            "fee_source":   fee_source,
            "fee_amount":   fee_amount,
            "mo_decision":  _r.mo_decision,
        })
    approved_fee_total_fmt = f"${approved_fee_total:,.0f}" if approved_fee_total else "—"

    import json as _json
    return templates.TemplateResponse(
        "reports.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            # KPI cards
            "total_intakes": total_intakes,
            "this_month_count": this_month_count,
            "conversion_rate": conversion_rate,
            "decline_rate": decline_rate,
            "avg_days": avg_days,
            "avg_mo_to_proposal": avg_mo_to_proposal,
            "most_common_red_flag": most_common_red_flag,
            # Approved fee pipeline
            "approved_fee_total": approved_fee_total_fmt,
            "approved_rows": approved_rows,
            # Tables (kept for aria / fallback)
            "red_flag_table": red_flag_table,
            "project_type_table": project_type_table,
            "architect_table": architect_table,
            # Chart JSON blobs
            "j_status_labels":  _json.dumps(status_chart_labels),
            "j_status_values":  _json.dumps(status_chart_values),
            "j_monthly_labels": _json.dumps(monthly_labels),
            "j_monthly_values": _json.dumps(monthly_values),
            "j_rf_labels":      _json.dumps(rf_labels),
            "j_rf_values":      _json.dumps(rf_values),
            "j_rf_sevs":        _json.dumps(rf_sevs),
            "j_pt_labels":      _json.dumps(pt_labels),
            "j_pt_proceed":     _json.dumps(pt_proceed),
            "j_pt_decline":     _json.dumps(pt_decline),
            "j_pt_pending":     _json.dumps(pt_pending),
            "j_sev_labels":     _json.dumps(severity_labels),
            "j_sev_values":     _json.dumps(severity_values),
        },
    )


@app.get("/past-projects", response_class=HTMLResponse)
def past_projects(request: Request) -> HTMLResponse:
    import json as _json
    error: Optional[str] = None
    type_options = project_search.DEFAULT_TYPE_OPTIONS
    total = 0
    company_options: list = []
    try:
        data = project_search.get_projects()
        type_options = data["type_options"]
        total = data["total"]
        company_options = data.get("company_options", [])
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        "past_projects.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "type_options_json": _json.dumps(type_options),
            "type_keys": list(type_options.keys()),
            "total": total,
            "company_options": company_options,
            "error": error,
        },
    )


@app.get("/api/past-projects")
def api_past_projects(
    type: Optional[str] = None,
    wallSystem: Optional[str] = None,
    roof: Optional[str] = None,
    slab: Optional[str] = None,
    foundation: Optional[str] = None,
    company: Optional[str] = None,
    limit: int = 500,
) -> dict[str, Any]:
    filters = {
        "type":        type or "",
        "wallSystem":  wallSystem or "",
        "roof":        roof or "",
        "slab":        slab or "",
        "foundation":  foundation or "",
        "company":     company or "",
    }
    try:
        return project_search.search_projects(filters, limit=min(max(limit, 1), 5000))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/past-projects/refresh")
def api_past_projects_refresh() -> dict[str, str]:
    project_search.invalidate_cache()
    return {"status": "cache cleared"}


@app.get("/api/calendar/ifp-dates")
def api_calendar_ifp_dates(days: int = 180) -> dict[str, Any]:
    try:
        dates = calendar_sync.get_ifp_events(days_ahead=min(max(days, 30), 365))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "ok": True,
        "configured": calendar_sync.has_config(),
        "dates": dates,
    }


@app.post("/api/calendar/refresh")
def api_calendar_refresh() -> dict[str, str]:
    calendar_sync.invalidate_cache()
    return {"status": "cache cleared"}


@app.get("/calendar", response_class=HTMLResponse)
def calendar_page(request: Request) -> HTMLResponse:
    import json as _json
    return templates.TemplateResponse(
        "calendar.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "phase_colors": _json.dumps(db.PHASE_COLORS),
            "valid_phases": db.VALID_PHASES,
            "team_colors": _json.dumps(db.TEAM_COLORS),
            "team_members": db.TEAM_MEMBERS,
        },
    )


def _validate_calendar_payload(body: dict) -> None:
    if not body.get("phase"):
        raise HTTPException(status_code=400, detail="phase is required.")
    if body["phase"] not in db.VALID_PHASES:
        raise HTTPException(status_code=400, detail=f"Invalid phase: {body['phase']}")
    if not body.get("start_date"):
        raise HTTPException(status_code=400, detail="start_date is required.")
    if not body.get("end_date"):
        raise HTTPException(status_code=400, detail="end_date is required.")


@app.get("/api/calendar/events")
def api_calendar_events_list(
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> list[dict]:
    events = db.list_calendar_events(start=start, end=end)
    return [e.to_dict() for e in events]


@app.get("/api/calendar/events/check-conflict")
def api_calendar_check_conflict(date: str, phase: str) -> dict:
    if phase != "IFP":
        return {"conflict": False, "count": 0}
    count = db.count_ifp_on_date(date)
    return {"conflict": count >= 2, "count": count}


@app.post("/api/calendar/events")
async def api_calendar_events_create(request: Request) -> dict:
    from datetime import date as _date, timedelta
    body = await request.json()
    _validate_calendar_payload(body)
    conflict = False
    if body.get("phase") == "IFP" and not body.get("is_ooo"):
        start_d = _date.fromisoformat(body["start_date"][:10])
        end_d = _date.fromisoformat(body["end_date"][:10])
        d = start_d
        while d <= end_d:
            if db.count_ifp_on_date(d.isoformat()) >= 2:
                conflict = True
                break
            d += timedelta(days=1)

    tier_raw = body.get("tier")
    tier = int(tier_raw) if tier_raw and str(tier_raw).isdigit() and 1 <= int(tier_raw) <= 5 else None
    event_id = db.create_calendar_event(
        project_number=body.get("project_number") or "",
        client=body.get("client") or "",
        location=body.get("location") or "",
        phase=body["phase"],
        team=body.get("team") or [],
        project_type=body.get("project_type") or "",
        start_date=body["start_date"],
        end_date=body["end_date"],
        is_ooo=bool(body.get("is_ooo", False)),
        tier=tier,
        phase_jump=bool(body.get("phase_jump", False)),
        metadata=body.get("metadata"),
    )
    event = db.get_calendar_event(event_id)
    return {**(event.to_dict() if event else {"id": event_id}), "ifp_conflict": conflict}


@app.put("/api/calendar/events/{event_id}")
async def api_calendar_events_update(request: Request, event_id: str) -> dict:
    existing = db.get_calendar_event(event_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()
    _validate_calendar_payload(body)
    tier_raw = body.get("tier")
    tier = int(tier_raw) if tier_raw and str(tier_raw).isdigit() and 1 <= int(tier_raw) <= 5 else None
    db.update_calendar_event(
        event_id,
        project_number=body.get("project_number") or "",
        client=body.get("client") or "",
        location=body.get("location") or "",
        phase=body["phase"],
        team=body.get("team") or [],
        project_type=body.get("project_type") or "",
        start_date=body["start_date"],
        end_date=body["end_date"],
        is_ooo=bool(body.get("is_ooo", False)),
        tier=tier,
        phase_jump=bool(body.get("phase_jump", False)),
        metadata=body.get("metadata"),
    )
    updated = db.get_calendar_event(event_id)
    return updated.to_dict() if updated else {"id": event_id}


@app.delete("/api/calendar/events/{event_id}")
def api_calendar_events_delete(event_id: str) -> dict:
    existing = db.get_calendar_event(event_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Not found.")
    db.delete_calendar_event(event_id)
    return {"deleted": event_id}


# ── Projected Capacity ────────────────────────────────────────────────────────

@app.get("/api/capacity/projected")
def api_projected_capacity(start: str, end: str) -> dict[str, Any]:
    try:
        ws = date.fromisoformat(start)
        we = date.fromisoformat(end)
    except ValueError:
        raise HTTPException(status_code=400, detail="start and end must be YYYY-MM-DD.")
    if we < ws:
        raise HTTPException(status_code=400, detail="end must be >= start.")
    return db.get_all_projected_capacity(ws, we)


# ── Schedule Generator ────────────────────────────────────────────────────────

@app.post("/api/calendar/preview-schedule")
async def api_preview_schedule(request: Request) -> dict[str, Any]:
    body = await request.json()
    start_str = (body.get("start_date") or "")[:10]
    end_str   = (body.get("end_date") or "")[:10]
    project_type = body.get("project_type") or "new_construction"
    try:
        ws = date.fromisoformat(start_str)
        we = date.fromisoformat(end_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="start_date and end_date required (YYYY-MM-DD).")
    if we <= ws:
        raise HTTPException(status_code=400, detail="end_date must be after start_date.")
    phases = db.calculate_phase_schedule(ws, we, project_type)
    capacity = db.get_all_projected_capacity(ws, we)
    return {"phases": phases, "capacity": capacity}


@app.post("/api/calendar/create-schedule")
async def api_create_schedule(request: Request) -> dict[str, Any]:
    body = await request.json()
    phases = body.get("phases") or []
    if not phases:
        raise HTTPException(status_code=400, detail="phases array required.")
    project_number = body.get("project_number") or ""
    client_name    = body.get("client") or ""
    project_type   = body.get("project_type") or ""
    tier_raw       = body.get("tier")
    tier = int(tier_raw) if tier_raw and str(tier_raw).isdigit() and 1 <= int(tier_raw) <= 5 else None
    team = body.get("team") or []
    created_ids = []
    for phase in phases:
        phase_code = phase.get("phase_code") or phase.get("phase") or ""
        start_d    = (phase.get("start_date") or "")[:10]
        end_d      = (phase.get("end_date") or "")[:10]
        if not phase_code or not start_d or not end_d:
            continue
        event_id = db.create_calendar_event(
            project_number=project_number,
            client=client_name,
            location=body.get("location") or "",
            phase=phase_code,
            team=team,
            project_type=project_type,
            start_date=start_d + "T00:00:00Z",
            end_date=end_d + "T23:59:59Z",
            is_ooo=False,
            tier=tier,
            phase_jump=False,
        )
        created_ids.append(event_id)
    return {"created_event_ids": created_ids, "count": len(created_ids)}


# ── Time-Off Management ───────────────────────────────────────────────────────

@app.get("/time-off", response_class=HTMLResponse)
def timeoff_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "timeoff.html",
        {
            "request":       request,
            "now_local":     _now_local_iso(),
            "team_members":  db.TEAM_MEMBERS,
            "reasons":       db.TIME_OFF_REASONS,
            "team_colors":   _json.dumps(db.TEAM_COLORS),
        },
    )


@app.get("/api/time-off")
def api_list_time_off(
    engineer: Optional[str] = None,
    start:    Optional[str] = None,
    end:      Optional[str] = None,
) -> list[dict[str, Any]]:
    return db.list_time_off(engineer=engineer, start=start, end=end)


@app.post("/api/time-off")
async def api_create_time_off(request: Request) -> dict[str, Any]:
    body = await request.json()
    engineer = (body.get("engineer_initials") or "").strip().upper()
    start_d  = (body.get("start_date") or "").strip()
    end_d    = (body.get("end_date") or "").strip()
    reason   = (body.get("reason") or "Vacation").strip()
    notes    = _as_str(str(body.get("notes") or ""))
    created_by = _as_str(str(body.get("created_by") or ""))
    if not engineer:
        raise HTTPException(status_code=400, detail="engineer_initials required.")
    if not start_d or not end_d:
        raise HTTPException(status_code=400, detail="start_date and end_date required.")
    try:
        s = date.fromisoformat(start_d)
        e = date.fromisoformat(end_d)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be YYYY-MM-DD.")
    if e < s:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date.")
    if reason not in db.TIME_OFF_REASONS:
        reason = "Other"
    time_off_id = db.create_time_off(
        engineer_initials=engineer,
        start_date=start_d,
        end_date=end_d,
        reason=reason,
        notes=notes,
        created_by=created_by,
    )
    entries = db.list_time_off(engineer=engineer, start=start_d, end=end_d)
    created = next((r for r in entries if r["id"] == time_off_id), {"id": time_off_id})
    return created


@app.delete("/api/time-off/{time_off_id}")
def api_delete_time_off(time_off_id: int) -> dict[str, Any]:
    db.delete_time_off(time_off_id)
    return {"deleted": time_off_id}


@app.get("/api/intakes/{intake_id}/fee-estimate")
def api_intake_fee_estimate(intake_id: int) -> dict:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    decision = compute_decision(intake.answers)
    enriched = {**intake.answers, "_complexity": decision["complexity_estimate"]}
    est = cognasync_estimate_from_answers(intake.project_name, enriched)
    midpoint: Optional[float] = None
    if est and not est.get("needs_manual_review"):
        fee_range = est.get("suggested_fee_range") or {}
        lo = fee_range.get("low") or 0
        hi = fee_range.get("high") or 0
        if lo and hi:
            midpoint = round((lo + hi) / 2 / 500) * 500
    return {
        "intake_id":        intake_id,
        "project_name":     intake.project_name,
        "complexity":       decision["complexity_estimate"],
        "fee_range":        decision.get("fee_range_estimate"),
        "cognasync":        est,
        "suggested_midpoint": midpoint,
        "client_name":      intake.client_name or "",
        "location_region":  intake.location_region or "",
        "project_type":     intake.answers.get("project_type") or "",
        "building_type":    intake.answers.get("building_type") or "",
        "approx_sf":        intake.answers.get("approx_sf") or "",
        "architect_name":   intake.architect_name or "",
    }


@app.get("/capacity", response_class=HTMLResponse)
def capacity_page(request: Request) -> HTMLResponse:
    import json as _json
    return templates.TemplateResponse(
        "capacity.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "team_colors":      _json.dumps(db.TEAM_COLORS),
            "phase_colors":     _json.dumps(db.PHASE_COLORS),
            "valid_phases":     db.VALID_PHASES,
            "team_members":     db.TEAM_MEMBERS,
            "team_full_names":  _json.dumps(db.TEAM_FULL_NAMES),
        },
    )


@app.get("/api/capacity")
def api_capacity() -> dict:
    import json as _json
    from datetime import date
    today = date.today().isoformat()
    events = db.list_calendar_events(start=today + "T00:00:00Z")
    snapshot = weu_engine.get_capacity_snapshot([e.to_dict() for e in events])
    return snapshot


# ── Settings ─────────────────────────────────────────────────────────────────

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    seed_row = db.get_project_number_seed()
    billing_defs = db.get_billing_phase_definitions()
    return templates.TemplateResponse(
        "settings.html",
        {
            "request":      request,
            "now_local":    _now_local_iso(),
            "last_number":  seed_row.get("last_number", 9000),
            "updated_at":   seed_row.get("updated_at"),
            "billing_defs": billing_defs,
        },
    )


@app.post("/api/settings/project-number-seed")
async def api_set_project_number_seed(request: Request) -> dict[str, Any]:
    body = await request.json()
    try:
        seed = int(body.get("seed", 0))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="seed must be an integer.")
    if seed < 1000 or seed > 99999:
        raise HTTPException(status_code=400, detail="Seed must be between 1000 and 99999.")
    db.set_project_number_seed(seed)
    return {"success": True, "seed": seed, "next_number": seed + 1}


# ── Phase Budgets ─────────────────────────────────────────────────────────────

@app.get("/api/intakes/{intake_id}/phase-budgets")
def api_get_phase_budgets(intake_id: int) -> list[dict[str, Any]]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    return db.list_phase_budgets(intake_id)


@app.patch("/api/intakes/{intake_id}/phase-budgets/{phase_code}")
async def api_update_phase_budget(
    request: Request, intake_id: int, phase_code: str
) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()
    try:
        budgeted_hours = float(body.get("budgeted_hours", 0))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="budgeted_hours must be a number.")
    if budgeted_hours < 0:
        raise HTTPException(status_code=400, detail="budgeted_hours must be non-negative.")
    db.update_phase_budget(intake_id, phase_code, budgeted_hours)
    return {"success": True, "intake_id": intake_id, "phase_code": phase_code, "budgeted_hours": budgeted_hours}


@app.get("/api/intakes/{intake_id}/phase-matrix")
def api_phase_matrix(intake_id: int) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    budgets = db.list_phase_budgets(intake_id)
    entries = db.list_time_entries_for_intake(intake_id)

    by_phase_eng: dict[str, dict[str, float]] = {}
    for e in entries:
        phase = e["phase_code"]
        eng = e["engineer_initials"]
        by_phase_eng.setdefault(phase, {})
        by_phase_eng[phase][eng] = by_phase_eng[phase].get(eng, 0.0) + float(e["hours"])

    total_budget = total_used = 0.0
    phases = []
    for b in budgets:
        phase = b["phase_code"]
        eng_map = by_phase_eng.get(phase, {})
        engineers = [
            {
                "initials": eng,
                "role": db.ENGINEER_ROLES.get(eng, ""),
                "hours": round(hrs, 2),
                "color": db.TEAM_COLORS.get(eng, "#888"),
            }
            for eng, hrs in sorted(eng_map.items(), key=lambda x: -x[1])
        ]
        total_budget += b["budgeted_hours"]
        total_used += b["hours_used"]
        phases.append({**b, "engineers": engineers})

    total_remaining = round(total_budget - total_used, 2)
    total_pct = round(total_used / total_budget * 100, 1) if total_budget > 0 else 0.0
    return {
        "phases": phases,
        "totals": {
            "budgeted_hours": round(total_budget, 2),
            "hours_used": round(total_used, 2),
            "remaining": total_remaining,
            "pct_used": total_pct,
        },
    }


@app.get("/api/intakes/{intake_id}/time-entries")
def api_intake_time_entries(intake_id: int) -> list[dict[str, Any]]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    return db.list_time_entries_for_intake(intake_id)


@app.patch("/api/intakes/{intake_id}/schedule")
async def api_patch_schedule(request: Request, intake_id: int) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    if not intake.ifp_due_date:
        raise HTTPException(status_code=400, detail="No IFP due date set.")
    body = await request.json()
    try:
        shift_days = int(body.get("shift_days", 0))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="shift_days must be an integer.")
    if not (-14 <= shift_days <= 14):
        raise HTTPException(status_code=400, detail="shift_days must be between -14 and 14.")
    new_date = date.fromisoformat(intake.ifp_due_date) + timedelta(days=shift_days)
    db.update_intake_ifp_date(intake_id, new_date.isoformat())
    return {"success": True, "ifp_due_date": new_date.isoformat()}


# ── Time Entries ──────────────────────────────────────────────────────────────

@app.get("/api/time-entries")
def api_list_time_entries(
    start: Optional[str] = None,
    end: Optional[str] = None,
    engineer: Optional[str] = None,
) -> list[dict[str, Any]]:
    return db.list_time_entries(start=start, end=end, engineer=engineer)


@app.post("/api/time-entries")
async def api_create_time_entry(request: Request) -> dict[str, Any]:
    body = await request.json()
    engineer = (body.get("engineer_initials") or "").strip().upper()
    project_number = (body.get("project_number") or "").strip()
    phase_code = (body.get("phase_code") or "").strip()
    entry_date = (body.get("entry_date") or "").strip()
    notes = _as_str(str(body.get("notes") or ""))
    if not engineer:
        raise HTTPException(status_code=400, detail="engineer_initials is required.")
    if not project_number:
        raise HTTPException(status_code=400, detail="project_number is required.")
    if not phase_code:
        raise HTTPException(status_code=400, detail="phase_code is required.")
    if not entry_date:
        raise HTTPException(status_code=400, detail="entry_date is required.")
    try:
        hours = float(body.get("hours", 0))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="hours must be a number.")
    if hours <= 0 or hours > 24:
        raise HTTPException(status_code=400, detail="hours must be between 0 and 24.")
    intake_id_raw = body.get("intake_id")
    intake_id: Optional[int] = int(intake_id_raw) if intake_id_raw else None
    entry_id = db.create_time_entry(
        engineer_initials=engineer,
        project_number=project_number,
        intake_id=intake_id,
        phase_code=phase_code,
        entry_date=entry_date,
        hours=hours,
        notes=notes,
    )
    return {"id": entry_id, "success": True}


@app.patch("/api/time-entries/{entry_id}")
async def api_update_time_entry(request: Request, entry_id: int) -> dict[str, Any]:
    entry = db.get_time_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()
    try:
        hours = float(body.get("hours", entry["hours"]))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="hours must be a number.")
    if hours <= 0 or hours > 24:
        raise HTTPException(status_code=400, detail="hours must be between 0 and 24.")
    notes = _as_str(str(body.get("notes") or ""))
    db.update_time_entry(entry_id, hours=hours, notes=notes)
    return {"success": True, "id": entry_id}


@app.delete("/api/time-entries/{entry_id}")
def api_delete_time_entry(entry_id: int) -> dict[str, Any]:
    entry = db.get_time_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Not found.")
    db.delete_time_entry(entry_id)
    return {"deleted": entry_id}


@app.get("/api/active-projects")
def api_active_projects(engineer: Optional[str] = None) -> list[dict[str, Any]]:
    return db.list_active_projects(engineer=engineer)


# ── Timesheet page ────────────────────────────────────────────────────────────

@app.get("/timesheet", response_class=HTMLResponse)
def timesheet_page(request: Request) -> HTMLResponse:
    import json as _json
    start, end = _current_pay_period()
    return templates.TemplateResponse(
        "timesheet.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "team_members": db.TEAM_MEMBERS,
            "team_full_names_json": _json.dumps(db.TEAM_FULL_NAMES),
            "phase_colors_json": _json.dumps(db.PHASE_COLORS),
            "valid_phases": db.VALID_PHASES,
            "default_period_start": start,
            "default_period_end": end,
        },
    )


# ── Payroll Export ────────────────────────────────────────────────────────────

@app.get("/payroll-export", response_class=HTMLResponse)
def payroll_export_page(request: Request) -> HTMLResponse:
    # Default: most recently completed pay period
    today = date.today()
    start, end = _current_pay_period()
    if end >= today.isoformat():
        # go back one period
        prev_start = date.fromisoformat(start) - timedelta(days=14)
        prev_end = date.fromisoformat(end) - timedelta(days=14)
        start, end = prev_start.isoformat(), prev_end.isoformat()
    return templates.TemplateResponse(
        "payroll_export.html",
        {
            "request": request,
            "now_local": _now_local_iso(),
            "default_start": start,
            "default_end": end,
        },
    )


@app.get("/api/payroll-export")
def api_payroll_export(start: Optional[str] = None, end: Optional[str] = None) -> dict[str, Any]:
    if not start or not end:
        s, e = _current_pay_period()
        start = start or s
        end = end or e
    return db.get_payroll_data(start, end)


@app.get("/api/payroll-export/csv")
def api_payroll_export_csv(start: Optional[str] = None, end: Optional[str] = None) -> StreamingResponse:
    if not start or not end:
        s, e = _current_pay_period()
        start = start or s
        end = end or e

    data = db.get_payroll_data(start, end)
    entries = data.get("entries") or []
    intake_by_pn: dict = data.get("intake_by_pn") or {}

    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow([
        "Pay Period Start", "Pay Period End", "Engineer", "Project Number",
        "Client", "Phase", "Date", "Hours", "Notes",
    ])
    for e in entries:
        pn = e.get("project_number") or ""
        intake = intake_by_pn.get(pn, {})
        client = intake.get("client_name") or ""
        writer.writerow([
            start, end,
            e.get("engineer_initials") or "",
            pn,
            client,
            e.get("phase_code") or "",
            e.get("entry_date") or "",
            f"{float(e.get('hours', 0)):.2f}",
            e.get("notes") or "",
        ])

    output.seek(0)
    filename = f"avs-payroll-{start}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/pipeline", response_class=HTMLResponse)
def pipeline_page(request: Request) -> HTMLResponse:
    data = db.get_pipeline_data()
    return templates.TemplateResponse(
        "pipeline.html",
        {
            "request":         request,
            "now_local":       _now_local_iso(),
            "pipeline":        data,
            "team_colors_json": _json.dumps(db.TEAM_COLORS),
            "phase_colors_json": _json.dumps(db.PHASE_COLORS),
            "prod_phase_order": _json.dumps(db.PRODUCTION_PHASE_ORDER),
            "billing_labels":   _json.dumps(db.BILLING_PHASE_LABELS),
        },
    )


@app.get("/api/pipeline")
def api_pipeline() -> dict[str, Any]:
    return db.get_pipeline_data()


@app.post("/api/projects/{intake_id}/advance-production-phase")
async def api_advance_production_phase(request: Request, intake_id: int) -> dict[str, Any]:
    body = await request.json()
    to_phase = str(body.get("to_phase") or "").strip()
    completed_by = str(body.get("completed_by") or "").strip()
    note = str(body.get("note") or "").strip()
    if not to_phase:
        raise HTTPException(status_code=400, detail="to_phase required")
    if not note:
        raise HTTPException(status_code=400, detail="note required")
    return db.advance_production_phase(intake_id, to_phase, completed_by or "SYSTEM", note)


@app.post("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/approve-invoice")
async def api_approve_invoice(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    body = await request.json()
    return db.approve_invoice(
        intake_id,
        billing_phase_code,
        approved_by=str(body.get("approved_by") or "MO"),
        fee_override=float(body["fee_override"]) if body.get("fee_override") is not None else None,
        note=str(body.get("note") or "") or None,
    )


@app.post("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/decline-invoice")
async def api_decline_invoice(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    body = await request.json()
    reason = str(body.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="reason required")
    return db.decline_invoice(
        intake_id,
        billing_phase_code,
        declined_by=str(body.get("declined_by") or "MO"),
        reason=reason,
    )


@app.post("/api/projects/{intake_id}/toggle-change-order")
async def api_toggle_change_order(request: Request, intake_id: int) -> dict[str, Any]:
    body = await request.json()
    pending = bool(body.get("pending", False))
    note = str(body.get("note") or "") or None
    db.set_change_order(intake_id, pending, note)
    return {"success": True, "change_order_pending": pending}


@app.get("/api/projects/{intake_id}/billing-phases")
def api_project_billing_phases(intake_id: int) -> list[dict[str, Any]]:
    return db.get_project_billing_phases(intake_id)


@app.get("/api/billing-phase-definitions")
def api_billing_phase_definitions() -> list[dict[str, Any]]:
    return db.get_billing_phase_definitions()


@app.post("/api/billing-phase-definitions")
async def api_update_billing_phase_definitions(request: Request) -> dict[str, Any]:
    body = await request.json()
    updates = body.get("phases", [])
    total = sum(float(u.get("default_pct", 0)) for u in updates)
    if abs(total - 1.0) > 0.001:
        raise HTTPException(
            status_code=400,
            detail=f"Percentages total {total * 100:.1f}% — they must sum to 100%.",
        )
    for u in updates:
        db.update_billing_phase_definition(u["code"], float(u["default_pct"]))
    return {"success": True}


@app.get("/api/intakes/{intake_id}/proposal")
def api_get_proposal_text(intake_id: int) -> dict:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found")
    return {"proposal_text": intake.proposal_text or "", "intake_id": intake_id}


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/intakes")
def api_intakes(status: Optional[str] = None) -> list[dict[str, Any]]:
    rows = db.list_intakes(status=status)
    result = []
    for r in rows:
        answers = r.answers or {}
        cx = complexity_estimate(answers)
        fee_review, fee_review_reason = check_fee_review_required(answers, cx)
        result.append({
            "id": r.id,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
            "inquiry_date": r.inquiry_date,
            "project_name": r.project_name,
            "client_name": r.client_name,
            "architect_name": r.architect_name,
            "status": r.status,
            "recommendation": r.recommendation,
            "recommendation_reason": r.recommendation_reason,
            "red_flag_counts": r.red_flag_counts,
            "mo_decision": r.mo_decision,
            "mo_reviewed_at": r.mo_reviewed_at,
            "fee_requires_review": fee_review,
            "review_reason": fee_review_reason,
        })
    return result


@app.get("/api/intakes/by-project-number/{project_number}")
def api_intake_by_project_number(project_number: str) -> dict[str, Any]:
    rows = db.list_intakes()
    for r in rows:
        if str(r.project_number or "").strip() == project_number.strip():
            return {
                "id": r.id,
                "project_name": r.project_name,
                "client_name": r.client_name,
                "ifp_due_date": r.ifp_due_date,
            }
    raise HTTPException(status_code=404, detail="No intake found for that project number.")
