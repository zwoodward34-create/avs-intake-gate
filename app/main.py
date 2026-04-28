from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from fastapi import UploadFile, File

from . import calendar_sync
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


templates.env.filters["badge_class"] = _badge_class
templates.env.filters["days_ago"] = _days_ago
templates.env.globals["pending_mo_count"] = lambda: len(db.list_pending_mo())


app = FastAPI(title="AVS Intake Gate")
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")


@app.on_event("startup")
def _startup() -> None:
    db.init_db()


@app.get("/", response_class=HTMLResponse)
def launch(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "launch.html",
        {"request": request, "now_local": _now_local_iso(), "hide_nav_actions": True},
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
    if intake.mo_fee_decision == "OVERRIDE" and intake.mo_fee_override:
        try:
            proposal_fee_default = float(intake.mo_fee_override)
        except (ValueError, TypeError):
            pass
    elif (
        intake.mo_fee_decision == "ACCEPTED"
        and cognasync_estimate
        and not cognasync_estimate.needs_manual_review
    ):
        lo = cognasync_estimate.suggested_fee_range.low
        hi = cognasync_estimate.suggested_fee_range.high
        proposal_fee_default = round((lo + hi) / 2 / 500) * 500

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
async def intake_upload_post(request: Request, file: UploadFile = File(...)) -> HTMLResponse:
    import types

    error: Optional[str] = None
    prefill: dict[str, Any] = {}

    try:
        raw = await file.read()
        if not raw:
            raise ValueError("Uploaded file is empty.")
        text = document_extractor.extract_text(file.filename or "upload.txt", raw)
        if text.startswith("[") and "error" in text.lower():
            raise ValueError(text)
        prefill = document_extractor.extract_intake_fields(text)
    except Exception as exc:
        error = str(exc)
        return templates.TemplateResponse(
            "upload_intake.html",
            {"request": request, "now_local": _now_local_iso(), "error": error},
        )

    # Parse city/state from location_region ("Kissimmee, FL" → city, state)
    city, state = "", ""
    loc = (prefill.get("location_region") or "").strip()
    if "," in loc:
        parts = [p.strip() for p in loc.split(",", 1)]
        city = parts[0]
        state = parts[1][:2].upper() if len(parts) > 1 else ""

    # Build a pseudo-intake and answers dict for the form template
    fake_intake = types.SimpleNamespace(
        id=None,
        project_name=prefill.get("project_name") or "",
        client_name=prefill.get("client_name") or "",
        architect_name=prefill.get("architect_name") or "",
        lead_contact=prefill.get("lead_contact") or "",
        location_region=loc,
        submitted_by="",
        inquiry_date="",
        ifp_due_date=prefill.get("deadline_date") or "",
        status="",
    )

    answers: dict[str, Any] = {
        "city": city,
        "state": state,
        "approx_sf": prefill.get("approx_sf") or "",
        "est_construction_cost": prefill.get("est_construction_cost") or "",
        "project_type": prefill.get("project_type") or "unknown",
        "building_type": prefill.get("building_type") or "other",
        "structural_system": prefill.get("structural_system") or "",
        "scope_description": prefill.get("scope_description") or "",
        "architect_firm": prefill.get("architect_firm") or "",
    }

    return templates.TemplateResponse(
        "intake_form.html",
        {
            "request": request,
            "mode": "new",
            "intake": fake_intake,
            "answers": answers,
            "now_local": _now_local_iso(),
            "project_templates": db.list_templates(),
            "prefill_notice": f"Pre-filled from: {file.filename}",
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
    intakes = db.list_pending_mo()
    return templates.TemplateResponse(
        "mo_queue.html",
        {
            "request": request,
            "intakes": intakes,
            "now_local": _now_local_iso(),
        },
    )


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
            if est:
                low  = est["suggested_fee_range"]["low"]
                high = est["suggested_fee_range"]["high"]
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
