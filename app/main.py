from __future__ import annotations

import os
from dotenv import load_dotenv
load_dotenv()

from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

import csv
import io
import json as _json
import re as _re
from datetime import date, timedelta

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from fastapi import UploadFile, File

from . import calendar_sync
from . import weu as weu_engine
from . import db
from . import document_extractor
from . import project_search
from . import proposal_generator
from . import staffing
from .decision import compute_decision, complexity_estimate
from .fee_estimator import cognasync_estimate_from_answers, check_fee_review_required
from .weu import ROLE_BUCKETS as _WEU_ROLE_BUCKETS


APP_DIR = Path(__file__).resolve().parent

templates = Jinja2Templates(directory=str(APP_DIR / "templates"))


def _status_from_recommendation(recommendation: str) -> str:
    # All new intakes go directly into the decision queue regardless of recommendation.
    # The actual proceed/conditions/decline/needs-info decision is made from the queue.
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
        "contact_title": _as_str(form.get("contact_title")),
        "city": _as_str(form.get("city")),
        "state": _as_str(form.get("state")),
        "relationship_type": _as_str(form.get("relationship_type")) or "new",
        "inquiry_source": _as_str(form.get("inquiry_source")) or "cold_inquiry",
        "primary_material": getlist("primary_material") or ["structural_steel"],
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

    # Phase bypass — hidden field set by AI prefill or preserved from edit
    _sp_raw = form.get("skipped_phases")
    _skipped_phases: list[str] = []
    if _sp_raw:
        try:
            _parsed = _json.loads(str(_sp_raw))
            if isinstance(_parsed, list):
                _skipped_phases = [str(x) for x in _parsed if str(x).strip()]
        except Exception:
            pass
    answers["skipped_phases"] = _skipped_phases

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
        return db.count_pending_review()
    except Exception:
        return 0


templates.env.globals["timesheet_period_count"] = _timesheet_period_count


def _pending_invoice_count() -> int:
    try:
        return db.count_pending_invoice_approvals()
    except Exception:
        return 0


templates.env.globals["pending_invoice_count"] = _pending_invoice_count


def _pending_review_count() -> int:
    try:
        return db.count_pending_review()
    except Exception:
        return 0


templates.env.globals["pending_review_count"] = _pending_review_count


def _upcoming_ooo_count() -> int:
    try:
        return db.count_upcoming_ooo(30)
    except Exception:
        return 0


templates.env.globals["upcoming_ooo_count"] = _upcoming_ooo_count


def _burn_nav_badge_count() -> int:
    try:
        return db.count_burn_at_risk(date.today())
    except Exception:
        return 0


templates.env.globals["burn_nav_badge_count"] = _burn_nav_badge_count


app = FastAPI(title="AVS Intake Gate")
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.mount("/static", StaticFiles(directory=str(APP_DIR / "static")), name="static")

# ── Auth ──────────────────────────────────────────────────────────────────────

# Emails are compared case-insensitively (stored lowercase here).
_USER_DIRECTORY: dict[str, dict[str, str]] = {
    "mkateeb@avschwan.com":   {"role": "admin",          "initials": "MK", "name": "Mo Kateeb"},
    "nkline@avschwan.com":    {"role": "admin",          "initials": "NK", "name": "Nathan Kline"},
    "zwoodward@avschwan.com": {"role": "admin",          "initials": "ZW", "name": "Zac Woodward"},
    "rolson@avschwan.com":    {"role": "admin",          "initials": "RO", "name": "Ryan Olson"},
    "rsmith@avschwan.com":    {"role": "employee",       "initials": "RS", "name": "Randall Smith"},
    "swebb@avschwan.com":     {"role": "employee",       "initials": "SW", "name": "Scott Webb"},
    "jprado@avschwan.com":    {"role": "employee",       "initials": "JP", "name": "Jesus Prado"},
    "jwadman@avschwan.com":   {"role": "employee",       "initials": "JW", "name": "Jacob Wadman"},
    "jrobinder@avschwan.com": {"role": "employee",       "initials": "JR", "name": "Josh Robinder"},
    "rkanth@avschwan.com":    {"role": "employee",       "initials": "RK", "name": "Rajul Kanth"},
    "nsongco@avschwan.com":   {"role": "office_manager", "initials": "NS", "name": "Natalie Songco"},
}

_ROLE_REDIRECT: dict[str, str] = {
    "office_manager": "/billing-queue",
    "admin":          "/",
    "employee":       "/engineer-dashboard",
    "engineer":       "/engineer-dashboard",
    "drafter":        "/engineer-dashboard",
}

_is_production = os.environ.get("APP_ENV", "development").lower() == "production"

_session_secret = os.environ.get("SESSION_SECRET_KEY") or ""
if not _session_secret:
    raise RuntimeError(
        "SESSION_SECRET_KEY environment variable is not set. "
        "Generate one with: python3 -c \"import secrets; print(secrets.token_hex(32))\""
    )

app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret,
    session_cookie="avs_session",
    https_only=_is_production,
    max_age=28800,
)

def _session_user(request: Request) -> Optional[dict]:
    return request.session.get("user")


def _require_auth(request: Request) -> Optional[RedirectResponse]:
    if not request.session.get("user"):
        return RedirectResponse("/login", status_code=302)
    return None


_ROLE_HOME = {
    "admin":          "/",
    "office_manager": "/billing-queue",
    "employee":       "/engineer-dashboard",
    "engineer":       "/engineer-dashboard",
    "drafter":        "/engineer-dashboard",
    "billing":        "/billing-queue",
}

# Pages each role may visit (admin implicit everywhere)
_EMPLOYEE_PAGES = {
    "/timesheet", "/calendar", "/time-off", "/past-projects",
    "/my-launch", "/my-time", "/engineer-dashboard",
}
_OFFICE_PAGES   = {"/timesheet", "/calendar", "/time-off",
                   "/billing-queue", "/burn-health", "/capacity",
                   "/approvals"}

_ROLE_ALLOWED: dict[str, set[str]] = {
    "admin":          None,   # None = unrestricted
    "office_manager": _OFFICE_PAGES,
    "billing":        _OFFICE_PAGES,
    "employee":       _EMPLOYEE_PAGES,
    "engineer":       _EMPLOYEE_PAGES,
    "drafter":        _EMPLOYEE_PAGES,
}


def _require_role(request: Request, *allowed: str) -> Optional[RedirectResponse]:
    """Auth + role gate. Returns a redirect if the user lacks the required role."""
    if redir := _require_auth(request):
        return redir
    user = _session_user(request) or {}
    role = user.get("role", "")
    if role not in allowed:
        home = _ROLE_HOME.get(role, "/timesheet")
        return RedirectResponse(home, status_code=302)
    return None


_EMPLOYEE_ROLES = {"employee", "engineer", "drafter"}

def _assigned_engineer_initials(intake: Any) -> set[str]:
    raw = getattr(intake, "assigned_engineers", None)
    if not raw:
        return set()
    try:
        assigned = _json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return set()
    if not isinstance(assigned, list):
        return set()
    return {str(initials).strip().upper() for initials in assigned if str(initials).strip()}


def _user_can_view_intake(user: dict[str, Any], intake: Any) -> bool:
    role = user.get("role", "")
    if role == "admin":
        return True
    if role in _EMPLOYEE_ROLES:
        initials = str(user.get("initials") or "").strip().upper()
        return bool(initials and initials in _assigned_engineer_initials(intake))
    return False


def _enforce_own_entries(request: Request, engineer_initials: str) -> None:
    """Raise 403 if a non-admin user tries to touch another engineer's entries."""
    user = _session_user(request) or {}
    if user.get("role") in _EMPLOYEE_ROLES:
        if engineer_initials.upper() != (user.get("initials") or "").upper():
            raise HTTPException(status_code=403, detail="You can only log time for yourself.")


def _api_require(request: Request, *roles: str) -> dict:
    """Auth + role gate for JSON API routes. Raises HTTPException instead of redirecting."""
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required.")
    if roles and user.get("role") not in roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions.")
    return user


def _check_page_access(request: Request, path: str) -> Optional[RedirectResponse]:
    """Generic per-page guard based on _ROLE_ALLOWED table."""
    if redir := _require_auth(request):
        return redir
    user = _session_user(request) or {}
    role = user.get("role", "")
    allowed = _ROLE_ALLOWED.get(role)
    if allowed is not None and path not in allowed:
        home = _ROLE_HOME.get(role, "/timesheet")
        return RedirectResponse(home, status_code=302)
    return None


@app.on_event("startup")
def _startup() -> None:
    _required = ["SESSION_SECRET_KEY", "AVS_LOGIN_PASSWORD", "SUPABASE_URL", "SUPABASE_SERVICE_KEY"]
    missing = [v for v in _required if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Required environment variables not set: {', '.join(missing)}")
    db.init_db()


# ── Login / Logout ────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request, error: Optional[str] = None) -> HTMLResponse:
    if _session_user(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error":   error,
        "hide_nav_actions": True,
        "title": "Sign In — AVS",
    })


@app.post("/api/auth/login")
@limiter.limit("5/minute")
async def api_login(request: Request) -> JSONResponse:
    body = await request.json()
    email = (body.get("email") or "").strip().lower()
    password = (body.get("password") or "").strip()
    expected_password = os.environ.get("AVS_LOGIN_PASSWORD") or ""
    if not expected_password:
        raise HTTPException(status_code=503, detail="Server misconfiguration.")
    user_info = _USER_DIRECTORY.get(email)
    if not user_info:
        raise HTTPException(status_code=401, detail="Access Denied: Email not registered in the system.")
    if password != expected_password:
        raise HTTPException(status_code=401, detail="Incorrect password.")
    request.session["user"] = {
        "email":    email,
        "role":     user_info["role"],
        "initials": user_info["initials"],
        "name":     user_info["name"],
    }
    return JSONResponse({
        "email":        email,
        "role":         user_info["role"],
        "initials":     user_info["initials"],
        "name":         user_info["name"],
        "redirect_url": _ROLE_REDIRECT.get(user_info["role"], "/"),
    })


@app.get("/api/auth/logout")
def api_logout(request: Request) -> RedirectResponse:
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


@app.get("/", response_class=HTMLResponse)
def launch(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/"): return redir
    user = _session_user(request) or {}
    user_name = user.get("name", "")

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
            "user_name": user_name,
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
    if redir := _check_page_access(request, "/dashboard"): return redir
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
    if redir := _check_page_access(request, "/intakes/new"): return redir
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
    _api_require(request, "admin")
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
def api_delete_template(request: Request, template_id: int) -> dict[str, Any]:
    _api_require(request, "admin")
    db.delete_template(template_id)
    return {"deleted": template_id}


@app.delete("/api/intakes/{intake_id}")
def api_delete_intake(request: Request, intake_id: int) -> dict[str, Any]:
    _api_require(request, "admin")
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    db.delete_intake(intake_id)
    return {"deleted": intake_id}


@app.patch("/api/intakes/{intake_id}/assigned-engineers")
async def api_update_assigned_engineers(request: Request, intake_id: int) -> dict[str, Any]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401)
    body = await request.json()
    engineers = body.get("assigned_engineers")
    if not isinstance(engineers, list):
        raise HTTPException(status_code=400, detail="assigned_engineers must be a list.")
    engineers = [str(e).strip().upper() for e in engineers if str(e).strip()]
    db._client().table("intakes").update({
        "assigned_engineers": _json.dumps(engineers),
        "updated_at":         db._utc_now_iso(),
    }).eq("id", intake_id).execute()
    return {"success": True, "assigned_engineers": engineers}


@app.post("/intakes")
async def intake_create(request: Request) -> RedirectResponse:
    _api_require(request)
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
    if redir := _require_auth(request):
        return redir
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    user = _session_user(request) or {}
    if not _user_can_view_intake(user, intake):
        raise HTTPException(status_code=403, detail="You can only view intakes for projects assigned to you.")

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

    # Days since proposal was sent
    days_since_proposal: Optional[int] = None
    if intake.proposal_sent_date:
        try:
            sent_d = date.fromisoformat(str(intake.proposal_sent_date)[:10])
            days_since_proposal = (today_d - sent_d).days
        except (ValueError, AttributeError):
            pass

    # All accordion sections default to collapsed — user expands what they need
    default_open: set[str] = set()

    return templates.TemplateResponse(
        "intake_view.html",
        {
            "request": request,
            "intake": intake,
            "answers": intake.answers,
            "complexity_estimate": decision["complexity_estimate"],
            "fee_range_estimate": decision["fee_range_estimate"],
            "fast_track": decision["fast_track"],
            "soft_blockers": decision.get("soft_blockers", []),
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
            "db_team_colors":   db.TEAM_COLORS,
            "db_team_members":  db.TEAM_MEMBERS,
            "has_budget":       bool(phase_budgets),
            "budget_fee":       phase_budgets[0]["approved_fee"] if phase_budgets else 0.0,
            "days_since_proposal": days_since_proposal,
            "phase_colors_json": _json.dumps(db.PHASE_COLORS),
        },
    )


@app.get("/intakes/{intake_id}/edit", response_class=HTMLResponse)
def intake_edit(request: Request, intake_id: int) -> HTMLResponse:
    if redir := _check_page_access(request, "/intakes/{intake_id}/edit"): return redir
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
def push_to_mo_queue(request: Request, intake_id: int) -> RedirectResponse:
    _api_require(request)
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    if not intake.mo_decision:
        db.set_status(intake_id, "PENDING_MO_REVIEW")
    return RedirectResponse(url="/mo-queue", status_code=303)


# ── Document upload + AI extraction ──────────────────────────────────────────

@app.get("/intake/upload", response_class=HTMLResponse)
def intake_upload_get(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/intake/upload"): return redir
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

    _ALLOWED_UPLOAD_EXT = {".pdf", ".docx", ".txt", ".eml", ".msg"}
    _MAX_UPLOAD_BYTES   = 10 * 1024 * 1024  # 10 MB

    try:
        paste_clean = (paste_text or "").strip()
        if paste_clean:
            text = paste_clean
            source_label = "pasted text"
        elif file and file.filename:
            # Validate extension before reading bytes
            ext = Path(file.filename).suffix.lower()
            if ext not in _ALLOWED_UPLOAD_EXT:
                raise ValueError(
                    f"Unsupported file type '{ext}'. "
                    f"Please upload a PDF, DOCX, TXT, or EML file."
                )
            raw = await file.read()
            if not raw:
                raise ValueError("Uploaded file is empty.")
            if len(raw) > _MAX_UPLOAD_BYTES:
                raise ValueError("File exceeds the 10 MB size limit.")
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
    request: Request,
    intake_id: int,
    fee_amount: float = Form(...),
    structural_system: Optional[str] = Form(None),
) -> RedirectResponse:
    _api_require(request)
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

    # Healthcare mandatory exclusion (radiation, laser, and medical-gas shielding)
    if (intake.answers.get("building_type") or "").lower() == "healthcare":
        text += (
            "\n\n---\n"
            "SCOPE EXCLUSIONS (Healthcare Project)\n"
            "The following items are explicitly excluded from this proposal unless separately scoped "
            "and authorized in writing:\n"
            "  • Radiation shielding design (requires licensed medical physicist)\n"
            "  • Laser shielding / laser suite structural barriers\n"
            "  • Medical gas piping supports (MEP coordination only; not structural design)\n"
            "  • Seismic anchorage of medical equipment (unless specifically itemized above)\n"
            "These exclusions are standard on all AVS healthcare engagements."
        )

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
    if redir := _check_page_access(request, "/mo-queue"): return redir
    import json as _json

    raw_intakes = db.list_pending_mo()
    enriched: list[dict[str, Any]] = []

    for intake in raw_intakes:
        flags = intake.red_flags
        flag_critical = sum(1 for f in flags if (f.get("severity") or "").lower() == "critical")
        flag_high     = sum(1 for f in flags if (f.get("severity") or "").lower() == "high")
        flag_medium   = sum(1 for f in flags if (f.get("severity") or "").lower() == "medium")

        days_in_queue: Optional[int] = None
        try:
            submitted_dt = datetime.fromisoformat(intake.created_at.replace("Z", "+00:00").split(".")[0])
            days_in_queue = (datetime.now(timezone.utc) - submitted_dt).days
        except Exception:
            pass

        answers = intake.answers
        fee_midpoint: Optional[float] = None
        fee_low: Optional[float] = None
        fee_high: Optional[float] = None
        fee_analysis: Optional[dict] = None
        try:
            dec = compute_decision(answers)
            enriched_ans = {**answers, "_complexity": dec["complexity_estimate"]}
            est = cognasync_estimate_from_answers(intake.project_name, enriched_ans)
            if est:
                if not est.get("needs_manual_review"):
                    fr = est.get("suggested_fee_range") or {}
                    lo = fr.get("low") or 0
                    hi = fr.get("high") or 0
                    if lo and hi:
                        fee_midpoint = round((lo + hi) / 2 / 500) * 500
                        fee_low, fee_high = lo, hi
                fee_analysis = {
                    "needs_review":  est.get("needs_manual_review") or est.get("fee_requires_review"),
                    "review_reason": est.get("review_reason"),
                    "sq_ft":         est.get("sq_ft") or 0,
                    "complexity":    dec["complexity_estimate"],
                    "delivery":      est.get("delivery_bucket"),
                    "building_type": est.get("building_type"),
                    "rate_low":      est.get("effective_rate_low"),
                    "rate_high":     est.get("effective_rate_high"),
                    "base_low":      (est.get("base_fee_range") or {}).get("low"),
                    "base_high":     (est.get("base_fee_range") or {}).get("high"),
                    "cx_mult":       est.get("complexity_multiplier"),
                    "cx_low":        (est.get("complexity_adjusted_range") or {}).get("low"),
                    "cx_high":       (est.get("complexity_adjusted_range") or {}).get("high"),
                    "risk_mult":     est.get("risk_multiplier"),
                    "flag_count":    est.get("flag_count") or 0,
                    "floor":         est.get("floor_fee"),
                    "floor_applied": est.get("floor_applied"),
                    "range_low":     fee_low,
                    "range_high":    fee_high,
                    "midpoint":      fee_midpoint,
                }
        except Exception:
            pass

        enriched.append({
            "id":                   intake.id,
            "project_name":         intake.project_name,
            "client_name":          intake.client_name,
            "architect_name":       intake.architect_name,
            "location_region":      intake.location_region,
            "recommendation":       intake.recommendation,
            "recommendation_reason": intake.recommendation_reason,
            "red_flags":            flags,
            "flag_critical":        flag_critical,
            "flag_high":            flag_high,
            "flag_medium":          flag_medium,
            "project_number":       intake.project_number,
            "inquiry_date":         intake.inquiry_date,
            "ifp_due_date":         intake.ifp_due_date,
            "proposed_start_date":  intake.proposed_start_date,
            "proposed_end_date":    intake.proposed_end_date,
            "created_at":           intake.created_at,
            "days_in_queue":        days_in_queue,
            "project_type":         answers.get("project_type", ""),
            "answers":              answers,
            "fee_midpoint":         fee_midpoint,
            "fee_low":              fee_low,
            "fee_high":             fee_high,
            "fee_analysis":         fee_analysis,
        })

    pending_invoices = db.get_pending_invoice_approvals()
    try:
        timesheet_queue = db.get_enriched_review_queue()
    except Exception:
        timesheet_queue = []
    return templates.TemplateResponse(
        "mo_queue.html",
        {
            "request":             request,
            "intakes":             enriched,
            "has_items":           len(enriched) > 0,
            "pending_invoices":    pending_invoices,
            "timesheet_queue":     timesheet_queue,
            "now_local":           _now_local_iso(),
            "valid_phases":        db.VALID_PHASES,
            "team_colors_json":    _json.dumps(db.TEAM_COLORS),
            "phase_colors_json":   _json.dumps(db.PHASE_COLORS),
            "billing_labels_json": _json.dumps(db.BILLING_PHASE_LABELS),
            "billing_labels":      db.BILLING_PHASE_LABELS,
        },
    )


@app.post("/api/intakes/{intake_id}/mo-review")
async def api_mo_review_json(request: Request, intake_id: int) -> dict:
    _api_require(request, "admin")
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
        # New decision values from the overhauled Mo Queue
        "NEEDS_MORE_INFO":         "NEEDS_INFO",
        "CONDITIONS":              "PROCEED_WITH_CONDITIONS",
        "DECLINED":                "DECLINED",
    }.get(mo_decision)
    if not status:
        raise HTTPException(status_code=400, detail="Invalid decision.")
    mo_fee_decision = (body.get("mo_fee_decision") or "").strip().upper() or None
    fee_override = _as_str(str(body.get("mo_fee_override") or "")) if mo_fee_decision == "OVERRIDE" else None
    mo_notes_val = _as_str(str(body.get("mo_notes") or body.get("mo_decision_notes") or ""))

    db.set_mo_review(
        intake_id,
        mo_decision=mo_decision,
        mo_notes=mo_notes_val,
        mo_conditions=_as_str(str(body.get("mo_conditions") or "")),
        mo_fee_decision=mo_fee_decision,
        mo_fee_override=fee_override,
        status=status,
        mo_decision_notes=_as_str(str(body.get("mo_decision_notes") or "")),
    )

    # Engineer assignment and resource commitment are deferred until "Mark as Won".
    return {"success": True, "status": status, "intake_id": intake_id, "project_number": intake.project_number}


@app.post("/api/intakes/{intake_id}/generate-proposal")
async def api_generate_proposal_json(request: Request, intake_id: int) -> dict:
    _api_require(request)
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    # Cooldown: prevent repeated AI generation within 60 seconds
    if intake.proposal_generated_at:
        try:
            _last_gen = datetime.fromisoformat(intake.proposal_generated_at.replace("Z", "+00:00"))
            _now_utc = datetime.now(_last_gen.tzinfo)
            if (_now_utc - _last_gen).total_seconds() < 60:
                _wait = int(60 - (_now_utc - _last_gen).total_seconds())
                raise HTTPException(
                    status_code=429,
                    detail=f"Proposal was just generated. Please wait {_wait}s before regenerating.",
                )
        except HTTPException:
            raise
        except Exception:
            pass  # If timestamp parsing fails, allow generation
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

    # Healthcare mandatory exclusion (radiation, laser, and medical-gas shielding)
    if (intake.answers.get("building_type") or "").lower() == "healthcare":
        text += (
            "\n\n---\n"
            "SCOPE EXCLUSIONS (Healthcare Project)\n"
            "The following items are explicitly excluded from this proposal unless separately scoped "
            "and authorized in writing:\n"
            "  • Radiation shielding design (requires licensed medical physicist)\n"
            "  • Laser shielding / laser suite structural barriers\n"
            "  • Medical gas piping supports (MEP coordination only; not structural design)\n"
            "  • Seismic anchorage of medical equipment (unless specifically itemized above)\n"
            "These exclusions are standard on all AVS healthcare engagements."
        )

    db.save_proposal(intake_id, text)
    return {"proposal_text": text, "intake_id": intake_id}


@app.get("/intakes/{intake_id}/mo-review", response_class=HTMLResponse)
def mo_review_get(request: Request, intake_id: int) -> HTMLResponse:
    return RedirectResponse(url="/mo-queue", status_code=303)


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

    return RedirectResponse(url="/mo-queue", status_code=303)


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


@app.post("/api/intakes/{intake_id}/mark-proposal-sent")
async def api_mark_proposal_sent(request: Request, intake_id: int) -> dict[str, Any]:
    _api_require(request, "admin")
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    db.mark_proposal_sent(intake_id)
    return {"ok": True, "status": "PROPOSAL_OUT", "intake_id": intake_id}


@app.post("/api/intakes/{intake_id}/mark-won")
async def api_mark_project_won(request: Request, intake_id: int) -> dict[str, Any]:
    _api_require(request, "admin")
    body = await request.json()
    win_prob = int(body.get("win_probability") or 100)
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    # Engineers, dates, and Wave 1 phase config may come from the Won modal
    body_engineers     = body.get("assigned_engineers")
    body_start         = body.get("proposed_start_date") or None
    body_end           = body.get("proposed_end_date") or None
    body_sel_phases    = body.get("selected_phases") or None      # list[str] | None
    body_phase_dates   = body.get("phase_due_dates") or None      # dict[str, str] | None
    body_cad_revit     = body.get("cad_or_revit") or None
    body_overview      = body.get("project_overview") or None

    # Persist engineers + dates + Wave 1 fields back to intake if supplied
    update_payload: dict = {}
    if body_engineers is not None:
        update_payload["assigned_engineers"] = _json.dumps(body_engineers)
    if body_start:
        update_payload["proposed_start_date"] = body_start
    if body_end:
        update_payload["proposed_end_date"] = body_end
    if body_sel_phases is not None:
        update_payload["selected_phases"] = body_sel_phases   # JSONB — pass list directly
    if body_phase_dates is not None:
        update_payload["phase_due_dates"] = body_phase_dates  # JSONB — pass dict directly
    if body_cad_revit:
        update_payload["cad_or_revit"] = body_cad_revit
    if body_overview:
        update_payload["project_overview"] = body_overview
    if update_payload:
        update_payload["updated_at"] = db._utc_now_iso()
        db._client().table("intakes").update(update_payload).eq("id", intake_id).execute()
        intake = db.get_intake(intake_id)  # re-fetch with updated values

    # ── Commitment Lock: this is the single point where resources are allocated ──
    # 1. Assign project number (if not already set)
    project_number = intake.project_number
    if not project_number:
        project_number = db.assign_next_project_number()
        db.set_intake_project_number(intake_id, project_number)

    # 2. Commit phase budgets and billing phases (also sets pipeline_active=1)
    approved_fee: Optional[float] = None
    if intake.mo_fee_override:
        try:
            approved_fee = float(intake.mo_fee_override)
        except (ValueError, TypeError):
            pass
    _win_warnings: list[str] = []
    if approved_fee and approved_fee > 0:
        try:
            _sel_for_budget = intake.selected_phases_list or None
            db.generate_phase_budgets(intake_id, project_number, approved_fee,
                                      selected_phases=_sel_for_budget)
        except Exception as _e:
            _win_warnings.append(f"Phase budget generation failed: {_e}")
            print(f"[mark_project_won] phase_budgets error for intake {intake_id}: {_e}")
        try:
            db.create_billing_phases_for_project(intake_id, approved_fee)
        except Exception as _e:
            _win_warnings.append(f"Billing phase creation failed: {_e}")
            print(f"[mark_project_won] billing_phases error for intake {intake_id}: {_e}")

    # 3. Write calendar events (WEU load committed to engineers)
    _start = intake.proposed_start_date
    _ifp   = intake.ifp_due_date
    _team  = _json.loads(intake.assigned_engineers or "[]")
    if _start and _ifp:
        _fresh = db.get_intake(intake_id)
        _tier  = db.infer_tier_from_intake(_fresh) if _fresh else 3
        _sel   = _fresh.selected_phases_list if _fresh else None
        _dates = _fresh.phase_due_dates_dict if _fresh else None
        try:
            db.generate_phase_calendar_events(
                intake_id=intake_id,
                project_number=project_number,
                start_date=_start,
                ifp_date=_ifp,
                team=_team,
                weu_hours=40.0,
                replace_existing=True,
                tier=_tier,
                selected_phases=_sel or None,
                phase_due_dates=_dates or None,
            )
        except Exception as _e:
            _win_warnings.append(f"Calendar event generation failed: {_e}")
            print(f"[mark_project_won] calendar_events error for intake {intake_id}: {_e}")

    # 4. Mark as ACTIVE_PROJECT and record win probability
    db.mark_project_won(intake_id, win_probability=win_prob)

    # 5. Auto-save to searchable historical database
    try:
        import datetime as _dt
        answers: dict = {}
        try:
            answers = _json.loads(intake.answers_json or "{}")
        except Exception:
            pass
        hist_record = {
            "project_name":    intake.project_name or "",
            "project_number":  project_number or "",
            "location":        intake.location_region or "",
            "year_completed":  _dt.datetime.utcnow().year,
            "project_type":    answers.get("project_type") or getattr(intake, "project_type", None) or "",
            "material":        answers.get("primary_structural_material") or "",
            "client":          intake.client_name or "",
            "raw_description": answers.get("scope_description") or intake.project_name or "",
        }
        db._client().table("historical_projects").insert(hist_record).execute()
        project_search.invalidate_cache()
    except Exception as _e:
        _win_warnings.append(f"Historical record save failed: {_e}")
        print(f"[mark_project_won] historical_projects error for intake {intake_id}: {_e}")

    response: dict[str, Any] = {
        "ok": True,
        "status": "ACTIVE_PROJECT",
        "intake_id": intake_id,
        "project_number": project_number,
    }
    if _win_warnings:
        response["warnings"] = _win_warnings
    return response


@app.patch("/api/intakes/{intake_id}/project-details")
async def api_update_project_details(request: Request, intake_id: int) -> dict[str, Any]:
    """
    Wave 1: save project detail fields (overview, CAD/Revit, phase selection,
    per-phase due dates) without touching the rest of the intake record.
    Available to admin and office_manager roles.
    """
    _api_require(request, "admin", "office_manager")
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    body = await request.json()

    cad_or_revit    = body.get("cad_or_revit")           # "CAD" | "Revit" | None
    project_overview= body.get("project_overview")        # str | None
    selected_phases = body.get("selected_phases")         # list[str] | None
    phase_due_dates = body.get("phase_due_dates")         # dict[str,str] | None

    db.update_project_details(
        intake_id,
        cad_or_revit=cad_or_revit,
        project_overview=project_overview,
        selected_phases=selected_phases,
        phase_due_dates=phase_due_dates,
    )

    # If the project is already ACTIVE, regenerate calendar and phase budgets
    # with the new phase config so both stay in sync.
    if intake.status == "ACTIVE_PROJECT":
        fresh = db.get_intake(intake_id)
        if fresh:
            _sel   = fresh.selected_phases_list or None
            _dates = fresh.phase_due_dates_dict or None

            # Regenerate calendar if schedule info is present
            if fresh.proposed_start_date and fresh.ifp_due_date:
                _team  = _json.loads(fresh.assigned_engineers or "[]")
                _tier  = db.infer_tier_from_intake(fresh)
                try:
                    db.generate_phase_calendar_events(
                        intake_id=intake_id,
                        project_number=fresh.project_number or "",
                        start_date=fresh.proposed_start_date,
                        ifp_date=fresh.ifp_due_date,
                        team=_team,
                        weu_hours=40.0,
                        replace_existing=True,
                        tier=_tier,
                        selected_phases=_sel,
                        phase_due_dates=_dates,
                    )
                except Exception:
                    pass

            # Regenerate phase budgets if phases changed and an approved fee exists
            if selected_phases is not None and fresh.project_number:
                existing_budgets = db.list_phase_budgets(intake_id)
                _fee = existing_budgets[0]["approved_fee"] if existing_budgets else None
                if _fee and _fee > 0:
                    try:
                        db.generate_phase_budgets(
                            intake_id, fresh.project_number, _fee,
                            selected_phases=_sel,
                        )
                    except Exception:
                        pass

    return {"ok": True, "intake_id": intake_id}


@app.post("/api/intakes/{intake_id}/mark-lost")
async def api_mark_project_lost(request: Request, intake_id: int) -> dict[str, Any]:
    _api_require(request, "admin")
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    if intake.status != "PROPOSAL_OUT":
        raise HTTPException(status_code=400, detail="Only proposals in PROPOSAL_OUT status can be marked lost.")
    db._client().table("intakes").delete().eq("id", intake_id).execute()
    return {"ok": True, "deleted": intake_id}


@app.get("/api/active-bids")
async def api_active_bids(request: Request) -> list[dict[str, Any]]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    try:
        return db.get_active_bids()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/intakes/{intake_id}/draft-follow-up")
async def api_draft_follow_up(request: Request, intake_id: int) -> dict[str, Any]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=503, detail="ANTHROPIC_API_KEY not configured.")

    import anthropic as _anthropic
    answers = intake.answers
    project_type = answers.get("project_type") or "project"
    sf = answers.get("approx_sf") or ""
    sf_note = f", approximately {sf} SF" if sf else ""
    location = intake.location_region or ""

    prompt = (
        f"Write a short, professional follow-up email from A.V. Schwan & Associates (AVS) "
        f"to a client who has not responded to a structural engineering proposal.\n\n"
        f"Project: {intake.project_name or 'the project'}\n"
        f"Client: {intake.client_name or 'the client'}\n"
        f"Location: {location}\n"
        f"Scope: {project_type}{sf_note}\n"
        f"Contact: {intake.lead_contact or 'the project team'}\n\n"
        f"The email should:\n"
        f"- Be 3–4 sentences, warm but professional\n"
        f"- Reference the project name and our proposal\n"
        f"- Ask if they have questions or need any revisions\n"
        f"- Close with an offer to schedule a quick call\n"
        f"- Use plain text only (no markdown)\n"
        f"- Sign off as 'The AVS Team'\n\n"
        f"Output only the email body — no subject line, no extra commentary."
    )

    _ac = _anthropic.Anthropic(api_key=api_key)
    msg = _ac.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    email_text = msg.content[0].text.strip()
    new_count = db.increment_follow_up(intake_id)
    return {"email_text": email_text, "follow_up_count": new_count}


@app.get("/reports", response_class=HTMLResponse)
def reports(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/reports"): return redir
    import json as _json
    all_intakes = db.list_intakes()
    now = datetime.now()

    # ── Helper: get fee estimate for an intake ───────────────────────────────
    def _get_fee(r) -> Optional[float]:
        if r.mo_fee_decision == "OVERRIDE" and r.mo_fee_override:
            try:
                return float(r.mo_fee_override)
            except (ValueError, TypeError):
                pass
        est = cognasync_estimate_from_answers(r.project_name, r.answers)
        fee_range = (est or {}).get("suggested_fee_range")
        if fee_range:
            low  = fee_range.get("low") or 0
            high = fee_range.get("high") or 0
            if low and high:
                return (low + high) / 2
        return None

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

    # ── New KPIs ─────────────────────────────────────────────────────────────
    _raw_status = Counter(r.status for r in all_intakes)
    won_count          = _raw_status.get("ACTIVE_PROJECT", 0)
    proposals_out      = _raw_status.get("PROPOSAL_OUT", 0)
    pending_review     = _raw_status.get("PENDING_MO_REVIEW", 0)

    # Win rate = won / (won + declined + proposal_out that have been decided)
    proposals_sent     = sum(1 for r in all_intakes if r.proposal_sent_date)
    win_rate           = f"{round(won_count / proposals_sent * 100)}%" if proposals_sent else "—"

    # Active project fees
    active_fee_total   = sum(_get_fee(r) or 0 for r in all_intakes if r.status == "ACTIVE_PROJECT")
    active_fee_fmt     = f"${active_fee_total:,.0f}" if active_fee_total else "—"

    # Pipeline funnel stages (sequential conversion)
    funnel_stages  = ["Intakes", "Mo Reviewed", "Approved", "Proposal Sent", "Won"]
    funnel_values  = [
        total_intakes,
        sum(1 for r in all_intakes if r.mo_reviewed_at),
        sum(1 for r in all_intakes if r.status in {"PROCEED_TO_PROPOSAL", "PROCEED_WITH_CONDITIONS",
                                                    "PROPOSAL_OUT", "ACTIVE_PROJECT"}),
        proposals_sent,
        won_count,
    ]

    # ── Chart data ──────────────────────────────────────────────────────────
    # Status donut
    _STATUS_LABELS = {
        "ACTIVE_PROJECT":           "Active",
        "PROPOSAL_OUT":             "Proposal Out",
        "PROCEED_TO_PROPOSAL":      "Approved",
        "PROCEED_WITH_CONDITIONS":  "With Conditions",
        "PENDING_MO_REVIEW":        "Decision Queue",
        "NEEDS_INFO":               "Needs Info",
        "DECLINED":                 "Declined",
    }
    status_chart_labels = list(_STATUS_LABELS.values())
    status_chart_values = [_raw_status.get(k, 0) for k in _STATUS_LABELS]

    # Monthly intake count trend
    _monthly_raw: dict[str, int] = defaultdict(int)
    _monthly_fee_raw: dict[str, float] = defaultdict(float)
    for _r in all_intakes:
        _src = (_r.inquiry_date or _r.created_at or "")[:7]
        if len(_src) == 7:
            _monthly_raw[_src] += 1
    # Monthly won fee (by proposal_sent_date month for won projects)
    for _r in all_intakes:
        if _r.status == "ACTIVE_PROJECT":
            _src = (_r.proposal_sent_date or _r.created_at or "")[:7]
            if len(_src) == 7:
                _fee = _get_fee(_r)
                if _fee:
                    _monthly_fee_raw[_src] += _fee
    _sorted_months  = sorted(set(list(_monthly_raw) + list(_monthly_fee_raw)))
    monthly_labels  = _sorted_months
    monthly_values  = [_monthly_raw.get(m, 0) for m in _sorted_months]
    monthly_fee_vals= [round(_monthly_fee_raw.get(m, 0)) for m in _sorted_months]

    # Red flag frequency (top 10, longest-title truncated to 32 chars)
    rf_labels   = [r["title"][:32] for r in red_flag_table[:10]]
    rf_values   = [r["count"]      for r in red_flag_table[:10]]
    rf_sevs     = [r["top_severity"] for r in red_flag_table[:10]]

    # Project type stacked bar (win/pending/decline)
    pt_labels  = [r["type"]    for r in project_type_table]
    pt_proceed = [r["proceed"] for r in project_type_table]
    pt_decline = [r["decline"] for r in project_type_table]
    pt_pending = [r["total"] - r["proceed"] - r["decline"] for r in project_type_table]

    # Win rate by project type bar
    pt_winrate = []
    for r in project_type_table:
        decided = r["proceed"] + r["decline"]
        pt_winrate.append(round(r["proceed"] / decided * 100) if decided else 0)

    # ── Active projects table ──────────────────────────────────────────────
    active_rows: list[dict[str, Any]] = []
    for _r in all_intakes:
        if _r.status != "ACTIVE_PROJECT":
            continue
        fee = _get_fee(_r)
        active_rows.append({
            "id":           _r.id,
            "project_name": _r.project_name,
            "project_number": _r.project_number or "—",
            "client_name":  _r.client_name or "—",
            "fee_amount":   fee,
            "ifp_due_date": _r.ifp_due_date or "—",
        })
    active_rows.sort(key=lambda x: x["ifp_due_date"])

    # ── Proposals out table ────────────────────────────────────────────────
    proposal_rows: list[dict[str, Any]] = []
    for _r in all_intakes:
        if _r.status != "PROPOSAL_OUT":
            continue
        fee = _get_fee(_r)
        sent_date = _r.proposal_sent_date or ""
        days_out: Optional[int] = None
        if sent_date:
            try:
                days_out = (now - datetime.strptime(sent_date[:10], "%Y-%m-%d")).days
            except Exception:
                pass
        proposal_rows.append({
            "id":              _r.id,
            "project_name":    _r.project_name,
            "client_name":     _r.client_name or "—",
            "fee_amount":      fee,
            "days_out":        days_out,
            "win_probability": _r.win_probability,
        })
    proposal_rows.sort(key=lambda x: x["days_out"] or 0, reverse=True)

    # ── Approved fee pipeline (not yet won) ───────────────────────────────
    _approved_statuses = {"PROCEED_TO_PROPOSAL", "PROCEED_WITH_CONDITIONS"}
    approved_rows: list[dict[str, Any]] = []
    approved_fee_total = 0.0
    for _r in all_intakes:
        if _r.status not in _approved_statuses:
            continue
        fee_amount = _get_fee(_r)
        fee_source = "Mo override" if _r.mo_fee_decision == "OVERRIDE" else "Estimate midpoint"
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

    return templates.TemplateResponse(
        "reports.html",
        {
            "request":    request,
            "now_local":  _now_local_iso(),
            # ── KPI row 1: volume ──────────────────────────────────────────
            "total_intakes":       total_intakes,
            "this_month_count":    this_month_count,
            "won_count":           won_count,
            "proposals_out":       proposals_out,
            "pending_review":      pending_review,
            # ── KPI row 2: performance ─────────────────────────────────────
            "win_rate":            win_rate,
            "conversion_rate":     conversion_rate,
            "decline_rate":        decline_rate,
            "avg_days":            avg_days,
            "avg_mo_to_proposal":  avg_mo_to_proposal,
            "active_fee_fmt":      active_fee_fmt,
            "approved_fee_total":  approved_fee_total_fmt,
            # ── Tables ────────────────────────────────────────────────────
            "active_rows":         active_rows,
            "proposal_rows":       proposal_rows,
            "approved_rows":       approved_rows,
            "red_flag_table":      red_flag_table,
            "project_type_table":  project_type_table,
            "architect_table":     architect_table,
            # ── Chart JSON ────────────────────────────────────────────────
            "j_funnel_stages":    _json.dumps(funnel_stages),
            "j_funnel_values":    _json.dumps(funnel_values),
            "j_status_labels":    _json.dumps(status_chart_labels),
            "j_status_values":    _json.dumps(status_chart_values),
            "j_monthly_labels":   _json.dumps(monthly_labels),
            "j_monthly_values":   _json.dumps(monthly_values),
            "j_monthly_fee":      _json.dumps(monthly_fee_vals),
            "j_rf_labels":        _json.dumps(rf_labels),
            "j_rf_values":        _json.dumps(rf_values),
            "j_rf_sevs":          _json.dumps(rf_sevs),
            "j_pt_labels":        _json.dumps(pt_labels),
            "j_pt_proceed":       _json.dumps(pt_proceed),
            "j_pt_decline":       _json.dumps(pt_decline),
            "j_pt_pending":       _json.dumps(pt_pending),
            "j_pt_winrate":       _json.dumps(pt_winrate),
        },
    )


@app.get("/past-projects", response_class=HTMLResponse)
def past_projects(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/past-projects"): return redir
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
            "total": total,
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


@app.post("/api/nl-search-projects")
async def api_nl_search_projects(request: Request) -> dict[str, Any]:
    _api_require(request)
    body = await request.json()
    query = (body.get("query") or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required.")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY is not configured.")
    import anthropic as _anthropic
    client = _anthropic.Anthropic(api_key=api_key)
    prompt = (
        "Extract search keywords from this structural engineering project search query.\n"
        "Return ONLY a JSON array of strings — individual keywords or short phrases.\n"
        "Include variations, abbreviations, and synonyms. "
        "IMPORTANT — AVS internal project-type vocabulary: "
        "BTS / Build-to-Suit = new construction, new build, ground-up, shell building, "
        "new_construction, build_to_suit_retrofit, build to suit, built to suit. "
        "TI / Tenant Improvement = tenant improvement, interior build-out, tenant_improvement. "
        "Tilt-up = tilt, tilt panel, tilt up, tilt-up, concrete tilt. "
        "Wood frame = wood, timber, light frame. "
        "Examples: "
        "for 'BTS' include ['bts', 'build to suit', 'new construction', 'new_construction', "
        "'build_to_suit_retrofit', 'ground up', 'new build', 'shell']; "
        "for 'new build' include ['new build', 'new construction', 'bts', 'build to suit', "
        "'ground up', 'new_construction', 'shell']; "
        "for 'TI' include ['ti', 'tenant improvement', 'tenant_improvement', 'interior', "
        "'build-out', 'fit-out']; "
        "for 'tilt-up' include ['tilt', 'tilt panel', 'tilt up', 'tilt-up', 'concrete tilt'].\n\n"
        f"Query: {query}"
    )
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        keywords = _json.loads(raw.strip())
        if not isinstance(keywords, list):
            keywords = [str(keywords)]
    except _json.JSONDecodeError:
        keywords = [query]

    try:
        data = project_search.get_projects()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    normalized_kw = [kw.lower().strip() for kw in keywords if str(kw).strip()]
    scored: list[tuple[int, dict]] = []
    for row in data["rows"]:
        haystack = " ".join(str(v).lower() for v in row.values() if v)
        score = sum(1 for kw in normalized_kw if kw in haystack)
        if score > 0:
            scored.append((score, row))

    scored.sort(key=lambda x: x[0], reverse=True)
    matches = [row for _, row in scored[:500]]

    # ── Annotate rows with intake_id when project_number matches an intake ──
    id_col = data["col_map"].get("id")   # e.g. "Project Number" column name
    intake_lookup: dict[str, int] = {}   # project_number (upper) -> intake id
    if id_col:
        candidate_pnums = {
            str(row.get(id_col) or "").strip().upper()
            for row in matches
            if str(row.get(id_col) or "").strip()
        }
        if candidate_pnums:
            try:
                resp = (
                    db._client()
                    .table("intakes")
                    .select("id,project_number")
                    .in_("project_number", list(candidate_pnums))
                    .execute()
                )
                for rec in (resp.data or []):
                    pn = str(rec.get("project_number") or "").strip().upper()
                    if pn:
                        intake_lookup[pn] = rec["id"]
            except Exception:
                pass

    annotated = []
    for row in matches:
        pn = str(row.get(id_col) or "").strip().upper() if id_col else ""
        annotated.append({**row, "_intake_id": intake_lookup.get(pn)})

    return {
        "ok": True,
        "headers": data["headers"],
        "col_map": data["col_map"],
        "type_options": data["type_options"],
        "total": data["total"],
        "returned": len(annotated),
        "truncated": len(annotated) >= 500,
        "rows": annotated,
    }


@app.post("/api/analyze-project")
async def api_analyze_project(request: Request) -> dict[str, Any]:
    _api_require(request)
    body = await request.json()
    description = (body.get("description") or "").strip()
    if not description:
        raise HTTPException(status_code=400, detail="description is required.")
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY is not configured.")
    import anthropic as _anthropic
    client = _anthropic.Anthropic(api_key=api_key)
    prompt = (
        "Extract technical details from the following project description.\n"
        "Return ONLY a JSON object with these exact keys (use null if unknown):\n"
        "  project_name, project_number, location, year_completed, project_type,\n"
        "  material, roof, lfrs, slab, foundation, client, ahj, site_visit, notes\n\n"
        "For site_visit: return true if a site visit is mentioned, false if explicitly not done, null if unknown.\n"
        "For slab: e.g. 'Structural Slab', 'Elevated Slab', 'Slab on Grade'.\n"
        "For foundation: e.g. 'Spread Footing', 'Piers'.\n"
        "For client: the company or tenant the project was built for.\n\n"
        f"Description:\n{description}"
    )
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    try:
        return _json.loads(raw.strip())
    except _json.JSONDecodeError:
        raise HTTPException(status_code=500, detail=f"Claude returned non-JSON: {raw}")


@app.post("/api/historical-projects")
async def api_save_historical_project(request: Request) -> dict[str, Any]:
    _api_require(request)
    body = await request.json()
    try:
        year = int(body["year_completed"]) if body.get("year_completed") else None
    except (ValueError, TypeError):
        year = None
    site_visit_raw = body.get("site_visit")
    if isinstance(site_visit_raw, bool):
        site_visit = site_visit_raw
    elif isinstance(site_visit_raw, str):
        site_visit = site_visit_raw.lower() in ("true", "yes", "1") if site_visit_raw else None
    else:
        site_visit = None
    record = {
        "project_name":    (body.get("project_name") or "").strip() or None,
        "project_number":  (body.get("project_number") or "").strip() or None,
        "location":        (body.get("location") or "").strip() or None,
        "year_completed":  year,
        "project_type":    (body.get("project_type") or "").strip() or None,
        "material":        (body.get("material") or "").strip() or None,
        "roof":            (body.get("roof") or "").strip() or None,
        "lfrs":            (body.get("lfrs") or "").strip() or None,
        "slab":            (body.get("slab") or "").strip() or None,
        "foundation":      (body.get("foundation") or "").strip() or None,
        "client":          (body.get("client") or "").strip() or None,
        "ahj":             (body.get("ahj") or "").strip() or None,
        "site_visit":      site_visit,
        "notes":           (body.get("notes") or "").strip() or None,
        "raw_description": (body.get("raw_description") or "").strip() or None,
    }
    try:
        resp = db._client().table("historical_projects").insert(record).execute()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if resp.data:
        return resp.data[0]
    raise HTTPException(status_code=500, detail="Insert returned no data. Run migrate_003_historical_projects.sql first.")


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
    if redir := _check_page_access(request, "/calendar"): return redir
    import json as _json
    return templates.TemplateResponse(
        "calendar.html",
        {
            "request":       request,
            "now_local":     _now_local_iso(),
            "phase_colors":  _json.dumps(db.PHASE_COLORS),
            "valid_phases":  db.VALID_PHASES,
            "team_colors":   _json.dumps(db.TEAM_COLORS),
            "team_members":  db.TEAM_MEMBERS,
            "db_team_colors": db.TEAM_COLORS,   # dict for Jinja inline use
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
    _api_require(request)
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
    _api_require(request)
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
def api_calendar_events_delete(request: Request, event_id: str) -> dict:
    _api_require(request)
    existing = db.get_calendar_event(event_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Not found.")
    db.delete_calendar_event(event_id)
    return {"deleted": event_id}


@app.get("/api/calendar/phase-events")
def api_phase_events(year: Optional[int] = None, month: Optional[int] = None) -> dict[str, Any]:
    today = date.today()
    y = year  or today.year
    m = month or today.month
    events = db.list_phase_calendar_events(y, m)
    # Attach project_name, cad_or_revit, project_overview from intakes
    intake_ids = list({e["intake_id"] for e in events if e.get("intake_id")})
    intake_meta: dict[int, dict] = {}
    if intake_ids:
        resp = (
            db._client()
            .table("intakes")
            .select("id,project_name,project_number,cad_or_revit,project_overview")
            .in_("id", intake_ids)
            .execute()
        )
        for row in (resp.data or []):
            intake_meta[row["id"]] = row
    for e in events:
        iid = e.get("intake_id")
        meta = intake_meta.get(iid) if iid else None
        e["project_name"]    = (meta or {}).get("project_name") or e["project_number"]
        e["cad_or_revit"]    = (meta or {}).get("cad_or_revit") or ""
        e["project_overview"]= (meta or {}).get("project_overview") or ""
    return {"events": events, "year": y, "month": m}


@app.post("/api/intakes/{intake_id}/regenerate-calendar")
def api_regenerate_calendar(intake_id: int) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")
    if not (intake.project_number and intake.proposed_start_date and intake.ifp_due_date):
        raise HTTPException(
            status_code=400,
            detail="Project must have project_number, proposed_start_date, and ifp_due_date.",
        )
    import json as _json
    team: list[str] = []
    try:
        team = _json.loads(intake.assigned_engineers or "[]")
    except Exception:
        pass
    try:
        events = db.generate_phase_calendar_events(
            intake_id=intake_id,
            project_number=intake.project_number,
            start_date=intake.proposed_start_date,
            ifp_date=intake.ifp_due_date,
            team=team,
            weu_hours=40.0,
            replace_existing=True,
            selected_phases=intake.selected_phases_list or None,
            phase_due_dates=intake.phase_due_dates_dict or None,
        )
        return {"ok": True, "phases_generated": len(events)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


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
    try:
        return db.get_all_projected_capacity(ws, we)
    except Exception:
        return {
            "window_start": ws.isoformat(),
            "window_end": we.isoformat(),
            "window_days": db.count_working_days(ws, we),
            "engineering_pool": [{"engineer_initials": m, "utilization_pct": 0.0, "has_ooo": False, "ooo_days": 0, "ooo_bar_pct": 0.0, "working_days": 0, "available_days": 0, "available_hours": 0.0, "committed_hours": 0.0} for m in db.ENGINEERING_POOL],
            "drafting_pool":    [{"engineer_initials": m, "utilization_pct": 0.0, "has_ooo": False, "ooo_days": 0, "ooo_bar_pct": 0.0, "working_days": 0, "available_days": 0, "available_hours": 0.0, "committed_hours": 0.0} for m in db.DRAFTING_POOL],
        }


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
    return RedirectResponse(url="/my-time#timeoff", status_code=302)


@app.get("/my-time", response_class=HTMLResponse)
def my_time_page(
    request: Request,
    engineer: Optional[str] = None,
) -> HTMLResponse:
    if redir := _check_page_access(request, "/my-time"): return redir
    user     = _session_user(request) or {}
    is_admin = user.get("role") == "admin"

    # My Hours data
    if engineer and is_admin:
        target = engineer.upper()
    else:
        target = (user.get("initials") or "NK").upper()
    view = db.get_engineer_bucket_view(target)
    team_members = {k: v for k, v in db.TEAM_FULL_NAMES.items() if k in _WEU_ROLE_BUCKETS}

    # Timesheet data
    import json as _json
    start, end = _current_pay_period()
    current_user_role     = user.get("role", "")
    current_user_initials = user.get("initials", "")

    # Pre-fetch submission status for the current period (employee-only, avoids JS round-trip)
    current_submission: Optional[dict] = None
    if current_user_initials and current_user_role not in ("admin", "office_manager", "billing"):
        try:
            current_submission = db.get_submission(current_user_initials, start)
        except Exception:
            current_submission = None

    return templates.TemplateResponse(
        "my_time.html",
        {
            "request":               request,
            "now_local":             _now_local_iso(),
            # My Hours
            "view":                  view,
            "is_admin":              is_admin,
            "team_members":          team_members,
            # Timesheet
            "team_members_list":     db.TEAM_MEMBERS,
            "team_full_names_json":  _json.dumps(db.TEAM_FULL_NAMES),
            "phase_colors_json":     _json.dumps(db.PHASE_COLORS),
            "valid_phases":          db.VALID_PHASES,
            "default_period_start":  start,
            "default_period_end":    end,
            "current_user_role":     current_user_role,
            "current_user_initials": current_user_initials,
            "current_submission":    current_submission,
            # Time Off
            "reasons":               db.TIME_OFF_REASONS,
            "team_colors_json":      _json.dumps(db.TEAM_COLORS),
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
    # Confidence scoring based on flag count and data completeness
    flag_count   = decision["counts"]["total"]
    has_sf       = bool(intake.answers.get("approx_sf"))
    if flag_count == 0 and has_sf:
        confidence       = "High"
        confidence_notes = "No red flags; complete square footage data."
    elif flag_count <= 2 and has_sf:
        confidence       = "Medium"
        confidence_notes = f"{flag_count} flag(s) detected; estimate may shift after Mo review."
    else:
        confidence       = "Low"
        confidence_notes = (
            f"{flag_count} flag(s)" if flag_count else "Missing square footage"
        ) + "; manual review recommended before using this estimate."

    calc_log: list[str] = []
    if est and not est.get("needs_manual_review"):
        if est.get("base_fee_range"):
            calc_log.append(f"Base: ${est['base_fee_range']['low']:,.0f}–${est['base_fee_range']['high']:,.0f}")
        if est.get("effective_multiplier") and est["effective_multiplier"] != 1.0:
            calc_log.append(f"Effective multiplier: ×{est['effective_multiplier']:.2f} (max of complexity/risk)")
        if est.get("rush_premium") and est["rush_premium"] != 1.0:
            calc_log.append(f"Rush premium: ×{est['rush_premium']:.2f} (<6 weeks to permit)")
        if est.get("floor_applied"):
            calc_log.append(f"Floor applied: ${est['floor_fee']:,.0f}")

    return {
        "intake_id":          intake_id,
        "project_name":       intake.project_name,
        "complexity":         decision["complexity_estimate"],
        "difficulty_tier":    decision.get("difficulty_tier", 2),
        "confidence":         confidence,
        "confidence_notes":   confidence_notes,
        "calculation_log":    calc_log,
        "fee_range":          decision.get("fee_range_estimate"),
        "cognasync":          est,
        "suggested_midpoint": midpoint,
        "client_name":        intake.client_name or "",
        "location_region":    intake.location_region or "",
        "project_type":       intake.answers.get("project_type") or "",
        "building_type":      intake.answers.get("building_type") or "",
        "approx_sf":          intake.answers.get("approx_sf") or "",
        "architect_name":     intake.architect_name or "",
    }


@app.get("/capacity", response_class=HTMLResponse)
def capacity_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/capacity"): return redir
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


def _snap_to_monday(d_str: Optional[str]) -> tuple[date, date, str]:
    """
    Snap a week_start string to the Monday of that week. Returns
    (monday, sunday, monday_iso). Falls back to the current week on parse error.
    """
    if d_str:
        try:
            ws = date.fromisoformat(d_str[:10])
        except ValueError:
            ws = date.today()
    else:
        ws = date.today()
    ws = ws - timedelta(days=ws.weekday())
    we = ws + timedelta(days=6)
    return ws, we, ws.isoformat()


def _build_week_load_grid(
    ws: date,
    we: date,
    *,
    include_pipeline: bool = True,
    include_existing_applied: bool = True,
) -> tuple[dict[str, dict[str, float]], list[dict[str, Any]], list[dict[str, Any]], list[staffing.AppliedMitigation]]:
    """
    Build the §10 load grid for the week [ws, we].

    Returns ``(load_grid, active_events, pipeline_events, applied)``.
    The grid already has applied mitigations folded in if
    ``include_existing_applied`` is True.
    """
    week_iso = ws.isoformat()

    events = db.list_calendar_events(
        start=ws.isoformat() + "T00:00:00Z",
        end=we.isoformat()   + "T23:59:59Z",
    )
    event_dicts = [e.to_dict() for e in events]
    covered_pns = {e.get("project_number") for e in event_dicts if e.get("project_number")}
    intake_events = db.get_active_intake_pseudo_events(covered_pns)

    staffing_events: list[dict[str, Any]] = []
    for ev in event_dicts + intake_events:
        e = dict(ev)
        e.setdefault("status", "ACTIVE_PROJECT")
        staffing_events.append(e)

    pipeline_events: list[dict[str, Any]] = []
    if include_pipeline:
        pipeline_events = _build_pipeline_pseudo_events(ws, we)
        staffing_events.extend(pipeline_events)

    load_grid = staffing.build_load_grid(staffing_events)

    applied: list[staffing.AppliedMitigation] = []
    if include_existing_applied:
        applied_rows = db.list_applied_mitigations(week=week_iso, include_reverted=False)
        applied = [_applied_from_row(r) for r in applied_rows]
        load_grid = staffing.apply_deltas_to_grid(load_grid, applied)

    return load_grid, event_dicts + intake_events, pipeline_events, applied


@app.get("/api/capacity")
def api_capacity(week_start: Optional[str] = None, include_pipeline: bool = True) -> dict:
    """
    Capacity snapshot for the week beginning ``week_start`` (snapped to Monday).

    The response combines the legacy WEU pool view (engineering/drafting cards)
    with the Section 10 staffing-engine output: per-person collisions, the
    matching mitigation suggestions, and an optional pipeline overlay where
    PROPOSAL_OUT projects contribute at 50% weight (§10.5 Commitment Lock).
    """
    ws, we, week_iso = _snap_to_monday(week_start)

    # Existing pool snapshot (unchanged) — feeds the existing UI cards.
    events = db.list_calendar_events(
        start=ws.isoformat() + "T00:00:00Z",
        end=we.isoformat()   + "T23:59:59Z",
    )
    event_dicts = [e.to_dict() for e in events]
    covered_pns = {e.get("project_number") for e in event_dicts if e.get("project_number")}
    intake_events = db.get_active_intake_pseudo_events(covered_pns)

    snapshot = weu_engine.get_capacity_snapshot(event_dicts + intake_events)
    snapshot["week_start"] = week_iso
    snapshot["week_end"]   = we.isoformat()

    # ── §10 Staffing engine overlay ──────────────────────────────────────
    load_grid, _all_events, pipeline_events, applied = _build_week_load_grid(
        ws, we, include_pipeline=include_pipeline, include_existing_applied=True
    )

    collisions = staffing.detect_collisions(load_grid)
    mitigations = staffing.suggest_mitigations(collisions, load_grid)

    # Filter to this week's collisions/mitigations so the UI doesn't show stale alerts.
    week_collisions  = [c for c in collisions  if c.week == week_iso]
    week_mitigations = [m for m in mitigations if m.week == week_iso]

    # Per-person summary keyed by initials for fast UI lookup.
    person_summary: dict[str, dict[str, Any]] = {}
    for initials, weekly in load_grid.items():
        hours = float(weekly.get(week_iso, 0.0))
        cap = float(staffing.ROSTER.get(initials, {}).get("weekly_cap_hours") or 0)
        person_summary[initials] = {
            "projected_hours": round(hours, 1),
            "cap_hours": cap,
            "utilization_pct": round((hours / cap * 100.0) if cap else 0.0, 1),
        }

    snapshot["staffing"] = {
        "week": week_iso,
        "include_pipeline": include_pipeline,
        "pipeline_events_count": len(pipeline_events),
        "collisions":  [c.to_dict() for c in week_collisions],
        "mitigations": [m.to_dict() for m in week_mitigations],
        "applied":     [a.to_dict() for a in applied],
        "person_summary": person_summary,
        "all_collisions_horizon":  len(collisions),    # across all weeks for stats
        "all_mitigations_horizon": len(mitigations),
    }
    return snapshot


def _applied_from_row(r: dict[str, Any]) -> staffing.AppliedMitigation:
    """Convert a DB row into a staffing.AppliedMitigation dataclass."""
    week_val = r.get("week") or ""
    if isinstance(week_val, str):
        week_iso = week_val[:10]
    else:
        week_iso = str(week_val)[:10]
    return staffing.AppliedMitigation(
        id=r.get("id"),
        pattern=str(r.get("pattern") or ""),
        week=week_iso,
        from_person=str(r.get("from_person") or ""),
        to_person=str(r.get("to_person") or ""),
        hours_delta=float(r.get("hours_delta") or 0.0),
        rationale=str(r.get("rationale") or ""),
        applied_by=str(r.get("applied_by") or ""),
        applied_at=str(r.get("applied_at") or ""),
        reverted_at=r.get("reverted_at"),
    )


@app.post("/api/staffing/apply-mitigation")
async def api_apply_mitigation(request: Request) -> JSONResponse:
    """
    Apply a mitigation suggestion. Body:
        {
          "pattern": "shift_pm_admin",
          "week":    "2026-07-27",
          "from_person": "RO",
          "to_person":   "JW",
          "hours_delta": 6.0,
          "rationale":   "Ryan saturated; Jacob has slack.",
          "acknowledge_warnings": false   // optional
        }

    Pre-flight safety check via ``staffing.preview_mitigation_safety``:
      * If the shift would push the receiver over their weekly cap, or
        push Mo above his 8-hr ceiling, or exceed the sender's available
        hours, the route returns **422** with the warnings.
      * To proceed anyway, the caller must re-submit with
        ``acknowledge_warnings: true``. In that case the warnings are
        appended to the persisted rationale for the audit trail.
    """
    _api_require(request, "admin")
    body = await request.json()
    required = ("pattern", "week", "from_person", "to_person", "hours_delta")
    missing = [k for k in required if k not in body]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing fields: {missing}")

    from_person = str(body["from_person"]).upper()
    to_person   = str(body["to_person"]).upper()
    if from_person not in staffing.ROSTER or to_person not in staffing.ROSTER:
        raise HTTPException(status_code=400, detail="Unknown roster initials.")
    if from_person == to_person:
        raise HTTPException(status_code=400, detail="from_person and to_person must differ.")
    try:
        hours = float(body["hours_delta"])
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="hours_delta must be numeric.")
    if hours <= 0:
        raise HTTPException(status_code=400, detail="hours_delta must be > 0.")

    week_iso = str(body["week"])[:10]
    try:
        ws = date.fromisoformat(week_iso)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Bad week '{body['week']}'. Expected YYYY-MM-DD.")
    we = ws + timedelta(days=6)

    # Build the current load grid (with already-applied mitigations folded in)
    # so the safety check reflects the receiver's CURRENT projected hours.
    load_grid, _events, _pipeline, _applied = _build_week_load_grid(
        ws, we, include_pipeline=True, include_existing_applied=True,
    )
    preview = staffing.preview_mitigation_safety(
        load_grid, from_person, to_person, week_iso, hours,
    )
    acknowledge = bool(body.get("acknowledge_warnings"))

    if not preview["safe"] and not acknowledge:
        return JSONResponse(
            status_code=422,
            content={
                "success": False,
                "safe": False,
                "warnings": preview["warnings"],
                "detail": (
                    "Mitigation would create a new bottleneck or breach a hard ceiling. "
                    "Re-submit with `acknowledge_warnings: true` to proceed anyway."
                ),
            },
        )

    # Persist. If the user acknowledged warnings, append them to rationale
    # for audit purposes so it's clear the shift was made despite known risks.
    rationale = str(body.get("rationale") or "")
    if not preview["safe"] and acknowledge:
        rationale = (rationale + " " if rationale else "") + "[ack: " + "; ".join(preview["warnings"]) + "]"

    user = ""
    try:
        user = request.session.get("user_email") or ""
    except Exception:
        user = ""

    row = db.record_applied_mitigation(
        pattern=str(body["pattern"]),
        week=week_iso,
        from_person=from_person,
        to_person=to_person,
        hours_delta=hours,
        rationale=rationale,
        applied_by=user,
    )
    if row is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Could not persist mitigation. Ensure the `staffing_mitigations` "
                "table exists in Supabase (see db.py docstring for schema)."
            ),
        )
    return JSONResponse(
        status_code=200,
        content={
            "success": True,
            "safe": preview["safe"],
            "warnings": preview["warnings"],
            "acknowledged": acknowledge and not preview["safe"],
            "applied": _applied_from_row(row).to_dict(),
        },
    )


@app.post("/api/staffing/mitigations/{mitigation_id}/revert")
async def api_revert_mitigation(mitigation_id: int) -> dict[str, Any]:
    """Soft-revert an applied mitigation."""
    existing = db.get_applied_mitigation(mitigation_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Mitigation not found.")
    if existing.get("reverted_at"):
        return {"success": True, "already_reverted": True}
    ok = db.revert_applied_mitigation(mitigation_id)
    if not ok:
        raise HTTPException(status_code=503, detail="Revert failed.")
    return {"success": True}


def _build_pipeline_pseudo_events(ws: date, we: date) -> list[dict[str, Any]]:
    """
    Synthesize PROPOSAL_OUT events from outstanding bids so the staffing
    engine can register them at 50% weight (§10.5).

    Each bid is converted to a single event spanning
    [inquiry_date or today] → [ifp_due_date or today+60d], with weu_hours
    derived from the approved fee (fee ÷ $200/hr efficiency ratio midpoint).
    The team is the default §10.2 template for the derived complexity tier.
    """
    try:
        bids = db.get_active_bids() or []
    except Exception:
        return []
    if not bids:
        return []

    today = date.today()
    out: list[dict[str, Any]] = []
    for bid in bids:
        approved_fee = float(bid.get("approved_fee") or 0.0)
        if approved_fee <= 0:
            continue

        # Window for the bid — clamp to a plausible default if missing
        start_str = bid.get("inquiry_date") or today.isoformat()
        end_str   = bid.get("ifp_due_date") or (today + timedelta(days=60)).isoformat()
        try:
            bid_start = date.fromisoformat(start_str[:10])
            bid_end   = date.fromisoformat(end_str[:10])
        except ValueError:
            continue
        if bid_end < bid_start:
            continue
        # If the bid window doesn't touch this week at all, skip it
        if bid_end < ws or bid_start > we:
            continue

        # Tier from project signals
        intake_hint = {
            "project_type": bid.get("project_type") or "",
            "sq_ft":        bid.get("approx_sf") or 0,
            "state":        bid.get("location") or "",
        }
        tier = staffing.derive_complexity_tier(intake_hint)
        team = staffing.assign_tier_template(tier, project_type=intake_hint["project_type"])

        # WEU hours from fee at $200/hr efficiency ratio midpoint
        weu_hours = approved_fee / 200.0

        out.append({
            "id":          f"bid-{bid.get('intake_id')}",
            "intake_id":   bid.get("intake_id"),
            "phase":       "DD",                                 # representative midpoint
            "tier":        tier,
            "team":        team.all_members,
            "weu_hours":   round(weu_hours, 1),
            "start_date":  bid_start.isoformat() + "T00:00:00Z",
            "end_date":    bid_end.isoformat()   + "T23:59:59Z",
            "client":      bid.get("client_name") or "",
            "location":    bid.get("location") or "",
            "project_number": "",
            "project_type":   bid.get("project_type") or "",
            "status":      "PROPOSAL_OUT",                       # → 50% weight
            "is_legacy":   False,
        })
    return out


# ── Settings ─────────────────────────────────────────────────────────────────

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/settings"): return redir
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
    _api_require(request, "admin")
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
    _api_require(request, "admin")
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


@app.get("/api/intakes/{intake_id}/time-summary")
def api_time_summary(intake_id: int) -> dict[str, Any]:
    entries = db.list_time_entries_for_intake(intake_id)
    by_phase: dict[str, dict] = {}
    for e in entries:
        phase = e["phase_code"]
        eng   = e["engineer_initials"]
        hrs   = float(e["hours"])
        if phase not in by_phase:
            by_phase[phase] = {"total_hours": 0.0, "engineers": {}}
        by_phase[phase]["total_hours"] = round(by_phase[phase]["total_hours"] + hrs, 1)
        by_phase[phase]["engineers"][eng] = round(
            by_phase[phase]["engineers"].get(eng, 0.0) + hrs, 1
        )
    result: dict[str, Any] = {}
    total_logged = 0.0
    for phase, d in by_phase.items():
        engs = sorted(
            [{"initials": k, "hours": v} for k, v in d["engineers"].items()],
            key=lambda x: -x["hours"],
        )
        result[phase] = {"total_hours": d["total_hours"], "engineers": engs}
        total_logged = round(total_logged + d["total_hours"], 1)
    return {"intake_id": intake_id, "total_logged": total_logged, "by_phase": result}


@app.get("/api/intakes/{intake_id}/projected-burn")
def api_projected_burn(intake_id: int) -> dict[str, Any]:
    intake = db.get_intake(intake_id)
    if not intake:
        raise HTTPException(status_code=404, detail="Not found.")

    _zero = {
        "intake_id": intake_id, "approved_fee": 0.0,
        "current_burn_hours": 0.0, "current_burn_value": 0.0, "current_burn_pct": 0.0,
        "remaining_resourced_hours": 0.0, "remaining_resourced_value": 0.0,
        "projected_burn_value": 0.0, "projected_burn_pct": 0.0,
        "remaining_budget": 0.0, "is_over_budget": False, "is_at_risk": False,
    }

    budgets = db.list_phase_budgets(intake_id)
    if not budgets:
        return _zero
    approved_fee = float(budgets[0]["approved_fee"])
    if approved_fee <= 0:
        return _zero

    potential = db.get_potential_hours_for_intake(intake_id)
    current_burn_hours = potential["potential"]   # submitted + approved only
    draft_burn_hours   = potential["draft"]       # logged but not yet submitted
    current_burn_value = round(current_burn_hours * db.BILLING_RATE, 2)

    remaining_resourced_hours = 0.0
    if intake.project_number:
        remaining_resourced_hours = db.get_remaining_resourced_hours(
            intake.project_number, date.today()
        )
    remaining_resourced_value = round(remaining_resourced_hours * db.BILLING_RATE, 2)

    projected_burn_value = round(current_burn_value + remaining_resourced_value, 2)

    def _pct(value: float) -> float:
        return round(value / approved_fee * 100, 1)

    return {
        "intake_id":                intake_id,
        "approved_fee":             approved_fee,
        "current_burn_hours":       round(current_burn_hours, 1),
        "current_burn_value":       current_burn_value,
        "current_burn_pct":         _pct(current_burn_value),
        "draft_burn_hours":         round(draft_burn_hours, 1),
        "remaining_resourced_hours": round(remaining_resourced_hours, 1),
        "remaining_resourced_value": remaining_resourced_value,
        "projected_burn_value":     projected_burn_value,
        "projected_burn_pct":       _pct(projected_burn_value),
        "remaining_budget":         round(approved_fee - projected_burn_value, 2),
        "is_over_budget":           projected_burn_value > approved_fee,
        "is_at_risk":               _pct(projected_burn_value) >= 85,
    }


@app.get("/api/burn-health")
def api_burn_health() -> list[dict[str, Any]]:
    return db.get_burn_health_data(date.today())


@app.get("/burn-health")
def burn_health_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/burn-health"): return redir
    return templates.TemplateResponse("burn_health.html", {
        "request":    request,
        "page_title": "Burn Health",
        "title":      "Burn Health — AVS",
    })


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
    _enforce_own_entries(request, engineer)
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
    if db.is_period_locked(engineer, entry_date):
        raise HTTPException(status_code=403, detail="This pay period has been submitted for review and is locked.")
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
    # Check if logging these hours pushed a billing phase to capacity
    billing_triggered: Optional[str] = None
    if intake_id:
        for bp_code, prod_phases in db.BILLING_TO_PRODUCTION.items():
            if phase_code in prod_phases:
                check = db.check_phase_hours_vs_budget(intake_id, bp_code)
                if check.get("budgeted", 0) > 0 and check.get("actual", 0) >= check.get("budgeted", 0):
                    if db.auto_trigger_billing_phase(intake_id, bp_code):
                        billing_triggered = bp_code
                break
    return {"id": entry_id, "success": True, "billing_triggered": billing_triggered}


@app.patch("/api/time-entries/{entry_id}")
async def api_update_time_entry(request: Request, entry_id: int) -> dict[str, Any]:
    entry = db.get_time_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Not found.")
    _enforce_own_entries(request, entry["engineer_initials"])
    if db.is_period_locked(entry["engineer_initials"], entry["entry_date"]):
        raise HTTPException(status_code=403, detail="This pay period has been submitted for review and is locked.")
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
def api_delete_time_entry(request: Request, entry_id: int) -> dict[str, Any]:
    entry = db.get_time_entry(entry_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Not found.")
    _enforce_own_entries(request, entry["engineer_initials"])
    if db.is_period_locked(entry["engineer_initials"], entry["entry_date"]):
        raise HTTPException(status_code=403, detail="This pay period has been submitted for review and is locked.")
    db.delete_time_entry(entry_id)
    return {"deleted": entry_id}


# ── Timesheet Submission API ───────────────────────────────────────────────────

@app.get("/api/timesheet/submission")
def api_get_submission(engineer: str, start: str, end: str) -> dict[str, Any]:
    if not engineer or not start or not end:
        raise HTTPException(status_code=400, detail="engineer, start, end required.")
    try:
        sub = db.get_submission(engineer, start)
    except Exception:
        sub = None
    if not sub:
        return {"status": "DRAFT", "engineer": engineer, "period_start": start, "period_end": end}
    return sub


@app.post("/api/timesheet/submit")
async def api_submit_period(request: Request) -> dict[str, Any]:
    body = await request.json()
    engineer = (body.get("engineer") or "").strip().upper()
    period_start = (body.get("period_start") or "").strip()
    period_end = (body.get("period_end") or "").strip()
    if not engineer or not period_start or not period_end:
        raise HTTPException(status_code=400, detail="engineer, period_start, period_end required.")
    _enforce_own_entries(request, engineer)
    # Compute total hours for this engineer/period
    try:
        entries = db.list_time_entries(start=period_start, end=period_end, engineer=engineer)
        total_hours = round(sum(float(e.get("hours") or 0) for e in entries), 2)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read time entries: {exc}")
    if total_hours == 0:
        raise HTTPException(
            status_code=400,
            detail=f"No hours logged for {engineer} between {period_start} and {period_end}. "
                   "Make sure you've added time entries in the form below, then try again.",
        )
    try:
        sub = db.submit_period(engineer, period_start, period_end, total_hours)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not save submission: {exc}")
    return sub


@app.post("/api/timesheet/review/{submission_id}")
async def api_review_submission(request: Request, submission_id: int) -> dict[str, Any]:
    _api_require(request, "admin", "office_manager")
    body = await request.json()
    action = (body.get("action") or "").strip().lower()
    notes = _as_str(str(body.get("notes") or ""))
    # Accept both verb ("approve") and past-tense ("approved") forms
    _action_map = {"approve": "approve", "approved": "approve", "reject": "reject", "rejected": "reject"}
    action = _action_map.get(action, "")
    if not action:
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'.")
    result = db.review_submission(submission_id, action, notes or None)
    return result


@app.get("/api/timesheet/review-queue")
def api_review_queue(request: Request) -> list[dict[str, Any]]:
    _api_require(request, "admin", "office_manager")
    return db.get_review_queue()


@app.get("/my-launch", response_class=HTMLResponse)
def engineer_launch_page(request: Request, engineer: Optional[str] = None) -> HTMLResponse:
    qs = f"?engineer={engineer}" if engineer else ""
    return RedirectResponse(url=f"/my-time{qs}#hours", status_code=302)


# ── Engineer Dashboard ────────────────────────────────────────────────────────

@app.get("/engineer-dashboard", response_class=HTMLResponse)
def engineer_dashboard_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/engineer-dashboard"): return redir
    user     = _session_user(request) or {}
    initials = (user.get("initials") or "").upper()
    import json as _json
    data: dict[str, Any] = {}
    error: Optional[str] = None
    try:
        data = db.get_engineer_dashboard_data(initials)
    except Exception as exc:
        error = str(exc)
        data  = {
            "engineer": initials, "name": user.get("name", ""),
            "role_title": "", "color": "#888",
            "is_eit": False, "mentor_initials": None, "mentor_name": None,
            "week": {"start": "", "end": "", "logged_hours": 0,
                     "capacity_hours": 40, "utilization_pct": 0, "ooo_days": 0},
            "projects": [], "milestones": [],
        }
    return templates.TemplateResponse("engineer_dashboard.html", {
        "request":          request,
        "title":            f"{data.get('name', initials)} — Dashboard",
        "data":             data,
        "phase_colors":     db.PHASE_COLORS,
        "phase_colors_json": _json.dumps(db.PHASE_COLORS),
        "error":            error,
    })


@app.get("/api/engineer/dashboard/{initials}")
def api_engineer_dashboard(request: Request, initials: str) -> dict[str, Any]:
    user        = _session_user(request) or {}
    user_ini    = (user.get("initials") or "").upper()
    user_role   = user.get("role", "")
    target      = initials.upper()
    if user_role not in ("admin", "office_manager") and target != user_ini:
        raise HTTPException(status_code=403, detail="You can only view your own dashboard.")
    return db.get_engineer_dashboard_data(target)


@app.post("/api/engineer/qa-review/{intake_id}")
async def api_request_qa_review(request: Request, intake_id: int) -> dict[str, Any]:
    user     = _session_user(request) or {}
    initials = (user.get("initials") or "").strip()
    if not initials:
        raise HTTPException(status_code=401, detail="Not authenticated.")
    existing = (
        db._client()
        .table("intakes")
        .select("id,mo_conditions")
        .eq("id", intake_id)
        .maybe_single()
        .execute()
    )
    if not existing.data:
        raise HTTPException(status_code=404, detail="Project not found.")
    note = f"QA REVIEW REQUESTED by {initials} on {date.today().isoformat()}"
    cur  = (existing.data.get("mo_conditions") or "").strip()
    db._client().table("intakes").update({
        "mo_conditions": f"{cur}\n{note}".strip() if cur else note,
        "updated_at":    db._utc_now_iso(),
    }).eq("id", intake_id).execute()
    return {"success": True, "note": note}


@app.get("/approvals", response_class=HTMLResponse)
def approvals_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/approvals"): return redir
    error = None
    submitted: list[dict] = []
    all_recent: list[dict] = []
    approved_count = 0
    total_count = 0
    try:
        submitted  = db.get_enriched_review_queue()
        all_recent = db.get_all_recent_submissions(30)
        approved_count = sum(1 for r in all_recent if r.get("status") == "APPROVED")
        total_count    = len(all_recent)
    except Exception as exc:
        error = str(exc)
    return templates.TemplateResponse("approvals.html", {
        "request":        request,
        "submitted":      submitted,
        "all_recent":     all_recent,
        "approved_count": approved_count,
        "total_count":    total_count,
        "error":          error,
        "title":          "Timesheet Approvals — AVS",
    })


@app.get("/api/active-projects")
def api_active_projects(engineer: Optional[str] = None) -> list[dict[str, Any]]:
    return db.list_active_projects(engineer=engineer)


# ── Timesheet page ────────────────────────────────────────────────────────────

@app.get("/timesheet", response_class=HTMLResponse)
def timesheet_page(request: Request) -> HTMLResponse:
    return RedirectResponse(url="/my-time#timesheet", status_code=302)


# ── Payroll Export ────────────────────────────────────────────────────────────

@app.get("/billing-queue", response_class=HTMLResponse)
def billing_queue_page(request: Request) -> HTMLResponse:
    if redir := _check_page_access(request, "/billing-queue"): return redir
    start, end = _current_pay_period()
    try:
        pending_invoices = db.get_pending_invoice_approvals()
    except Exception:
        pending_invoices = []
    try:
        pending_billables = db.get_pending_billables()
    except Exception:
        pending_billables = []
    try:
        recent_invoices = db.get_all_invoices(limit=30)
    except Exception:
        recent_invoices = []
    try:
        burn_vs_bill = db.get_burn_vs_bill()
    except Exception:
        burn_vs_bill = []
    try:
        timecard_summary = db.get_firm_timecard_summary(start, end)
    except Exception:
        timecard_summary = []
    try:
        today_entries = db.list_time_entries_today()
    except Exception:
        today_entries = []
    try:
        payroll_audit = db.get_payroll_audit(start, end)
    except Exception:
        payroll_audit = []
    try:
        cash_flow = db.get_cash_flow_forecast()
    except Exception:
        cash_flow = {"total": 0.0, "count": 0}
    try:
        stale_projects = db.get_stale_projects(days=14)
    except Exception:
        stale_projects = []
    try:
        utilization = db.get_utilization_summary(start, end)
    except Exception:
        utilization = {"total": 0.0, "billable": 0.0, "admin": 0.0, "billable_pct": 0.0}
    try:
        engineer_project_hours = db.get_engineer_project_hours()
    except Exception:
        engineer_project_hours = []
    # Payroll Export tab data
    try:
        pending_submissions = db.get_review_queue()
    except Exception:
        pending_submissions = []
    try:
        pending_expenses = db.get_pending_reimbursable_expenses()
    except Exception:
        pending_expenses = []
    today_d = date.today()
    pr_start, pr_end = _current_pay_period()
    if pr_end >= today_d.isoformat():
        pr_start = (date.fromisoformat(pr_start) - timedelta(days=14)).isoformat()
        pr_end   = (date.fromisoformat(pr_end)   - timedelta(days=14)).isoformat()
    submitted   = sum(1 for e in timecard_summary if e["submission_status"] == "SUBMITTED")
    approved    = sum(1 for e in timecard_summary if e["submission_status"] == "APPROVED")
    not_started = sum(1 for e in timecard_summary if e["submission_status"] == "NOT_STARTED")
    today_total = round(sum(e["today_hours"] for e in timecard_summary), 1)
    profit_risk_count = sum(1 for p in burn_vs_bill if p.get("profit_risk"))
    return templates.TemplateResponse(
        "billing_queue.html",
        {
            "request":                request,
            "title":                  "Payroll Hub — AVS",
            "now_local":              _now_local_iso(),
            "period_start":           start,
            "period_end":             end,
            "pending_invoices":       pending_invoices,
            "pending_billables":      pending_billables,
            "recent_invoices":        recent_invoices,
            "burn_vs_bill":           burn_vs_bill,
            "profit_risk_count":      profit_risk_count,
            "timecard_summary":       timecard_summary,
            "today_entries":          today_entries,
            "submitted":              submitted,
            "approved":               approved,
            "not_started":            not_started,
            "today_total":            today_total,
            "staff_count":            len(timecard_summary),
            "team_colors":            db.TEAM_COLORS,
            "payroll_audit":          payroll_audit,
            "cash_flow":              cash_flow,
            "stale_projects":         stale_projects,
            "utilization":            utilization,
            "engineer_project_hours": engineer_project_hours,
            # Payroll Export tab
            "pending_submissions":    pending_submissions,
            "payroll_default_start":  pr_start,
            "payroll_default_end":    pr_end,
            # Expenses
            "pending_expenses":       pending_expenses,
        },
    )


@app.post("/api/expenses")
async def api_create_expense(request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    receipt_url: Optional[str] = None
    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" in content_type:
        form = await request.form()
        intake_id_raw   = form.get("intake_id", "")
        phase           = (form.get("phase") or "").strip() or None
        amount_raw      = form.get("amount", "0")
        category        = (form.get("category") or "").strip()
        description     = (form.get("description") or "").strip() or None
        is_reimbursable = str(form.get("is_reimbursable", "true")).lower() in ("true", "1", "on")
        receipt_file    = form.get("receipt")
        if receipt_file and hasattr(receipt_file, "filename") and receipt_file.filename:
            try:
                import mimetypes as _mt
                _ALLOWED_RECEIPT_EXT  = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".heic"}
                _MAX_RECEIPT_BYTES    = 10 * 1024 * 1024  # 10 MB
                # Sanitize: strip path components, then remove any character that
                # isn't alphanumeric, a dot, a hyphen, or an underscore.
                _raw_name = os.path.basename(receipt_file.filename.replace("\\", "/"))
                safe_name = _re.sub(r"[^\w.\-]", "_", _raw_name).strip("._") or "receipt"
                ext = Path(safe_name).suffix.lower()
                if ext not in _ALLOWED_RECEIPT_EXT:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Receipt must be a PDF, JPG, PNG, or WebP file (got '{ext}')."
                    )
                contents = await receipt_file.read()
                if len(contents) > _MAX_RECEIPT_BYTES:
                    raise HTTPException(status_code=400, detail="Receipt file exceeds the 10 MB size limit.")
                path = f"expenses/{user['initials']}/{_now_local_iso()[:10]}_{safe_name}"
                ct   = receipt_file.content_type or _mt.guess_type(safe_name)[0] or "application/octet-stream"
                db._client().storage.from_("receipts").upload(path, contents, {"content-type": ct})
                receipt_url = db._client().storage.from_("receipts").get_public_url(path)
            except HTTPException:
                raise
            except Exception:
                pass
    else:
        body            = await request.json()
        intake_id_raw   = body.get("intake_id", "")
        phase           = (body.get("phase") or "").strip() or None
        amount_raw      = body.get("amount", 0)
        category        = (body.get("category") or "").strip()
        description     = (body.get("description") or "").strip() or None
        is_reimbursable = bool(body.get("is_reimbursable", True))
        receipt_url     = (body.get("receipt_url") or "").strip() or None
    try:
        intake_id = int(intake_id_raw)
        amount    = float(amount_raw)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid intake_id or amount")
    if not intake_id or amount <= 0 or not category:
        raise HTTPException(status_code=400, detail="intake_id, amount, and category are required")
    return db.create_expense(
        intake_id=intake_id,
        engineer_initials=user["initials"],
        phase=phase,
        amount=amount,
        category=category,
        description=description,
        receipt_url=receipt_url,
        is_reimbursable=is_reimbursable,
    )


@app.get("/api/expenses/project/{intake_id}")
def api_expenses_for_project(intake_id: int, request: Request) -> list[dict[str, Any]]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return db.get_expenses_for_project(intake_id)


@app.get("/api/expenses/engineer/{initials}")
def api_expenses_for_engineer(initials: str, request: Request) -> list[dict[str, Any]]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    target = initials.upper()
    if user.get("role") not in ("admin", "office_manager") and user.get("initials", "").upper() != target:
        raise HTTPException(status_code=403, detail="Access denied")
    return db.get_expenses_for_engineer(target)


@app.patch("/api/expenses/{expense_id}")
async def api_update_expense(expense_id: str, request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Access denied")
    body = await request.json()
    allowed = {"status", "is_reimbursable"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid update fields provided")
    if "status" in updates and updates["status"] not in ("pending", "approved", "billed"):
        raise HTTPException(status_code=400, detail="Invalid status value")
    return db.update_expense(expense_id, updates)


@app.get("/api/payroll/all-timecards")
def api_all_timecards(request: Request) -> list[dict[str, Any]]:
    user = _session_user(request)
    if not user or user["role"] not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Access Denied: You do not have payroll oversight permissions.")
    start, end = _current_pay_period()
    return db.get_firm_timecard_summary(start, end)


@app.get("/api/payroll/period-lock-status")
def api_period_lock_status(start: str, end: str, request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Access denied")
    return {"locked": db.is_period_globally_locked(start, end)}


@app.post("/api/payroll/lock-period")
async def api_lock_pay_period(request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Access denied")
    body = await request.json()
    period_start = (body.get("period_start") or "").strip()
    period_end   = (body.get("period_end") or "").strip()
    if not period_start or not period_end:
        raise HTTPException(status_code=400, detail="period_start and period_end are required")
    db.lock_pay_period(period_start, period_end, user.get("initials", "?"))
    return {"ok": True, "period_start": period_start, "period_end": period_end}


@app.get("/payroll-export", response_class=HTMLResponse)
def payroll_export_page(request: Request) -> HTMLResponse:
    return RedirectResponse(url="/billing-queue#payroll", status_code=302)


@app.get("/api/payroll-export")
def api_payroll_export(request: Request, start: Optional[str] = None, end: Optional[str] = None) -> dict[str, Any]:
    _api_require(request, "admin", "office_manager", "billing")
    if not start or not end:
        s, e = _current_pay_period()
        start = start or s
        end = end or e
    data = db.get_payroll_data(start, end)
    approved_engineers = db.get_approved_engineers_for_period(start, end)
    entries = data.get("entries") or []
    unapproved_count = sum(
        1 for e in entries
        if e.get("engineer_initials") not in approved_engineers
    )
    data["approved_engineers"] = list(approved_engineers)
    data["unapproved_entry_count"] = unapproved_count
    return data


@app.get("/api/payroll-export/csv")
def api_payroll_export_csv(request: Request, start: Optional[str] = None, end: Optional[str] = None) -> StreamingResponse:
    _api_require(request, "admin", "office_manager", "billing")
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
    if redir := _check_page_access(request, "/pipeline"): return redir
    data = db.get_pipeline_data()
    return templates.TemplateResponse(
        "pipeline.html",
        {
            "request":          request,
            "now_local":        _now_local_iso(),
            "pipeline":         data,
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
    user = _api_require(request, "admin")
    body = await request.json()
    to_phase = str(body.get("to_phase") or "").strip()
    note = str(body.get("note") or "").strip()
    if not to_phase:
        raise HTTPException(status_code=400, detail="to_phase required")
    if not note:
        raise HTTPException(status_code=400, detail="note required")
    completed_by = user.get("initials") or user.get("name") or "SYSTEM"
    return db.advance_production_phase(intake_id, to_phase, completed_by, note)


@app.post("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/mark-invoiced")
async def api_mark_invoiced(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    user = _session_user(request)
    invoiced_by = (user or {}).get("initials") or "NK"
    try:
        db.mark_billing_phase_invoiced(intake_id, billing_phase_code, invoiced_by)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"ok": True, "invoiced_by": invoiced_by}


@app.get("/api/invoice-preview/{intake_id}/{phase_code}")
async def api_invoice_preview(request: Request, intake_id: int, phase_code: str) -> dict[str, Any]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    try:
        return db.get_invoice_preview(intake_id, phase_code)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/invoices")
async def api_create_invoice(request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401)
    body = await request.json()
    intake_id  = body.get("intake_id")
    phase_code = body.get("phase_code")
    amount     = body.get("amount")
    notes      = str(body.get("notes") or "")
    if not intake_id or not phase_code or amount is None:
        raise HTTPException(status_code=422, detail="intake_id, phase_code, amount required")
    try:
        return db.create_invoice(
            int(intake_id), phase_code, float(amount),
            user.get("initials", "NK"), notes,
            po_number=str(body.get("po_number") or ""),
            po_attachment_url=str(body.get("po_attachment_url") or ""),
            custom_fields=body.get("custom_fields") or [],
            use_timesheet_hours=bool(body.get("use_timesheet_hours", False)),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.patch("/api/invoices/{invoice_id}/status")
async def api_update_invoice_status(request: Request, invoice_id: int) -> dict[str, Any]:
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401)
    body   = await request.json()
    status = str(body.get("status") or "")
    if status not in ("draft", "sent", "paid"):
        raise HTTPException(status_code=422, detail="status must be draft, sent, or paid")
    db.update_invoice_status(invoice_id, status, user.get("initials", ""))
    return {"ok": True}


# ── Client Profiles ───────────────────────────────────────────────────────────

@app.get("/api/client-profiles")
def api_list_client_profiles(request: Request) -> list[dict[str, Any]]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    return db.list_client_profiles()


@app.get("/api/client-profiles/client-names")
def api_client_names(request: Request) -> list[str]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    return db.get_unique_client_names()


@app.get("/api/client-profiles/{client_name:path}")
def api_get_client_profile(client_name: str, request: Request) -> dict[str, Any]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    profile = db.get_client_profile(client_name)
    if not profile:
        return {"client_name": client_name, "requires_po": False, "po_number": "",
                "custom_invoice_fields": [], "invoice_notes": ""}
    return profile


@app.put("/api/client-profiles/{client_name:path}")
async def api_upsert_client_profile(client_name: str, request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Office manager access required")
    body = await request.json()
    return db.upsert_client_profile(
        client_name=client_name,
        requires_po=bool(body.get("requires_po", False)),
        po_number=str(body.get("po_number") or ""),
        custom_invoice_fields=body.get("custom_invoice_fields") or [],
        invoice_notes=str(body.get("invoice_notes") or ""),
    )


# ── Expense Reimbursement ─────────────────────────────────────────────────────

@app.patch("/api/expenses/{expense_id}/reimburse")
async def api_reimburse_expense(expense_id: str, request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Office manager access required")
    return db.mark_expense_reimbursed(expense_id, user.get("initials", "?"))


@app.post("/api/expenses/mark-client-invoiced")
async def api_expenses_client_invoiced(request: Request) -> dict[str, Any]:
    user = _session_user(request)
    if not user or user.get("role") not in ("admin", "office_manager"):
        raise HTTPException(status_code=403, detail="Office manager access required")
    body = await request.json()
    expense_ids    = body.get("expense_ids") or []
    invoice_number = str(body.get("invoice_number") or "")
    if not expense_ids:
        raise HTTPException(status_code=422, detail="expense_ids required")
    count = db.mark_expenses_client_invoiced(
        expense_ids, invoice_number, user.get("initials", "?")
    )
    return {"ok": True, "marked": count}


# ── Extended invoice creation (picks up new fields) ───────────────────────────
# NOTE: The existing POST /api/invoices already calls db.create_invoice.
# We patch it here to forward the new optional fields from the request body.
# The old route definition above is replaced by this one at runtime because
# FastAPI uses the last registered route for duplicate paths — so we re-register.


@app.get("/api/burn-vs-bill")
async def api_burn_vs_bill(request: Request) -> list[dict[str, Any]]:
    if not _session_user(request):
        raise HTTPException(status_code=401)
    try:
        return db.get_burn_vs_bill()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/approve-invoice")
async def api_approve_invoice(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    user = _api_require(request, "admin", "office_manager")
    body = await request.json()
    return db.approve_invoice(
        intake_id,
        billing_phase_code,
        approved_by=user.get("initials") or user.get("name") or "SYSTEM",
        fee_override=float(body["fee_override"]) if body.get("fee_override") is not None else None,
        note=str(body.get("note") or "") or None,
    )


@app.post("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/decline-invoice")
async def api_decline_invoice(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    user = _api_require(request, "admin", "office_manager")
    body = await request.json()
    reason = str(body.get("reason") or "").strip()
    if not reason:
        raise HTTPException(status_code=400, detail="reason required")
    return db.decline_invoice(
        intake_id,
        billing_phase_code,
        declined_by=user.get("initials") or user.get("name") or "SYSTEM",
        reason=reason,
    )


@app.patch("/api/projects/{intake_id}/billing-phases/{billing_phase_code}/fee")
async def api_update_billing_phase_fee(request: Request, intake_id: int, billing_phase_code: str) -> dict[str, Any]:
    body = await request.json()
    new_fee = body.get("fee_amount")
    if new_fee is None:
        raise HTTPException(status_code=400, detail="fee_amount required")
    try:
        new_fee = float(new_fee)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="fee_amount must be a number")
    db.update_billing_phase_fee(intake_id, billing_phase_code, new_fee)
    return {"success": True, "fee_amount": new_fee}


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
    _api_require(request, "admin")
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
