"""
Staffing Assignment Engine — implements CLAUDE.md Section 10.

Responsibilities
----------------
1. Derive a Section-5 complexity tier (1-4) from intake signals.
2. Assign a named-person team using the Section 10.2 tier template,
   with adjustments for Rush schedules, retrofits, and out-of-state stamping.
3. Validate hard constraints (§10.3): EITs blocked from Tier 4,
   Nathan must lead retrofits, Mo capped at 8 hrs/wk, etc.
4. Build a per-person / per-week load grid that folds PROPOSAL_OUT
   projects in at 50% weight (§10.5 Commitment Lock).
5. Detect collisions and severity-flag them (YELLOW/RED).
6. Suggest mitigations in the §10.6 priority order.

Design notes
------------
* The module operates on plain dicts so it can be tested without the DB.
* Hour caps in this module are real billable hours per week (per spec §10.1),
  separate from the normalized WEU capacity used by ``weu.compute_weu``.
* If you change a roster value here, also update ``weu.TEAM_CONFIG`` so the
  two systems agree on multipliers and pool assignments.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.1 — Roster
# ─────────────────────────────────────────────────────────────────────────────

ROSTER: dict[str, dict[str, Any]] = {
    "MK": {
        "name": "Mo Kateeb",
        "role": "President / Stamping PE",
        "seniority": "senior_plus",
        "pool": "engineering",
        "weekly_cap_hours": 8,        # oversight-only finite resource
        "efficiency": 1.0,
        "is_eit": False,
        "can_stamp": True,
        "leads_retrofits": True,
    },
    "NK": {
        "name": "Nathan Kline",
        "role": "Principal / Lead Senior PE",
        "seniority": "senior",
        "pool": "engineering",
        "weekly_cap_hours": 32,
        "efficiency": 1.0,
        "is_eit": False,
        "can_stamp": True,
        "leads_retrofits": True,
    },
    "RO": {
        "name": "Ryan Olson",
        "role": "Project Manager / Mid-PE",
        "seniority": "mid",
        "pool": "engineering",
        "weekly_cap_hours": 36,
        "efficiency": 1.1,
        "is_eit": False,
        "can_stamp": False,
        "leads_retrofits": False,
    },
    "JW": {
        "name": "Jacob Wadman",
        "role": "Project Engineer",
        "seniority": "mid",
        "pool": "engineering",
        "weekly_cap_hours": 36,
        "efficiency": 1.25,
        "is_eit": False,
        "can_stamp": False,
        "leads_retrofits": False,    # explicitly forbidden by §10.3
    },
    "JR": {
        "name": "Josh Robinder",
        "role": "EIT",
        "seniority": "junior",
        "pool": "engineering",
        "weekly_cap_hours": 36,
        "efficiency": 1.5,
        "is_eit": True,
        "can_stamp": False,
        "leads_retrofits": False,
        "edge_over_rajul": True,     # slightly stronger technical scope
    },
    "RK": {
        "name": "Rajul Kanth",
        "role": "EIT",
        "seniority": "junior",
        "pool": "engineering",
        "weekly_cap_hours": 36,
        "efficiency": 1.5,
        "is_eit": True,
        "can_stamp": False,
        "leads_retrofits": False,
    },
    "RS": {
        "name": "Randall Smith",
        "role": "CAD/BIM Manager",
        "seniority": "senior_plus",
        "pool": "drafting",
        "weekly_cap_hours": 24,
        "efficiency": 1.0,
        "is_eit": False,
        "production_approved": True, # §10.6 — explicitly approved for production sheets
    },
    "SW": {
        "name": "Scott Webb",
        "role": "Senior CAD Designer",
        "seniority": "senior",
        "pool": "drafting",
        "weekly_cap_hours": 36,
        "efficiency": 1.0,
        "is_eit": False,
    },
    "JP": {
        "name": "Jesus Prado",
        "role": "CAD Designer",
        "seniority": "standard",
        "pool": "drafting",
        "weekly_cap_hours": 36,
        "efficiency": 1.3,
        "is_eit": False,
    },
}

EITS: set[str] = {k for k, v in ROSTER.items() if v.get("is_eit")}
ENGINEERS: set[str] = {k for k, v in ROSTER.items() if v.get("pool") == "engineering"}
DRAFTERS: set[str] = {k for k, v in ROSTER.items() if v.get("pool") == "drafting"}


def baseline_capacity_hours() -> dict[str, float]:
    """Total weekly billable capacity by pool — for sanity checks (§10.1)."""
    eng = sum(ROSTER[k]["weekly_cap_hours"] for k in ENGINEERS if k != "MK")
    eng_with_mo = sum(ROSTER[k]["weekly_cap_hours"] for k in ENGINEERS)
    draft = sum(ROSTER[k]["weekly_cap_hours"] for k in DRAFTERS)
    return {
        "drafting": float(draft),
        "engineering_excl_mo": float(eng),
        "engineering_incl_mo": float(eng_with_mo),
        "mo_oversight": float(ROSTER["MK"]["weekly_cap_hours"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.2 — Default tier templates (named people)
# ─────────────────────────────────────────────────────────────────────────────

TIER_TEMPLATES: dict[int, dict[str, Any]] = {
    1: {
        "label": "Standard Industrial",
        "lead_pe": "JW",
        "stamp": "NK",                # QA only, ~5% WEU
        "eng_support": [],            # EIT optional, off by default
        "eng_support_optional": ["JR", "RK"],
        "drafting_lead": "SW",
        "drafting_support": ["JP"],
        "notes": "Keep Nathan light to preserve capacity for higher tiers.",
    },
    2: {
        "label": "Standard Design",
        "lead_pe": "RO",              # default; flips to JW based on slack
        "lead_pe_alternate": "JW",
        "stamp": "NK",
        "eng_support": ["JR"],        # one EIT default; alternate Rajul
        "drafting_lead": "SW",
        "drafting_support": ["JP", "RS"],
        "notes": "On Rush, prefer Ryan over Jacob for speed.",
    },
    3: {
        "label": "Complex Multi-Phase",
        "lead_pe": "NK",              # mandatory on retrofits
        "stamp": "MK",                # Mo final seal review
        "eng_support": ["RO", "JR"],  # Ryan PM duties + EIT (Josh preferred)
        "drafting_lead": "RS",        # Randall for retrofit/BIM models
        "drafting_support": ["SW"],
        "notes": "Nathan handles technical decisions; Ryan handles coord/client comms.",
    },
    4: {
        "label": "High-Liability",
        "lead_pe": "NK",
        "stamp": "MK",                # MANDATORY
        "eng_support": ["RO"],        # NO EITs
        "drafting_lead": "SW",
        "drafting_support": ["RS"],
        "notes": "NO EITs. Min 15% Senior PE oversight AND ≥ 4 hrs/wk while active.",
        "senior_pe_oversight_min_pct": 0.15,
        "senior_pe_min_hours_per_week": 4.0,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.7 — Phase-to-role weighting
# ─────────────────────────────────────────────────────────────────────────────

PHASE_WEU_SHARE: dict[str, float] = {
    "Intake": 0.05,
    "SD":     0.20,
    "DD":     0.30,
    "CD":     0.35,
    "CA":     0.10,
}

# Within each phase, allocate WEUs across role buckets.
ROLE_WEIGHTS_BY_PHASE: dict[str, dict[str, float]] = {
    "Intake": {"senior_pe": 0.30, "mid_pe": 0.50, "eit": 0.10, "senior_cad": 0.10, "cad": 0.00},
    "SD":     {"senior_pe": 0.25, "mid_pe": 0.35, "eit": 0.20, "senior_cad": 0.15, "cad": 0.05},
    "DD":     {"senior_pe": 0.20, "mid_pe": 0.30, "eit": 0.20, "senior_cad": 0.20, "cad": 0.10},
    "CD":     {"senior_pe": 0.10, "mid_pe": 0.20, "eit": 0.15, "senior_cad": 0.30, "cad": 0.25},
    "CA":     {"senior_pe": 0.30, "mid_pe": 0.50, "eit": 0.05, "senior_cad": 0.10, "cad": 0.05},
}


# ─────────────────────────────────────────────────────────────────────────────
# Section 5 — Complexity tier derivation
# ─────────────────────────────────────────────────────────────────────────────

HIGH_LIABILITY_TYPES = {"healthcare", "med_spa", "data_center", "historic", "urm"}
ADAPTIVE_REUSE_TYPES = {"adaptive_reuse", "retrofit", "renovation"}
INDUSTRIAL_TYPES = {"industrial_shell", "warehouse", "industrial"}


def derive_complexity_tier(intake: dict[str, Any]) -> int:
    """
    Map a Section-5 complexity tier (1–4) from intake signals.

    Distinct from ``db.infer_tier_from_intake`` which is fee-based 1–5.
    """
    project_type = str(intake.get("project_type") or "").lower()
    building_type = str(intake.get("building_type") or "").lower()
    sq_ft = _safe_int(intake.get("sq_ft") or intake.get("approx_sf"))

    # Tier 4 — High-liability (Healthcare, Data Center, Historic/URM)
    for keyword in HIGH_LIABILITY_TYPES:
        if keyword in project_type or keyword in building_type:
            return 4

    # Tier 3 — Complex Multi-Phase (Adaptive Reuse / Retrofit / large or mixed)
    for keyword in ADAPTIVE_REUSE_TYPES:
        if keyword in project_type or keyword in building_type:
            return 3
    if sq_ft > 50_000 and not _is_industrial(project_type, building_type):
        return 3
    if "mixed" in building_type or "education" in building_type:
        return 3

    # Tier 1 — Standard Industrial Shell
    if _is_industrial(project_type, building_type):
        return 1

    # Tier 2 — everything else (Standard Design, 5k–50k SF)
    return 2


def _is_industrial(project_type: str, building_type: str) -> bool:
    return any(k in project_type or k in building_type for k in INDUSTRIAL_TYPES)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.2/10.3 — Assignment
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TeamAssignment:
    tier: int
    tier_label: str
    lead_pe: str
    stamp: str
    eng_support: list[str] = field(default_factory=list)
    drafting_lead: str = ""
    drafting_support: list[str] = field(default_factory=list)
    notes: str = ""
    adjustments: list[str] = field(default_factory=list)

    @property
    def all_members(self) -> list[str]:
        seen: list[str] = []
        for m in [self.lead_pe, self.stamp, self.drafting_lead, *self.eng_support, *self.drafting_support]:
            if m and m not in seen:
                seen.append(m)
        return seen

    def to_dict(self) -> dict[str, Any]:
        return {
            "tier": self.tier,
            "tier_label": self.tier_label,
            "lead_pe": self.lead_pe,
            "stamp": self.stamp,
            "eng_support": list(self.eng_support),
            "drafting_lead": self.drafting_lead,
            "drafting_support": list(self.drafting_support),
            "all_members": self.all_members,
            "notes": self.notes,
            "adjustments": list(self.adjustments),
        }


def assign_tier_template(
    tier: int,
    *,
    schedule_tier: str = "Standard",
    project_type: str = "",
    state: str = "",
    eit_rotation_hint: Optional[str] = None,
) -> TeamAssignment:
    """
    Apply §10.2 template, then adjust for:
      * Rush schedule (Tier 2 → prefer Ryan over Jacob)
      * Retrofit / Adaptive Reuse / Historic → Nathan as lead PE
      * Out-of-state → Nathan as stamp-of-record (handled in validate)
      * EIT rotation hint (alternate Josh/Rajul; Josh preferred on technical)

    `eit_rotation_hint` is the previous project's primary EIT to alternate from.
    """
    if tier not in TIER_TEMPLATES:
        raise ValueError(f"Unknown tier: {tier}. Expected 1-4.")
    template = TIER_TEMPLATES[tier]
    adjustments: list[str] = []

    lead_pe = template["lead_pe"]
    stamp = template["stamp"]
    eng_support = list(template.get("eng_support", []))
    drafting_lead = template["drafting_lead"]
    drafting_support = list(template.get("drafting_support", []))

    pt_lower = (project_type or "").lower()
    is_retrofit = any(k in pt_lower for k in ADAPTIVE_REUSE_TYPES) or "historic" in pt_lower

    # Rule: retrofits/adaptive reuse → Nathan leads (§10.3)
    if is_retrofit and lead_pe != "NK":
        adjustments.append(f"Retrofit/Adaptive Reuse → Nathan replaces {lead_pe} as lead PE")
        lead_pe = "NK"

    # Rule: Rush Tier 2 → Ryan over Jacob (§10.4 schedule-pressure)
    if tier == 2 and schedule_tier.lower() == "rush":
        if lead_pe == "JW":
            adjustments.append("Rush Tier 2 → Ryan (1.1×) replaces Jacob (1.25×) as lead PE")
            lead_pe = "RO"

    # EIT rotation (alternate Josh/Rajul project-to-project)
    if eit_rotation_hint == "JR" and "JR" in eng_support and "RK" not in eng_support:
        eng_support = ["RK" if m == "JR" else m for m in eng_support]
        adjustments.append("EIT rotation → Rajul takes Josh's slot this round")

    return TeamAssignment(
        tier=tier,
        tier_label=template["label"],
        lead_pe=lead_pe,
        stamp=stamp,
        eng_support=eng_support,
        drafting_lead=drafting_lead,
        drafting_support=drafting_support,
        notes=template.get("notes", ""),
        adjustments=adjustments,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.3 — Hard constraint validation
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ConstraintViolation:
    severity: str            # "RED" | "YELLOW"
    rule: str                # short code
    person: Optional[str]
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {"severity": self.severity, "rule": self.rule, "person": self.person, "detail": self.detail}


def validate_hard_constraints(
    team: TeamAssignment,
    intake: dict[str, Any],
) -> list[ConstraintViolation]:
    """
    Enforce §10.3 hard constraints. Returns a list of violations
    (empty list = pass).
    """
    violations: list[ConstraintViolation] = []
    state = str(intake.get("state") or intake.get("location_region") or "").upper().strip()
    project_type = str(intake.get("project_type") or "").lower()

    # 1) No EITs on Tier 4
    if team.tier == 4:
        for m in team.all_members:
            if m in EITS:
                violations.append(ConstraintViolation(
                    "RED", "no_eit_on_tier_4", m,
                    f"{ROSTER[m]['name']} is an EIT — Tier-4 projects prohibit EIT assignment.",
                ))

    # 2) Mo is oversight-only — only valid as stamp, never as lead/support
    if team.lead_pe == "MK" or "MK" in team.eng_support:
        violations.append(ConstraintViolation(
            "RED", "mo_oversight_only", "MK",
            "Mo Kateeb is oversight-only; do not assign as lead or production engineer.",
        ))

    # 3) Retrofits / Adaptive Reuse must have Nathan as lead PE
    if any(k in project_type for k in ADAPTIVE_REUSE_TYPES) or "historic" in project_type:
        if team.lead_pe != "NK":
            violations.append(ConstraintViolation(
                "RED", "nathan_must_lead_retrofits", team.lead_pe,
                f"Retrofit/Adaptive Reuse project requires Nathan Kline as lead PE; got {team.lead_pe}.",
            ))

    # 4) Out-of-state → Nathan as stamp-of-record
    if state and state not in ("AZ", "CA") and team.stamp != "NK":
        violations.append(ConstraintViolation(
            "YELLOW", "out_of_state_stamping", team.stamp,
            f"Out-of-state project ({state}) should have Nathan Kline as stamp-of-record "
            "pending reciprocal licensure review.",
        ))

    # 5) Tier-4 oversight floor: lead_pe AND stamp must be senior+
    if team.tier == 4:
        if team.lead_pe not in ("NK",):
            violations.append(ConstraintViolation(
                "RED", "tier_4_senior_lead_required", team.lead_pe,
                "Tier-4 projects require Nathan Kline as lead PE.",
            ))
        if team.stamp != "MK":
            violations.append(ConstraintViolation(
                "RED", "tier_4_mo_stamp_required", team.stamp,
                "Tier-4 projects require Mo Kateeb as stamp / final seal review.",
            ))

    # 6) Jacob cannot lead retrofits (explicit §10.3 rule)
    if team.lead_pe == "JW" and (
        any(k in project_type for k in ADAPTIVE_REUSE_TYPES) or "historic" in project_type
    ):
        violations.append(ConstraintViolation(
            "RED", "jacob_no_retrofit_lead", "JW",
            "Jacob Wadman is not authorized to lead retrofit/adaptive reuse projects.",
        ))

    return violations


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.5 — Capacity collision detection
# ─────────────────────────────────────────────────────────────────────────────

PROPOSAL_OUT_WEIGHT = 0.5
INTAKE_PENDING_WEIGHT = 0.0
ACTIVE_WEIGHT = 1.0


def _iso_monday(d: date) -> str:
    """Return the ISO date string for the Monday of d's week."""
    monday = d - timedelta(days=d.weekday())
    return monday.isoformat()


def _weeks_between(start: date, end: date) -> Iterable[str]:
    """Yield ISO-Monday strings for every week that overlaps [start, end]."""
    if end < start:
        return
    cursor = start - timedelta(days=start.weekday())
    end_monday = end - timedelta(days=end.weekday())
    while cursor <= end_monday:
        yield cursor.isoformat()
        cursor += timedelta(days=7)


def _phase_role_hours(weu_hours: float, phase: str) -> dict[str, float]:
    """
    Split a phase's WEU hours across role buckets using §10.7 weights.
    Phase codes from the calendar may be ``SD/DD/CD/CA``; we also accept
    ``50%/75%/90%/IFP`` and map them into CD.
    """
    canonical = _canonicalize_phase(phase)
    weights = ROLE_WEIGHTS_BY_PHASE.get(canonical) or ROLE_WEIGHTS_BY_PHASE["DD"]
    return {role: weu_hours * w for role, w in weights.items()}


def _canonicalize_phase(phase: str) -> str:
    p = (phase or "").upper().strip()
    if p in {"SD"}:                              return "SD"
    if p in {"DD"}:                              return "DD"
    if p in {"CD", "50%", "75%", "90%", "IFP"}:  return "CD"
    if p in {"CA"}:                              return "CA"
    if p in {"INTAKE", "REV", "RFP"}:            return "Intake"
    return "DD"


def _event_role_per_week(event: dict[str, Any]) -> dict[str, dict[str, float]]:
    """
    For one event, distribute its weu_hours uniformly across the weeks it
    spans, allocated by §10.7 role weights.

    Returns {week_iso: {role: hours, ...}, ...}
    """
    try:
        start = date.fromisoformat((event.get("start_date") or "")[:10])
        end   = date.fromisoformat((event.get("end_date")   or "")[:10])
    except ValueError:
        return {}
    if end < start:
        return {}

    weu_hours = float(event.get("weu_hours") or 0.0)
    if weu_hours <= 0:
        return {}

    phase = event.get("phase") or event.get("phase_code") or ""
    role_totals = _phase_role_hours(weu_hours, phase)

    weeks = list(_weeks_between(start, end))
    if not weeks:
        return {}

    per_week_factor = 1.0 / len(weeks)
    out: dict[str, dict[str, float]] = {}
    for w in weeks:
        out[w] = {role: hrs * per_week_factor for role, hrs in role_totals.items()}
    return out


# Mapping from role bucket → preferred ordered list of people who can absorb that role
ROLE_TO_PEOPLE: dict[str, list[str]] = {
    "senior_pe":  ["NK", "MK"],
    "mid_pe":     ["RO", "JW"],
    "eit":        ["JR", "RK"],
    "senior_cad": ["SW", "RS"],
    "cad":        ["JP", "RS"],
}


def _distribute_role_hours_to_people(
    role_hours: dict[str, float],
    team: TeamAssignment,
) -> dict[str, float]:
    """
    Given role-bucket hours for one week and the assigned team, allocate
    those hours to the actually-assigned people. The first preferred
    person from the team takes the role; if multiple, split evenly.
    """
    assigned = set(team.all_members)
    per_person: dict[str, float] = {}
    for role, hrs in role_hours.items():
        if hrs <= 0:
            continue
        candidates = [p for p in ROLE_TO_PEOPLE.get(role, []) if p in assigned]
        if not candidates:
            # If the assigned team doesn't include this role bucket, fall
            # back to the spec's default holder (still record the load).
            candidates = ROLE_TO_PEOPLE.get(role, [])[:1]
        if not candidates:
            continue
        share = hrs / len(candidates)
        for p in candidates:
            per_person[p] = per_person.get(p, 0.0) + share
    return per_person


def build_load_grid(
    events: list[dict[str, Any]],
    team_by_event_id: Optional[dict[str, TeamAssignment]] = None,
) -> dict[str, dict[str, float]]:
    """
    Build a {person: {week_iso_monday: hours}} grid from a list of events.

    Each event must include either ``status`` ('ACTIVE_PROJECT' or
    'PROPOSAL_OUT') or a numeric ``weight``. PROPOSAL_OUT events are
    weighted at 50% per §10.5.

    If the event already has a ``team`` list of initials, those people
    each get an equal share of the role hours. Otherwise, a
    TeamAssignment from team_by_event_id is used (keyed by event id).
    """
    grid: dict[str, dict[str, float]] = {}
    team_by_event_id = team_by_event_id or {}

    for ev in events:
        weight = _event_weight(ev)
        if weight == 0:
            continue

        role_per_week = _event_role_per_week(ev)
        if not role_per_week:
            continue

        # Determine who absorbs the load
        event_team = ev.get("team")
        if isinstance(event_team, list) and event_team:
            assigned = TeamAssignment(
                tier=int(ev.get("tier") or 2),
                tier_label="(from event team)",
                lead_pe=event_team[0],
                stamp=event_team[0],
                eng_support=[m for m in event_team if m in ENGINEERS][1:],
                drafting_lead=next((m for m in event_team if m in DRAFTERS), ""),
                drafting_support=[m for m in event_team if m in DRAFTERS][1:],
            )
        else:
            assigned = team_by_event_id.get(str(ev.get("id") or ev.get("intake_id") or ""))
            if assigned is None:
                continue

        for week, role_hours in role_per_week.items():
            per_person = _distribute_role_hours_to_people(role_hours, assigned)
            for person, hrs in per_person.items():
                weighted = hrs * weight
                grid.setdefault(person, {})
                grid[person][week] = grid[person].get(week, 0.0) + weighted

    return grid


def _event_weight(event: dict[str, Any]) -> float:
    if "weight" in event:
        try:
            return float(event["weight"])
        except (TypeError, ValueError):
            pass
    status = str(event.get("status") or "").upper()
    if status == "ACTIVE_PROJECT" or status == "ACTIVE":
        return ACTIVE_WEIGHT
    if status == "PROPOSAL_OUT":
        return PROPOSAL_OUT_WEIGHT
    if status == "INTAKE_PENDING":
        return INTAKE_PENDING_WEIGHT
    # Unknown status defaults to ACTIVE (back-compat with existing calendar events)
    return ACTIVE_WEIGHT


@dataclass
class Collision:
    week: str            # ISO Monday
    person: str
    projected_hours: float
    cap_hours: float
    severity: str        # RED | YELLOW
    rule: str
    detail: str

    @property
    def utilization_pct(self) -> float:
        return (self.projected_hours / self.cap_hours * 100.0) if self.cap_hours else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "week": self.week,
            "person": self.person,
            "person_name": ROSTER.get(self.person, {}).get("name", self.person),
            "projected_hours": round(self.projected_hours, 1),
            "cap_hours": self.cap_hours,
            "utilization_pct": round(self.utilization_pct, 1),
            "severity": self.severity,
            "rule": self.rule,
            "detail": self.detail,
        }


def detect_collisions(load_grid: dict[str, dict[str, float]]) -> list[Collision]:
    """
    Apply §10.5 severity flags:
      * >100% cap → RED
      * >90% cap  → YELLOW
      * Nathan >28 hrs/wk for 3+ consecutive weeks → RED
      * Mo >8 hrs/wk → RED
    Tier-4 oversight floor (≥ 4 hrs/wk while active) is checked separately
    by ``check_tier4_oversight_floor`` since it needs project metadata.
    """
    collisions: list[Collision] = []

    for person, weekly in load_grid.items():
        cap = float(ROSTER.get(person, {}).get("weekly_cap_hours") or 0)
        if cap == 0:
            continue

        for week, hours in sorted(weekly.items()):
            util = hours / cap if cap else 0.0

            # Mo bandwidth — any breach is RED
            if person == "MK" and hours > cap:
                collisions.append(Collision(
                    week, person, hours, cap, "RED", "mo_bandwidth_breach",
                    f"Mo at {hours:.1f} hrs (cap {cap}). President bandwidth exhausted — re-allocate.",
                ))
                continue

            if util > 1.0:
                collisions.append(Collision(
                    week, person, hours, cap, "RED", "over_cap",
                    f"{ROSTER[person]['name']} at {util*100:.0f}% of weekly cap.",
                ))
            elif util > 0.9:
                collisions.append(Collision(
                    week, person, hours, cap, "YELLOW", "approaching_cap",
                    f"{ROSTER[person]['name']} at {util*100:.0f}% of weekly cap.",
                ))

    # Nathan: 28+ hrs for 3+ consecutive weeks
    nk_weekly = load_grid.get("NK", {})
    sorted_weeks = sorted(nk_weekly.keys())
    streak: list[str] = []
    for w in sorted_weeks:
        if nk_weekly[w] > 28:
            streak.append(w)
            if len(streak) >= 3:
                # Flag the middle/last week of the streak so the user sees one alert
                hrs = nk_weekly[w]
                collisions.append(Collision(
                    w, "NK", hrs, ROSTER["NK"]["weekly_cap_hours"], "RED", "nathan_burnout_3wk",
                    f"Nathan at {hrs:.1f} hrs — 3+ consecutive weeks above 28-hr soft cap. "
                    "Senior PE burnout risk; consider re-scoping or extending an IFP.",
                ))
        else:
            streak = []

    return collisions


def check_tier4_oversight_floor(
    intake: dict[str, Any],
    team: TeamAssignment,
    load_grid: dict[str, dict[str, float]],
) -> list[Collision]:
    """
    §10.3 / §10.5: Tier-4 projects need ≥ 4 hrs/wk of Senior PE oversight
    while active.
    """
    collisions: list[Collision] = []
    if team.tier != 4:
        return collisions
    senior = team.lead_pe  # for Tier 4 this should be NK
    weekly = load_grid.get(senior, {})
    project_weeks = _project_active_weeks(intake)
    for w in project_weeks:
        if weekly.get(w, 0.0) < 4.0:
            collisions.append(Collision(
                w, senior, weekly.get(w, 0.0), ROSTER[senior]["weekly_cap_hours"],
                "RED", "tier_4_oversight_floor_breached",
                f"Tier-4 oversight floor breached: {ROSTER[senior]['name']} has "
                f"{weekly.get(w, 0.0):.1f} hrs in week of {w} (min 4 required while active).",
            ))
    return collisions


def _project_active_weeks(intake: dict[str, Any]) -> list[str]:
    try:
        start = date.fromisoformat(str(intake.get("start_date") or intake.get("proposed_start_date") or "")[:10])
        ifp   = date.fromisoformat(str(intake.get("ifp_date")   or intake.get("ifp_due_date")        or "")[:10])
    except ValueError:
        return []
    return list(_weeks_between(start, ifp))


# ─────────────────────────────────────────────────────────────────────────────
# Applied-mitigation deltas
# ─────────────────────────────────────────────────────────────────────────────

def apply_deltas_to_grid(
    load_grid: dict[str, dict[str, float]],
    applied: list["AppliedMitigation"],
) -> dict[str, dict[str, float]]:
    """
    Return a copy of ``load_grid`` with each active applied mitigation
    folded in: ``hours_delta`` is subtracted from from_person and added
    to to_person for the specified week.

    Only active (non-reverted) mitigations are applied.
    """
    out: dict[str, dict[str, float]] = {p: dict(weeks) for p, weeks in load_grid.items()}
    for m in applied:
        if not m.is_active:
            continue
        out.setdefault(m.from_person, {})
        out.setdefault(m.to_person, {})
        out[m.from_person][m.week] = out[m.from_person].get(m.week, 0.0) - m.hours_delta
        out[m.to_person][m.week]   = out[m.to_person].get(m.week, 0.0)   + m.hours_delta
        # Clamp from_person to non-negative (a delta bigger than the load
        # is treated as zero — the user has effectively over-shifted)
        if out[m.from_person][m.week] < 0:
            out[m.from_person][m.week] = 0.0
    return out


def preview_mitigation_safety(
    load_grid: dict[str, dict[str, float]],
    from_person: str,
    to_person: str,
    week: str,
    hours_delta: float,
) -> dict[str, Any]:
    """
    Before applying a mitigation, check whether the proposed shift would
    push the receiver over their cap (or violate Mo's 8-hr ceiling).

    Returns ``{"safe": bool, "warnings": [str, ...]}``.
    """
    warnings: list[str] = []
    if hours_delta <= 0:
        return {"safe": False, "warnings": ["hours_delta must be > 0"]}

    receiver_cap = float(ROSTER.get(to_person, {}).get("weekly_cap_hours") or 0)
    receiver_current = float(load_grid.get(to_person, {}).get(week, 0.0))
    receiver_after = receiver_current + hours_delta

    if receiver_cap and receiver_after > receiver_cap:
        warnings.append(
            f"{ROSTER[to_person]['name']} would land at "
            f"{receiver_after:.1f}/{receiver_cap:.0f} hrs ({receiver_after/receiver_cap*100:.0f}%) — "
            "shift would just move the bottleneck."
        )

    if to_person == "MK" and receiver_after > 8:
        warnings.append(
            f"Mo would land at {receiver_after:.1f} hrs — over the 8-hr oversight ceiling. "
            "Use a smaller shift or escalate instead."
        )

    sender_current = float(load_grid.get(from_person, {}).get(week, 0.0))
    if sender_current < hours_delta:
        warnings.append(
            f"{ROSTER[from_person]['name']} only has {sender_current:.1f} hrs to give this week."
        )

    return {"safe": len(warnings) == 0, "warnings": warnings}


# ─────────────────────────────────────────────────────────────────────────────
# Section 10.6 — Bottleneck mitigation patterns
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Mitigation:
    rank: int
    pattern: str
    affected_person: str
    week: str
    action: str
    rationale: str
    # Structured fields for direct apply (populated for patterns 1, 2, 3).
    # Patterns 4-6 leave these None/False — they are advisory only.
    from_person: Optional[str] = None
    to_person: Optional[str] = None
    hours_delta: float = 0.0          # hours to move from -> to (always positive)
    applicable: bool = False           # True when this can be one-click applied

    def to_dict(self) -> dict[str, Any]:
        return {
            "rank": self.rank,
            "pattern": self.pattern,
            "affected_person": self.affected_person,
            "affected_person_name": ROSTER.get(self.affected_person, {}).get("name", self.affected_person),
            "week": self.week,
            "action": self.action,
            "rationale": self.rationale,
            "from_person": self.from_person,
            "to_person": self.to_person,
            "from_person_name": ROSTER.get(self.from_person, {}).get("name", self.from_person) if self.from_person else None,
            "to_person_name":   ROSTER.get(self.to_person, {}).get("name", self.to_person)     if self.to_person   else None,
            "hours_delta": round(self.hours_delta, 1),
            "applicable": self.applicable,
        }


@dataclass
class AppliedMitigation:
    """
    A recorded mitigation applied to the load grid.
    Reversible — set ``reverted_at`` to soft-delete.
    """
    id: Optional[int]
    pattern: str
    week: str
    from_person: str
    to_person: str
    hours_delta: float
    rationale: str
    applied_by: str = ""
    applied_at: str = ""
    reverted_at: Optional[str] = None

    @property
    def is_active(self) -> bool:
        return not self.reverted_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "pattern": self.pattern,
            "week": self.week,
            "from_person": self.from_person,
            "to_person":   self.to_person,
            "from_person_name": ROSTER.get(self.from_person, {}).get("name", self.from_person),
            "to_person_name":   ROSTER.get(self.to_person,   {}).get("name", self.to_person),
            "hours_delta": round(self.hours_delta, 1),
            "rationale": self.rationale,
            "applied_by": self.applied_by,
            "applied_at": self.applied_at,
            "reverted_at": self.reverted_at,
            "is_active": self.is_active,
        }


def suggest_mitigations(
    collisions: list[Collision],
    load_grid: dict[str, dict[str, float]],
) -> list[Mitigation]:
    """
    §10.6 patterns applied in priority order:
      1. Shift PM/admin from saturated Senior PEs to under-utilized Mid PEs.
      2. Pull Randall into production drafting to relieve Scott.
      3. Pull Mo earlier (architectural review pass) to relieve Nathan.
      4. Front-load EITs in SD windows of large projects.
      5. Re-sequence within the Compressed/Standard schedule envelope.
      6. ESCALATE TO MO with a re-scoping / extension request.
    """
    mitigations: list[Mitigation] = []
    seen_weeks: set[tuple[str, str]] = set()  # (pattern, week)

    def slack(person: str, week: str) -> float:
        cap = ROSTER.get(person, {}).get("weekly_cap_hours") or 0
        used = load_grid.get(person, {}).get(week, 0.0)
        return max(cap - used, 0.0)

    for c in collisions:
        # Pattern 1 — Senior PE saturated → shift PM admin to Mid PE
        if c.person in {"NK", "RO"} and c.severity in {"RED", "YELLOW"}:
            target = "JW" if c.person == "RO" else "RO"
            available_slack = slack(target, c.week)
            if available_slack >= 4 and ("pat1", c.week) not in seen_weeks:
                hours = min(6.0, available_slack)
                mitigations.append(Mitigation(
                    rank=1, pattern="shift_pm_admin",
                    affected_person=c.person, week=c.week,
                    action=f"Move ~{hours:.0f} hrs of coord/PM work from {ROSTER[c.person]['name']} to {ROSTER[target]['name']}",
                    rationale=f"{ROSTER[target]['name']} has {available_slack:.0f} hrs slack this week.",
                    from_person=c.person, to_person=target, hours_delta=hours, applicable=True,
                ))
                seen_weeks.add(("pat1", c.week))

        # Pattern 2 — Drafting bottleneck → pull Randall into production
        if c.person == "SW" and slack("RS", c.week) >= 4 and ("pat2", c.week) not in seen_weeks:
            available_slack = slack("RS", c.week)
            hours = min(6.0, available_slack)
            mitigations.append(Mitigation(
                rank=2, pattern="pull_randall_into_production",
                affected_person=c.person, week=c.week,
                action=f"Shift ~{hours:.0f} hrs of production sheet workload from Scott Webb to Randall Smith",
                rationale=f"Randall is approved for production sheets (§10.6) and has {available_slack:.0f} hrs slack.",
                from_person="SW", to_person="RS", hours_delta=hours, applicable=True,
            ))
            seen_weeks.add(("pat2", c.week))

        # Pattern 3 — Nathan saturated → pull Mo early for review pass
        if c.person == "NK" and slack("MK", c.week) >= 2 and ("pat3", c.week) not in seen_weeks:
            available_slack = slack("MK", c.week)
            hours = min(3.0, available_slack)  # Mo's bandwidth is precious — small shifts only
            mitigations.append(Mitigation(
                rank=3, pattern="pull_mo_early",
                affected_person="NK", week=c.week,
                action=f"Pull Mo Kateeb in {hours:.0f} hrs early for an architectural review pass / calc spot-check",
                rationale=f"Mo has {available_slack:.1f} hrs of oversight bandwidth this week.",
                from_person="NK", to_person="MK", hours_delta=hours, applicable=True,
            ))
            seen_weeks.add(("pat3", c.week))

    # Pattern 6 — Fallback escalation if any RED remains unmitigated
    if any(c.severity == "RED" for c in collisions) and not mitigations:
        mitigations.append(Mitigation(
            rank=6, pattern="escalate_to_mo",
            affected_person="MK", week=collisions[0].week if collisions else "",
            action="ESCALATE: re-scope or request schedule extension from client",
            rationale="No internal mitigation absorbs the overcapacity; do not silently absorb (§10.6).",
            applicable=False,  # advisory only
        ))

    mitigations.sort(key=lambda m: (m.rank, m.week))
    return mitigations


# ─────────────────────────────────────────────────────────────────────────────
# Convenience wrapper used by routes / scripts
# ─────────────────────────────────────────────────────────────────────────────

def analyze_project(
    intake: dict[str, Any],
    *,
    pipeline_events: Optional[list[dict[str, Any]]] = None,
    active_events: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    One-shot helper that derives the tier, applies the template, validates
    constraints, and builds a collision report against the current pipeline.

    Returns a dict suitable for serializing into the /api/capacity response
    or storing on the intake as a staffing decision audit trail.
    """
    tier = derive_complexity_tier(intake)
    team = assign_tier_template(
        tier,
        schedule_tier=str(intake.get("schedule_tier") or "Standard"),
        project_type=str(intake.get("project_type") or ""),
        state=str(intake.get("state") or intake.get("location_region") or ""),
    )
    constraint_violations = validate_hard_constraints(team, intake)

    # Combine the project under analysis with the pipeline + active events
    self_event = _intake_to_event(intake, team)
    all_events: list[dict[str, Any]] = []
    if self_event:
        all_events.append(self_event)
    all_events.extend(pipeline_events or [])
    all_events.extend(active_events or [])

    load_grid = build_load_grid(all_events)
    collisions = detect_collisions(load_grid)
    collisions.extend(check_tier4_oversight_floor(intake, team, load_grid))
    mitigations = suggest_mitigations(collisions, load_grid)

    return {
        "tier": tier,
        "team": team.to_dict(),
        "constraint_violations": [v.to_dict() for v in constraint_violations],
        "load_grid": load_grid,
        "collisions": [c.to_dict() for c in collisions],
        "mitigations": [m.to_dict() for m in mitigations],
    }


def _intake_to_event(intake: dict[str, Any], team: TeamAssignment) -> Optional[dict[str, Any]]:
    """Build a synthetic event for the intake under analysis (used by analyze_project)."""
    start = intake.get("start_date") or intake.get("proposed_start_date")
    ifp   = intake.get("ifp_date")   or intake.get("ifp_due_date")
    if not start or not ifp:
        return None
    weu_hours = float(intake.get("weu_hours") or intake.get("total_weus") or 0.0)
    if weu_hours <= 0:
        return None
    return {
        "id": intake.get("id") or "self",
        "phase": "DD",                       # midpoint phase for whole-project load
        "tier": team.tier,
        "team": team.all_members,
        "weu_hours": weu_hours,
        "start_date": f"{str(start)[:10]}T00:00:00Z",
        "end_date":   f"{str(ifp)[:10]}T23:59:59Z",
        "status": "PROPOSAL_OUT" if str(intake.get("status") or "").upper() == "PROPOSAL_OUT" else "ACTIVE_PROJECT",
    }
