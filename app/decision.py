from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


SEVERITY_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

_DOC_CLARIFICATION_QUESTIONS: dict[str, str] = {
    "Geotechnical report": (
        "Can you provide the geotechnical report, or confirm when it will be available? "
        "This is required before we can finalize foundation design."
    ),
    "Grading plan": (
        "Can you provide the grading/civil plan, or confirm who is producing it and the expected delivery date?"
    ),
    "Architectural drawings (schematic or better)": (
        "Can you share the current architectural drawings (schematic or better)? "
        "We need these to define structural scope."
    ),
    "Existing structural drawings": (
        "Do you have the existing structural drawings on hand? If not, have you checked county records or the original permit? "
        "This affects our ability to assess the existing system."
    ),
    "Site visit photos / survey": (
        "Can you provide site visit photos or a recent survey? "
        "Without these we'll need to schedule a site visit before proceeding."
    ),
    "Architectural program / RFP": (
        "Can you share the architectural program or RFP? "
        "We need this to understand scope and deliverables."
    ),
    "Site plan with address/coordinates": (
        "Can you provide a site plan with the project address or coordinates? "
        "This is needed for code research and permit applications."
    ),
    "Preliminary schedule/timeline": (
        "Can you share a preliminary schedule or target milestone dates? "
        "This helps us confirm our availability and set delivery expectations."
    ),
}


@dataclass(frozen=True)
class RedFlag:
    key: str
    title: str
    severity: str  # low|medium|high|critical
    category: str
    detail: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "title": self.title,
            "severity": self.severity,
            "category": self.category,
            "detail": self.detail,
        }


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        value_str = str(value).strip()
        if not value_str:
            return None
        return int(value_str)
    except Exception:
        return None


def complexity_estimate(answers: dict[str, Any]) -> str:
    """
    Tier scoring:  0 = low  |  1 = medium  |  2 = high

    1. Building type sets the base tier.
    2. High-risk scope types force HIGH regardless.
    3. Each qualifying factor bumps the tier up by one (capped at HIGH).
    """
    _TIER_NAMES = ["low", "medium", "high"]

    building_type    = (answers.get("building_type")             or "").strip()
    scope_risk_type  = (answers.get("scope_risk_type")           or "").strip()
    scope_def        = (answers.get("scope_definition")          or "").strip()
    scope_creep      = (answers.get("scope_creep_likelihood")    or "").strip()
    schedule         = (answers.get("schedule_realism")          or "").strip()
    arch_status      = (answers.get("architect_status")          or "").strip()
    project_type     = (answers.get("project_type")              or "").strip()
    primary_material = (answers.get("primary_structural_material") or "").strip()
    has_drawings     = bool(answers.get("doc_existing_struct_drawings"))

    # 1. Base tier from building type
    if building_type in {"healthcare", "data_center"}:
        tier = 2
    elif building_type in {"mixed_use", "education"}:
        tier = 1
    else:                                           # retail, warehouse, other, unknown
        tier = 0

    # 2. Scope risk type forces HIGH immediately
    if scope_risk_type in {"ti_high_liability", "adaptive_reuse"}:
        tier = max(tier, 2)

    # 3. Structural complexity adjustments
    #    Critical healthcare TI (dialysis, surgery, life-safety): force HIGH
    if building_type == "healthcare" and scope_risk_type == "ti_high_liability":
        tier = 2

    # 3. Additional upward factors — each adds one tier, capped at HIGH
    _existing_building = project_type in {
        "build_to_suit_retrofit", "tenant_improvement",
        "addition_expansion", "one_off_unique",
    }
    _masonry_existing = (
        primary_material in {"masonry_cmu", "mixed"} and _existing_building
    )
    factors = [
        scope_def    in {"undefined", "partial"},
        scope_creep  == "likely",
        schedule     in {"compressed", "unrealistic"},
        arch_status  in {"new", "not_identified"},
        _existing_building and not has_drawings,    # missing existing drawings
        _masonry_existing,                          # masonry existing building (URM risk)
    ]
    for triggered in factors:
        if triggered:
            tier = min(tier + 1, 2)

    return _TIER_NAMES[tier]


def fee_range_estimate(answers: dict[str, Any]) -> Optional[str]:
    """
    Quick baseline estimate (base $/SF only, no multipliers).
    Uses the same rate card as RiskAdjustedFeeEstimator.
    Shown as a fast read in the summary card; the detailed panel adds multipliers.
    """
    from .fee_estimator import _RATE_CARD, _DELIVERY_BUCKET, _floor_fee

    sf = _as_int(answers.get("approx_sf"))
    if not sf or sf <= 0:
        return None

    pt = (answers.get("project_type") or "").strip()
    bt = (answers.get("building_type") or "other").strip().lower()

    delivery = _DELIVERY_BUCKET.get(pt)
    if not delivery:
        return None   # unknown project type — skip rather than show bad number

    bucket  = _RATE_CARD.get(delivery, _RATE_CARD["ti"])
    rate_lo, rate_hi = bucket.get(bt, bucket["other"])
    low  = max(sf * rate_lo, _floor_fee(bt, delivery))
    high = sf * rate_hi
    if high < low:
        high = low

    return f"${low:,.0f} \u2013 ${high:,.0f}"


def compute_decision(answers: dict[str, Any]) -> dict[str, Any]:
    """
    Converts checklist answers into:
      - red_flags: list[{key,title,severity,category,detail}]
      - counts: {critical, high, medium, low, total}
      - recommendation: PROCEED_TO_PROPOSAL | NEEDS_MO_REVIEW | CLARIFY_FIRST | LIKELY_DECLINE
      - reason: short explanation
    """
    red_flags: list[RedFlag] = []
    needs_clarification_reasons: list[str] = []
    soft_blockers: list[dict[str, str]] = []

    project_type = (answers.get("project_type") or "").strip()
    building_type = (answers.get("building_type") or "").strip()

    scope_definition = answers.get("scope_definition")
    if scope_definition in {"undefined", "evolving"}:
        red_flags.append(
            RedFlag(
                key="scope_undefined",
                title="Scope is undefined / evolving",
                severity="high",
                category="Scope",
                detail="Scope not locked in writing; likely to change.",
            )
        )
    elif scope_definition in {"unknown", "", None}:
        needs_clarification_reasons.append("Scope definition is unknown.")

    scope_risk_type = answers.get("scope_risk_type")
    if scope_risk_type == "ti_high_liability":
        specialist_support = answers.get("specialist_support")
        if specialist_support == "no":
            severity = "critical"
            detail = "High-liability TI with no specialist support."
        else:
            severity = "high"
            detail = "High-liability TI; Mo approval required."
        red_flags.append(
            RedFlag(
                key="scope_ti_high_liability",
                title="TI with high liability exposure",
                severity=severity,
                category="Scope",
                detail=detail,
            )
        )
    elif scope_risk_type == "adaptive_reuse":
        red_flags.append(
            RedFlag(
                key="scope_adaptive_reuse",
                title="Adaptive reuse / historic building",
                severity="high",
                category="Scope",
                detail="Specialized code navigation / higher uncertainty.",
            )
        )
    elif scope_risk_type == "government_ahj":
        red_flags.append(
            RedFlag(
                key="scope_government_ahj",
                title="Government / AHJ-intensive project",
                severity="high",
                category="Scope",
                detail="Multiple agency reviews; typically longer timelines.",
            )
        )

    scope_creep = answers.get("scope_creep_likelihood")
    if scope_creep == "likely":
        red_flags.append(
            RedFlag(
                key="scope_creep_likely",
                title="Scope creep likely",
                severity="high",
                category="Scope",
                detail='Vague language like "all structural work required" suggests scope expansion.',
            )
        )
    elif scope_creep in {"unknown", "", None}:
        # Not always required; treat as a note only if scope isn't defined.
        if scope_definition in {"unknown", "", None}:
            needs_clarification_reasons.append("Scope creep likelihood is unknown.")

    # Timeline
    schedule_realism = answers.get("schedule_realism")
    if schedule_realism == "compressed":
        red_flags.append(
            RedFlag(
                key="timeline_compressed",
                title="Compressed schedule",
                severity="high",
                category="Timeline",
                detail="May require overtime; Mo approval recommended; may warrant rush fee.",
            )
        )
    elif schedule_realism == "unrealistic":
        # Decision matrix highlights unrealistic timeline (esp. < 2 weeks) as critical.
        red_flags.append(
            RedFlag(
                key="timeline_unrealistic",
                title="Unrealistic timeline",
                severity="critical",
                category="Timeline",
                detail="Timeline cannot be met without cutting corners on QC or scope.",
            )
        )
    elif schedule_realism in {"unknown", "", None}:
        needs_clarification_reasons.append("Schedule realism is unknown.")

    permit_weeks = _as_int(answers.get("weeks_to_permit_submission"))
    if permit_weeks is not None and permit_weeks < 2:
        red_flags.append(
            RedFlag(
                key="timeline_less_than_2_weeks",
                title="Permit submission needed in < 2 weeks",
                severity="critical",
                category="Timeline",
                detail="Hard stop timeline called out as a likely-decline condition.",
            )
        )

    hard_stops: list[str] = answers.get("hard_stop_deadlines") or []
    if isinstance(hard_stops, str):
        hard_stops = [hard_stops]
    if any(hard_stops):
        red_flags.append(
            RedFlag(
                key="timeline_hard_stop",
                title="Hard-stop deadline dependency",
                severity="high",
                category="Timeline",
                detail="Permit board / GC mobilization / lender deadline depends on deliverables.",
            )
        )

    # Site access
    site_access = answers.get("site_access")
    if site_access == "no":
        red_flags.append(
            RedFlag(
                key="site_no_access",
                title="Existing building with no site access",
                severity="critical",
                category="Site/Docs",
                detail="No visual inspection increases design risk significantly; Mo decision required.",
            )
        )
    elif site_access in {"uncertain", "unknown", "", None}:
        # Only matters if project is not clearly new construction.
        if project_type and project_type not in {"new_construction"}:
            needs_clarification_reasons.append("Site access is uncertain for an existing building.")

    # Docs checks (core)
    def doc_bool(key: str) -> bool:
        return bool(answers.get(key) is True)

    is_new_construction = project_type == "new_construction"
    is_existing_building = project_type in {
        "build_to_suit_retrofit",
        "tenant_improvement",
        "addition_expansion",
        "one_off_unique",
        "unknown",
        "",
        None,
    }

    missing_docs: list[str] = []
    if is_new_construction:
        for key, label in [
            ("doc_geotech", "Geotechnical report"),
            ("doc_grading_plan", "Grading plan"),
            ("doc_arch_drawings", "Architectural drawings (schematic or better)"),
        ]:
            if not doc_bool(key):
                missing_docs.append(label)
    elif is_existing_building:
        for key, label in [
            ("doc_existing_struct_drawings", "Existing structural drawings"),
            ("doc_site_photos", "Site visit photos / survey"),
        ]:
            if not doc_bool(key):
                missing_docs.append(label)

    for key, label in [
        ("doc_rfp_program", "Architectural program / RFP"),
        ("doc_site_plan", "Site plan with address/coordinates"),
        ("doc_prelim_schedule", "Preliminary schedule/timeline"),
    ]:
        if not doc_bool(key):
            missing_docs.append(label)

    # Build soft blockers — one clarification question per missing document.
    # These are advisory prompts for the client, not disqualifying flags.
    for doc_label in missing_docs:
        soft_blockers.append({
            "doc": doc_label,
            "question": _DOC_CLARIFICATION_QUESTIONS.get(
                doc_label,
                f"Can you provide the {doc_label.lower()}? It is required before we can proceed.",
            ),
        })

    docs_commitment = answers.get("docs_commitment")  # yes|no|unknown
    if missing_docs:
        if docs_commitment == "no":
            # Client declined docs — flag as high (not critical); soft blockers carry the detail.
            red_flags.append(
                RedFlag(
                    key="docs_refused",
                    title="Client declined to provide required documentation",
                    severity="high",
                    category="Site/Docs",
                    detail=(
                        f"Missing: {', '.join(missing_docs[:3])}{'...' if len(missing_docs) > 3 else ''}. "
                        "Client indicated documents will not be provided — clarification required."
                    ),
                )
            )
        elif docs_commitment in {"unknown", "", None}:
            needs_clarification_reasons.append(
                "Missing required documentation and commitment to provide is unclear."
            )
        # docs expected but missing → handled via soft_blockers, not a blocking red flag

    # Architect/client
    architect_status = answers.get("architect_status")  # known_good|known_fair|new|unknown|not_identified|inhouse
    architect_responsive = answers.get("architect_responsiveness")  # responsive|unresponsive|unknown
    if architect_status == "not_identified":
        needs_clarification_reasons.append("Architect is not identified yet.")
    if architect_responsive == "unresponsive":
        red_flags.append(
            RedFlag(
                key="architect_unresponsive",
                title="Architect unresponsive",
                severity="high",
                category="Client/Architect",
                detail="Communication risk; track record of delays/issues.",
            )
        )
    elif architect_status in {"new", "unknown"}:
        red_flags.append(
            RedFlag(
                key="architect_unknown",
                title="Architect is new/unknown",
                severity="medium",
                category="Client/Architect",
                detail="No track record yet; higher coordination risk.",
            )
        )

    decision_maker = answers.get("decision_maker_clarity")
    # Accept both short form ("unclear", "none") and AI-extracted long form ("unclear_red_flag")
    if decision_maker in {"unclear", "none", "unclear_red_flag"}:
        red_flags.append(
            RedFlag(
                key="decision_maker_unclear",
                title="No clear decision-maker",
                severity="medium",
                category="Client/Architect",
                detail="Request clarification before proceeding to avoid chaotic scope/timeline decisions.",
            )
        )
        needs_clarification_reasons.append("Need a single decision-maker / primary contact.")

    # Specialized building type comfort
    if building_type in {"healthcare", "data_center"}:
        building_experience = answers.get("building_type_experience")  # yes|no|unknown
        if building_experience == "no":
            red_flags.append(
                RedFlag(
                    key="building_type_experience_missing",
                    title="Limited recent experience with building type",
                    severity="high",
                    category="Scope",
                    detail="Healthcare/data center may require specialized expertise; Mo review recommended.",
                )
            )
        elif building_experience in {"unknown", "", None}:
            needs_clarification_reasons.append(
                "Need to confirm internal comfort/experience with this building type."
            )

    # Capacity
    capacity = answers.get("capacity_available")  # yes|no|unknown
    if capacity == "no":
        red_flags.append(
            RedFlag(
                key="capacity_constraint",
                title="Capacity constraint",
                severity="high",
                category="Internal",
                detail="No available bandwidth to resource the project adequately.",
            )
        )

    # Quick flags (optional)
    quick_flags = answers.get("quick_flags") or []
    if isinstance(quick_flags, str):
        quick_flags = [quick_flags]
    quick_map = {
        "quick_scope_unclear": ("Scope is unclear / will evolve", "high", "Scope"),
        "quick_ti_high_liability": ("TI + high liability (medical/critical)", "high", "Scope"),
        "quick_historic_adaptive_reuse": ("Historic building / adaptive reuse", "high", "Scope"),
        "quick_schedule_compressed": ("Schedule compressed or unrealistic", "high", "Timeline"),
        "quick_hard_stop_deadline": ("Hard-stop deadline", "high", "Timeline"),
        "quick_no_site_access": ("Existing building + no site access", "critical", "Site/Docs"),
        "quick_missing_geotech_or_drawings": ("Missing geotech or existing drawings", "high", "Site/Docs"),
        "quick_architect_unresponsive": ("Architect unresponsive or unproven", "high", "Client/Architect"),
        "quick_no_decision_maker": ("No clear decision-maker", "medium", "Client/Architect"),
    }
    for q in quick_flags:
        if q in quick_map:
            title, severity, category = quick_map[q]
            red_flags.append(
                RedFlag(
                    key=q,
                    title=title,
                    severity=severity,
                    category=category,
                    detail="Flagged during quick screening.",
                )
            )

    # Deduplicate by key (keeping highest severity if repeated)
    by_key: dict[str, RedFlag] = {}
    for flag in red_flags:
        existing = by_key.get(flag.key)
        if not existing:
            by_key[flag.key] = flag
        else:
            if SEVERITY_ORDER[flag.severity] > SEVERITY_ORDER[existing.severity]:
                by_key[flag.key] = flag

    red_flags_deduped = list(by_key.values())

    counts = {"critical": 0, "high": 0, "medium": 0, "low": 0, "total": 0}
    for flag in red_flags_deduped:
        counts[flag.severity] += 1
        counts["total"] += 1

    critical_count = counts["critical"]
    total_red_flags = counts["total"]

    if critical_count > 0:
        recommendation = "LIKELY_DECLINE"
        reason = "Critical red flag(s) present; likely decline unless Mo overrides."
    elif needs_clarification_reasons:
        recommendation = "CLARIFY_FIRST"
        reason = "Missing critical information: " + "; ".join(needs_clarification_reasons[:3])
    elif total_red_flags == 0:
        recommendation = "PROCEED_TO_PROPOSAL"
        reason = "No red flags detected."
    elif total_red_flags <= 2:
        recommendation = "NEEDS_MO_REVIEW"
        reason = "1–2 red flags detected; Mo should review before proposal."
    else:
        recommendation = "NEEDS_MO_REVIEW"
        reason = "3+ red flags detected (cumulative risk); Mo review required (likely decline)."

    fast_track = (
        project_type == "repeating_program"
        and (answers.get("architect_status") or "").strip() == "known_good"
        and (answers.get("scope_definition") or "").strip() == "defined"
        and len(red_flags_deduped) == 0
    )

    return {
        "red_flags": [f.as_dict() for f in red_flags_deduped],
        "counts": counts,
        "recommendation": recommendation,
        "reason": reason,
        "missing_docs": missing_docs,
        "soft_blockers": soft_blockers,
        "needs_clarification_reasons": needs_clarification_reasons,
        "complexity_estimate": complexity_estimate(answers),
        "fee_range_estimate": fee_range_estimate(answers),
        "fast_track": fast_track,
    }
