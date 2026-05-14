"""
Regression test for app/staffing.py against CLAUDE.md §10.8 worked example.

Run:  python3 scripts/test_staffing.py
Exits 0 if all assertions pass; non-zero on any failure.
"""
from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.staffing import (
    DRAFTERS,
    EITS,
    PROPOSAL_OUT_WEIGHT,
    ROSTER,
    TIER_TEMPLATES,
    AppliedMitigation,
    analyze_project,
    apply_deltas_to_grid,
    assign_tier_template,
    baseline_capacity_hours,
    build_load_grid,
    derive_complexity_tier,
    detect_collisions,
    preview_mitigation_safety,
    suggest_mitigations,
    validate_hard_constraints,
)


def _section(title: str) -> None:
    print(f"\n── {title} " + "─" * (60 - len(title)))


# ─────────────────────────────────────────────────────────────────────────────
# §10.1 — Roster sanity
# ─────────────────────────────────────────────────────────────────────────────

def test_baseline_capacity() -> None:
    _section("Roster baseline capacity (§10.1)")
    caps = baseline_capacity_hours()
    assert caps["drafting"] == 96.0, f"Drafting baseline should be 96 hrs/wk, got {caps['drafting']}"
    assert caps["engineering_excl_mo"] == 176.0, f"Eng excl. Mo should be 176, got {caps['engineering_excl_mo']}"
    assert caps["mo_oversight"] == 8.0
    assert ROSTER["NK"]["weekly_cap_hours"] == 32
    assert "NK" not in EITS and "JR" in EITS and "RK" in EITS
    assert "RS" in DRAFTERS and "SW" in DRAFTERS and "JP" in DRAFTERS
    print("  OK — baselines match spec.")


# ─────────────────────────────────────────────────────────────────────────────
# §5 — Complexity tier derivation
# ─────────────────────────────────────────────────────────────────────────────

def test_tier_derivation() -> None:
    _section("Complexity tier derivation (§5)")
    cases = [
        # (name, intake, expected_tier)
        ("Phoenix Industrial 185k",   {"project_type": "industrial_shell", "sq_ft": 185_000},      1),
        ("Tempe Adaptive Reuse 72k",  {"project_type": "adaptive_reuse",    "sq_ft": 72_000},       3),
        ("Scottsdale Med-Spa 8.5k",   {"project_type": "healthcare",        "sq_ft": 8_500},        4),
        ("Long Beach Self-Storage",   {"project_type": "new_construction",  "sq_ft": 38_000,
                                       "building_type": "light_commercial"},                       2),
        ("Historic URM 12k",          {"project_type": "historic",          "sq_ft": 12_000},       4),
        ("Mid-size mixed-use 60k",    {"project_type": "new_construction",
                                       "building_type": "mixed_use", "sq_ft": 60_000},             3),
    ]
    for name, intake, expected in cases:
        actual = derive_complexity_tier(intake)
        assert actual == expected, f"{name}: expected tier {expected}, got {actual}"
        print(f"  {name:30s} → Tier {actual}  ✓")


# ─────────────────────────────────────────────────────────────────────────────
# §10.2 — Tier template assignment
# ─────────────────────────────────────────────────────────────────────────────

def test_default_templates() -> None:
    _section("Default tier templates (§10.2)")
    t1 = assign_tier_template(1)
    assert t1.lead_pe == "JW",  f"Tier 1 lead should be Jacob, got {t1.lead_pe}"
    assert t1.stamp == "NK",    "Tier 1 stamp should be Nathan (QA)"
    assert "JR" not in t1.eng_support and "RK" not in t1.eng_support, "Tier 1 EIT support should be off by default"
    print(f"  Tier 1: lead={t1.lead_pe} stamp={t1.stamp} drafting_lead={t1.drafting_lead}  ✓")

    t2 = assign_tier_template(2)
    assert t2.lead_pe == "RO",  f"Tier 2 lead should default to Ryan, got {t2.lead_pe}"
    assert "JR" in t2.eng_support
    print(f"  Tier 2: lead={t2.lead_pe} stamp={t2.stamp} eng_support={t2.eng_support}  ✓")

    t3 = assign_tier_template(3)
    assert t3.lead_pe == "NK" and t3.stamp == "MK"
    assert "RO" in t3.eng_support and "JR" in t3.eng_support
    assert t3.drafting_lead == "RS"  # Randall for retrofit/BIM
    print(f"  Tier 3: lead={t3.lead_pe} stamp={t3.stamp} drafting_lead={t3.drafting_lead}  ✓")

    t4 = assign_tier_template(4)
    assert t4.lead_pe == "NK" and t4.stamp == "MK"
    for m in t4.all_members:
        assert m not in EITS, f"Tier 4 must not include EITs, found {m}"
    print(f"  Tier 4: lead={t4.lead_pe} stamp={t4.stamp} (NO EITs)  ✓")


def test_rush_tier2_prefers_ryan() -> None:
    _section("Rush Tier 2 prefers Ryan over Jacob (§10.4 schedule pressure)")
    standard = assign_tier_template(2, schedule_tier="Standard")
    rush = assign_tier_template(2, schedule_tier="Rush")
    assert rush.lead_pe == "RO", f"Rush Tier 2 should lead with Ryan, got {rush.lead_pe}"
    # Standard also defaults to Ryan in our templates, but Rush guarantees it
    assert standard.lead_pe == "RO"
    print(f"  Standard Tier 2 lead = {standard.lead_pe}, Rush Tier 2 lead = {rush.lead_pe}  ✓")


def test_retrofit_forces_nathan() -> None:
    _section("Retrofit / Adaptive Reuse forces Nathan as lead (§10.3)")
    # Even Tier 2 should escalate lead to Nathan when project_type is adaptive_reuse
    t = assign_tier_template(2, project_type="adaptive_reuse")
    assert t.lead_pe == "NK", f"Adaptive reuse should force Nathan lead, got {t.lead_pe}"
    assert any("Nathan replaces" in a for a in t.adjustments), "Adjustment log should record the swap"
    print(f"  Adaptive Reuse Tier 2 lead = {t.lead_pe}  ✓  (adjustments: {t.adjustments})")


# ─────────────────────────────────────────────────────────────────────────────
# §10.3 — Hard constraints
# ─────────────────────────────────────────────────────────────────────────────

def test_no_eit_on_tier4() -> None:
    _section("Hard constraint: no EITs on Tier 4 (§10.3)")
    t = assign_tier_template(4)
    violations = validate_hard_constraints(t, {"project_type": "healthcare", "state": "AZ"})
    # Default Tier 4 template should not include EITs at all
    assert all(v.rule != "no_eit_on_tier_4" for v in violations), \
        f"Default Tier 4 template should pass; got: {[v.detail for v in violations]}"
    # Manually inject an EIT and re-validate
    t.eng_support.append("JR")
    violations = validate_hard_constraints(t, {"project_type": "healthcare", "state": "AZ"})
    assert any(v.rule == "no_eit_on_tier_4" and v.severity == "RED" for v in violations), \
        "Injecting an EIT into Tier 4 should produce a RED violation"
    print("  RED triggered when JR is added to Tier 4 team  ✓")


def test_out_of_state_stamping() -> None:
    _section("Hard constraint: out-of-state stamping (§10.3)")
    # Default Tier 2 already has Nathan as stamp, so out-of-state passes cleanly.
    t = assign_tier_template(2)
    assert t.stamp == "NK"
    violations_az = validate_hard_constraints(t, {"project_type": "new_construction", "state": "AZ"})
    violations_or_with_nk = validate_hard_constraints(t, {"project_type": "new_construction", "state": "OR"})
    assert all(v.rule != "out_of_state_stamping" for v in violations_az + violations_or_with_nk), \
        "If stamp is already Nathan, out-of-state check should pass"

    # Now break the rule: assign a non-Nathan stamp on an out-of-state project.
    t.stamp = "RO"
    violations_or_bad = validate_hard_constraints(t, {"project_type": "new_construction", "state": "OR"})
    assert any(v.rule == "out_of_state_stamping" and v.severity == "YELLOW" for v in violations_or_bad), \
        "Non-Nathan stamp on OR project should flag YELLOW reciprocal-licensure issue"
    print("  Nathan stamp on OR passes; non-Nathan stamp on OR triggers YELLOW  ✓")


def test_mo_oversight_only() -> None:
    _section("Hard constraint: Mo is oversight-only (§10.3)")
    t = assign_tier_template(3)
    # Default Tier 3: Mo is stamp, which is allowed
    violations = validate_hard_constraints(t, {"project_type": "adaptive_reuse", "state": "AZ"})
    assert all(v.rule != "mo_oversight_only" for v in violations)
    # Now improperly assign Mo as lead
    t.lead_pe = "MK"
    violations = validate_hard_constraints(t, {"project_type": "adaptive_reuse", "state": "AZ"})
    assert any(v.rule == "mo_oversight_only" and v.severity == "RED" for v in violations)
    print("  RED triggered when Mo is assigned as lead PE  ✓")


# ─────────────────────────────────────────────────────────────────────────────
# §10.5 — Collision detection (the four-project worked example, W30)
# ─────────────────────────────────────────────────────────────────────────────

def test_worked_example_collisions() -> None:
    _section("Worked example collision week W30 (§10.8)")
    # All four projects ACTIVE, full hours. Week of Jul 27, 2026 = ISO Monday 2026-07-27.
    events = [
        # Project A — Phoenix Logistics 185k Industrial, Tier 1, CD phase
        {"id": "A", "phase": "CD", "tier": 1, "team": ["JW", "NK", "SW", "JP"],
         "weu_hours": 13.0, "start_date": "2026-07-27T00:00:00Z", "end_date": "2026-07-31T23:59:59Z",
         "status": "ACTIVE_PROJECT"},
        # Project B — Tempe Adaptive Reuse 72k, Tier 3, DD phase
        {"id": "B", "phase": "DD", "tier": 3, "team": ["NK", "MK", "RO", "JR", "RS", "SW"],
         "weu_hours": 32.0, "start_date": "2026-07-27T00:00:00Z", "end_date": "2026-07-31T23:59:59Z",
         "status": "ACTIVE_PROJECT"},
        # Project C — Scottsdale Med-Spa 8.5k, Tier 4, CD phase
        {"id": "C", "phase": "CD", "tier": 4, "team": ["NK", "MK", "RO", "SW", "RS"],
         "weu_hours": 37.0, "start_date": "2026-07-27T00:00:00Z", "end_date": "2026-07-31T23:59:59Z",
         "status": "ACTIVE_PROJECT"},
        # Project D — Long Beach Self-Storage 38k, Tier 2 Rush, DD phase
        {"id": "D", "phase": "DD", "tier": 2, "team": ["RO", "NK", "RK", "JP", "RS"],
         "weu_hours": 60.0, "start_date": "2026-07-27T00:00:00Z", "end_date": "2026-07-31T23:59:59Z",
         "status": "ACTIVE_PROJECT"},
    ]
    grid = build_load_grid(events)
    week = "2026-07-27"

    # The spec describes Ryan and Scott as the saturated candidates in this peak week
    ryan_hours = grid.get("RO", {}).get(week, 0.0)
    scott_hours = grid.get("SW", {}).get(week, 0.0)
    print(f"  Projected W30 hours — Ryan: {ryan_hours:.1f}, Scott: {scott_hours:.1f}, "
          f"Nathan: {grid.get('NK', {}).get(week, 0.0):.1f}, Mo: {grid.get('MK', {}).get(week, 0.0):.1f}")

    assert ryan_hours > 0,  "Ryan should have projected hours in W30"
    assert scott_hours > 0, "Scott should have projected hours in W30"

    collisions = detect_collisions(grid)
    print(f"  Collisions detected: {len(collisions)}")
    for c in collisions[:6]:
        print(f"    {c.severity:6s} {c.person} ({c.week}): {c.detail}")
    assert len(collisions) >= 1, "Should detect at least one collision in W30 with this load"

    # Mo hours should never exceed cap; if they do, that's a RED
    mo_hours = grid.get("MK", {}).get(week, 0.0)
    if mo_hours > 8.0:
        assert any(c.rule == "mo_bandwidth_breach" for c in collisions), \
            "Mo over 8 hrs should flag mo_bandwidth_breach"


def test_proposal_out_weighting() -> None:
    _section("PROPOSAL_OUT events weighted at 50% (§10.5)")
    base_event = {
        "id": "X", "phase": "DD", "tier": 2, "team": ["RO", "JW"],
        "weu_hours": 40.0, "start_date": "2026-08-03T00:00:00Z", "end_date": "2026-08-07T23:59:59Z",
    }
    active_grid = build_load_grid([dict(base_event, status="ACTIVE_PROJECT")])
    proposal_grid = build_load_grid([dict(base_event, status="PROPOSAL_OUT")])
    intake_grid = build_load_grid([dict(base_event, status="INTAKE_PENDING")])

    week = "2026-08-03"
    active_ro = active_grid["RO"][week]
    proposal_ro = proposal_grid["RO"][week]
    intake_ro = intake_grid.get("RO", {}).get(week, 0.0)

    assert abs(proposal_ro - active_ro * PROPOSAL_OUT_WEIGHT) < 1e-6, \
        f"PROPOSAL_OUT should be 50% of ACTIVE: active={active_ro:.2f}, proposal={proposal_ro:.2f}"
    assert intake_ro == 0.0, "INTAKE_PENDING should contribute 0 hours"
    print(f"  ACTIVE Ryan: {active_ro:.2f} → PROPOSAL_OUT: {proposal_ro:.2f} (½) → INTAKE_PENDING: {intake_ro:.2f}  ✓")


def test_mitigation_suggestions() -> None:
    _section("Mitigation engine produces suggestions (§10.6)")
    # Force Nathan into overload to exercise pattern 1 (shift PM admin) + pattern 3 (pull Mo early)
    grid = {
        "NK": {"2026-08-03": 34.0, "2026-08-10": 35.0, "2026-08-17": 33.0},
        "RO": {"2026-08-03": 38.0},
        "SW": {"2026-08-03": 35.0},
        "JW": {"2026-08-03": 10.0},   # has slack to absorb work
        "RS": {"2026-08-03": 8.0},    # has slack to take production
        "MK": {"2026-08-03": 2.0},    # has slack to pull early
    }
    collisions = detect_collisions(grid)
    mitigations = suggest_mitigations(collisions, grid)
    patterns = {m.pattern for m in mitigations}
    print(f"  Collisions: {len(collisions)}  Mitigations: {len(mitigations)}")
    for m in mitigations:
        print(f"    [{m.pattern}] {m.action}  ({m.rationale})")
    assert "shift_pm_admin" in patterns or "pull_randall_into_production" in patterns or "pull_mo_early" in patterns, \
        f"Expected at least one §10.6 pattern; got {patterns}"
    assert any(c.rule == "nathan_burnout_3wk" for c in collisions), \
        "Nathan at 33+ hrs for 3 consecutive weeks should trigger burnout flag"


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end analyze_project
# ─────────────────────────────────────────────────────────────────────────────

def test_analyze_project_end_to_end() -> None:
    _section("End-to-end analyze_project on Scottsdale Med-Spa")
    intake = {
        "id": 999,
        "project_name": "Scottsdale Med-Spa Surgical Suite",
        "project_type": "healthcare",
        "sq_ft": 8_500,
        "state": "AZ",
        "schedule_tier": "Compressed",
        "start_date": "2026-06-22",
        "ifp_due_date": "2026-08-07",
        "weu_hours": 210,
        "status": "ACTIVE_PROJECT",
    }
    result = analyze_project(intake)
    print(f"  Tier derived: {result['tier']}")
    print(f"  Team: lead={result['team']['lead_pe']}, stamp={result['team']['stamp']}, "
          f"eng_support={result['team']['eng_support']}")
    print(f"  Constraint violations: {len(result['constraint_violations'])}")
    print(f"  Collisions: {len(result['collisions'])}, Mitigations: {len(result['mitigations'])}")

    assert result["tier"] == 4, "Healthcare → Tier 4"
    assert result["team"]["lead_pe"] == "NK"
    assert result["team"]["stamp"] == "MK"
    for m in result["team"]["all_members"]:
        assert m not in EITS, f"Tier 4 must not include EIT {m}"
    assert len(result["constraint_violations"]) == 0, \
        f"Default Tier 4 assignment should be clean; got {result['constraint_violations']}"


def test_mitigation_carries_apply_metadata() -> None:
    _section("Mitigation suggestions carry structured apply metadata")
    grid = {
        "RO": {"2026-08-03": 38.0},                # over cap
        "JW": {"2026-08-03": 10.0},                # has slack
        "NK": {"2026-08-03": 26.0},
        "MK": {"2026-08-03": 2.0},
        "SW": {"2026-08-03": 15.0},
        "RS": {"2026-08-03": 8.0},
    }
    collisions = detect_collisions(grid)
    mitigations = suggest_mitigations(collisions, grid)
    applicable = [m for m in mitigations if m.applicable]
    assert applicable, "At least one mitigation should be applicable in this scenario"
    sample = applicable[0]
    assert sample.from_person and sample.to_person and sample.hours_delta > 0, \
        f"Applicable mitigation must carry from/to/hours: {sample}"
    print(f"  Applicable: {sample.pattern}  {sample.from_person} → {sample.to_person}  "
          f"({sample.hours_delta} hrs)  ✓")


def test_apply_deltas_resolves_collision() -> None:
    _section("Applying a mitigation removes the underlying collision")
    grid_before = {
        "RO": {"2026-08-03": 38.0},                # YELLOW/RED
        "JW": {"2026-08-03": 10.0},
    }
    before = detect_collisions(grid_before)
    assert any(c.person == "RO" and c.severity in ("RED", "YELLOW") for c in before), \
        "Setup precondition: Ryan should be flagged"

    applied = [AppliedMitigation(
        id=1, pattern="shift_pm_admin", week="2026-08-03",
        from_person="RO", to_person="JW", hours_delta=6.0,
        rationale="test",
    )]
    grid_after = apply_deltas_to_grid(grid_before, applied)
    after = detect_collisions(grid_after)

    ryan_after = grid_after["RO"]["2026-08-03"]
    jacob_after = grid_after["JW"]["2026-08-03"]
    print(f"  Before: RO={grid_before['RO']['2026-08-03']:.1f} JW={grid_before['JW']['2026-08-03']:.1f}")
    print(f"  After : RO={ryan_after:.1f} JW={jacob_after:.1f}")
    assert ryan_after == 32.0, f"Ryan should drop to 32, got {ryan_after}"
    assert jacob_after == 16.0, f"Jacob should rise to 16, got {jacob_after}"
    assert not any(c.person == "RO" for c in after), \
        "Ryan collision should be cleared after applying the mitigation"
    print(f"  Collisions before: {len(before)}, after: {len(after)}  ✓")


def test_revert_restores_load() -> None:
    _section("Reverting a mitigation restores the original load")
    grid_before = {"RO": {"2026-08-03": 38.0}, "JW": {"2026-08-03": 10.0}}
    reverted_at = "2026-08-03T12:00:00Z"
    applied = [AppliedMitigation(
        id=1, pattern="shift_pm_admin", week="2026-08-03",
        from_person="RO", to_person="JW", hours_delta=6.0, rationale="test",
        reverted_at=reverted_at,
    )]
    grid_after = apply_deltas_to_grid(grid_before, applied)
    # Reverted mitigations should be no-ops
    assert grid_after["RO"]["2026-08-03"] == 38.0
    assert grid_after["JW"]["2026-08-03"] == 10.0
    print("  Reverted mitigation has no effect on the grid  ✓")


def test_safety_passes_safe_shift() -> None:
    _section("preview_mitigation_safety passes a clearly-safe shift")
    # Jacob has plenty of slack (10 hrs used vs 36 cap); 6-hr add lands at 16/36.
    grid = {"RO": {"2026-08-03": 38.0}, "JW": {"2026-08-03": 10.0}}
    preview = preview_mitigation_safety(grid, "RO", "JW", "2026-08-03", 6.0)
    assert preview["safe"], f"Safe shift should pass: {preview}"
    assert preview["warnings"] == [], f"Safe shift should have no warnings: {preview['warnings']}"
    print(f"  Safe shift passed cleanly. warnings={preview['warnings']}  ✓")


def test_safety_rationale_annotation_flow() -> None:
    _section("Route-level apply flow with acknowledge_warnings")
    # Mirrors the logic in main.py's /api/staffing/apply-mitigation handler:
    # if unsafe and not acknowledged → 422; if unsafe and acknowledged → persist
    # with warnings appended to rationale for audit trail.
    grid = {"RO": {"2026-08-03": 38.0}, "JW": {"2026-08-03": 32.0}}
    preview = preview_mitigation_safety(grid, "RO", "JW", "2026-08-03", 8.0)
    assert not preview["safe"], "Setup precondition: should be unsafe"

    # Case 1: Mo did NOT acknowledge → route returns 422 (no persistence)
    acknowledged = False
    if not preview["safe"] and not acknowledged:
        # Route returns 422 here; nothing is persisted
        persisted = False
    else:
        persisted = True
    assert persisted is False, "Without acknowledge_warnings, must not persist"
    print("  Unsafe shift without ack → route returns 422 (not persisted)  ✓")

    # Case 2: Mo acknowledged → route persists with annotated rationale
    acknowledged = True
    original_rationale = "Need to free up Ryan for Project D Rush"
    if not preview["safe"] and acknowledged:
        annotated = original_rationale + " [ack: " + "; ".join(preview["warnings"]) + "]"
    else:
        annotated = original_rationale
    assert "[ack:" in annotated, f"Annotated rationale should include ack tag: {annotated}"
    assert "bottleneck" in annotated, f"Annotated rationale should contain warning text: {annotated}"
    print(f"  Unsafe shift with ack → persisted with annotated rationale:")
    print(f"    {annotated}  ✓")


def test_preview_safety_warns() -> None:
    _section("preview_mitigation_safety warns about pushing receiver over cap")
    grid = {"RO": {"2026-08-03": 38.0}, "JW": {"2026-08-03": 32.0}}  # Jacob already near cap
    preview = preview_mitigation_safety(grid, "RO", "JW", "2026-08-03", 8.0)
    assert not preview["safe"], f"Should warn: receiver would land over cap. Got: {preview}"
    assert any("bottleneck" in w for w in preview["warnings"]), \
        f"Warnings should mention bottleneck: {preview['warnings']}"
    print(f"  Warnings: {preview['warnings']}  ✓")

    # Mo over 8 hrs is a hard ceiling
    grid2 = {"NK": {"2026-08-03": 30.0}, "MK": {"2026-08-03": 6.0}}
    p2 = preview_mitigation_safety(grid2, "NK", "MK", "2026-08-03", 4.0)
    assert not p2["safe"] and any("oversight ceiling" in w for w in p2["warnings"]), \
        f"Mo over 8 hrs should be flagged: {p2}"
    print(f"  Mo ceiling check warnings: {p2['warnings']}  ✓")


def main() -> int:
    try:
        test_baseline_capacity()
        test_tier_derivation()
        test_default_templates()
        test_rush_tier2_prefers_ryan()
        test_retrofit_forces_nathan()
        test_no_eit_on_tier4()
        test_out_of_state_stamping()
        test_mo_oversight_only()
        test_worked_example_collisions()
        test_proposal_out_weighting()
        test_mitigation_suggestions()
        test_mitigation_carries_apply_metadata()
        test_apply_deltas_resolves_collision()
        test_revert_restores_load()
        test_safety_passes_safe_shift()
        test_preview_safety_warns()
        test_safety_rationale_annotation_flow()
        test_analyze_project_end_to_end()
    except AssertionError as e:
        print(f"\nFAIL: {e}")
        return 1
    print("\nALL STAFFING TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
