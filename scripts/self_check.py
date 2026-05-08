from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db
from app.decision import compute_decision, complexity_estimate, fee_range_estimate


def _check_fast_track() -> None:
    clean_repeating = {
        "project_type": "repeating_program",
        "building_type": "retail",
        "building_type_experience": "yes",
        "architect_status": "known_good",
        "architect_responsiveness": "responsive",
        "decision_maker_clarity": "direct",
        "scope_definition": "defined",
        "scope_risk_type": "standard",
        "specialist_support": "yes",
        "scope_creep_likelihood": "no",
        "schedule_realism": "comfortable",
        "weeks_to_permit_submission": "8",
        "hard_stop_deadlines": [],
        "site_access": "not_applicable",
        "docs_commitment": "yes",
        "capacity_available": "yes",
        "quick_flags": [],
        "doc_geotech": True,
        "doc_grading_plan": True,
        "doc_arch_drawings": True,
        "doc_rfp_program": True,
        "doc_site_plan": True,
        "doc_prelim_schedule": True,
    }
    result = compute_decision(clean_repeating)
    assert result["fast_track"] is True, f"Expected fast_track=True for clean repeating program, got {result['fast_track']}"

    # Same but with a red flag (compressed schedule)
    with_red_flag = dict(clean_repeating, schedule_realism="compressed")
    result2 = compute_decision(with_red_flag)
    assert result2["fast_track"] is False, f"Expected fast_track=False when red flags present, got {result2['fast_track']}"

    # Not repeating_program — should be False
    not_repeating = dict(clean_repeating, project_type="new_construction")
    result3 = compute_decision(not_repeating)
    assert result3["fast_track"] is False, f"Expected fast_track=False for non-repeating project type"


def _check_fee_range() -> None:
    # 20,000 SF new construction "other": rate 0.75–0.98 $/SF, floor $10k (not applied here)
    result = fee_range_estimate({"project_type": "new_construction", "approx_sf": "20000"})
    assert result == "$15,000 – $19,600", f"Expected '$15,000 – $19,600', got {result!r}"
    assert fee_range_estimate({"project_type": "new_construction", "approx_sf": "0"}) is None
    assert fee_range_estimate({"project_type": "new_construction"}) is None
    assert fee_range_estimate({"project_type": "unknown_type", "approx_sf": "10000"}) is None
    # 10,000 SF BTS: floor ($7,500) dominates at this project size
    bts = fee_range_estimate({"project_type": "build_to_suit_retrofit", "approx_sf": "10000"})
    assert bts == "$7,500 – $7,500", f"Unexpected BTS result: {bts!r}"
    # 10,000 SF TI: floor ($7,500) matches rate ceiling
    ti = fee_range_estimate({"project_type": "tenant_improvement", "approx_sf": "10000"})
    assert ti == "$7,500 – $7,500", f"Unexpected TI result: {ti!r}"


def _check_complexity() -> None:
    # complexity_estimate keys off building_type / scope_risk_type, not square footage
    assert complexity_estimate({"building_type": "healthcare"}) == "high", "healthcare → high"
    assert complexity_estimate({"building_type": "data_center"}) == "high", "data_center → high"
    assert complexity_estimate({"building_type": "mixed_use"}) == "medium", "mixed_use → medium"
    assert complexity_estimate({}) == "low", "no fields → low (safe default)"
    assert complexity_estimate({"scope_risk_type": "ti_high_liability"}) == "high", "high-liability TI → high"
    assert complexity_estimate({"scope_risk_type": "adaptive_reuse"}) == "high", "adaptive reuse → high"


def main() -> None:
    _check_fast_track()
    _check_fee_range()
    _check_complexity()

    answers = {
        "project_type": "new_construction",
        "building_type": "retail",
        "building_type_experience": "yes",
        "architect_status": "known_good",
        "architect_responsiveness": "responsive",
        "decision_maker_clarity": "direct",
        "relationship_type": "existing",
        "scope_definition": "defined",
        "scope_risk_type": "standard",
        "specialist_support": "yes",
        "scope_creep_likelihood": "no",
        "schedule_realism": "comfortable",
        "weeks_to_permit_submission": "12",
        "hard_stop_deadlines": [],
        "site_access": "not_applicable",
        "docs_commitment": "yes",
        "capacity_available": "yes",
        "quick_flags": [],
        "doc_geotech": True,
        "doc_grading_plan": True,
        "doc_arch_drawings": True,
        "doc_existing_struct_drawings": False,
        "doc_site_photos": False,
        "doc_rfp_program": True,
        "doc_site_plan": True,
        "doc_prelim_schedule": True,
    }

    decision = compute_decision(answers)
    assert decision["recommendation"] == "PROCEED_TO_PROPOSAL", decision
    assert "difficulty_tier" in decision, "V4.0: decision must include difficulty_tier"

    # DB round-trip (requires SUPABASE_URL + SUPABASE_KEY env vars; skipped if not set)
    import os
    if os.environ.get("SUPABASE_URL") and os.environ.get("SUPABASE_KEY"):
        intake_id = db.create_intake(
            inquiry_date="2026-04-10",
            ifp_due_date=None,
            project_name="Self-check: clean case",
            client_name="ACME",
            architect_name="Known Architect",
            lead_contact="test@example.com",
            location_region="AZ",
            submitted_by="Self-check",
            status="PROCEED_TO_PROPOSAL",
            recommendation=decision["recommendation"],
            recommendation_reason=decision["reason"],
            red_flags=decision["red_flags"],
            red_flag_counts=decision["counts"],
            answers=answers,
        )
        row = db.get_intake(intake_id)
        assert row is not None
        assert row.project_name.startswith("Self-check")
    else:
        print("  (DB round-trip skipped — SUPABASE_URL/KEY not set)")

    print("OK")


if __name__ == "__main__":
    main()
