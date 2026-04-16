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
    result = fee_range_estimate({"project_type": "new_construction", "approx_sf": "20000"})
    assert result == "$15,000 \u2013 $20,000", f"Expected '$15,000 – $20,000', got {result!r}"
    assert fee_range_estimate({"project_type": "new_construction", "approx_sf": "0"}) is None
    assert fee_range_estimate({"project_type": "new_construction"}) is None
    assert fee_range_estimate({"project_type": "unknown_type", "approx_sf": "10000"}) is None
    bts = fee_range_estimate({"project_type": "build_to_suit_retrofit", "approx_sf": "10000"})
    assert bts == "$3,000 \u2013 $6,700", f"Unexpected BTS result: {bts!r}"
    ti = fee_range_estimate({"project_type": "tenant_improvement", "approx_sf": "10000"})
    assert ti == "$2,500 \u2013 $5,000", f"Unexpected TI result: {ti!r}"


def _check_complexity() -> None:
    assert complexity_estimate({"approx_sf": "8000"}) == "low", "8000 SF should be low"
    assert complexity_estimate({"approx_sf": "18000"}) == "medium", "18000 SF should be medium"
    assert complexity_estimate({"approx_sf": "30000"}) == "high", "30000 SF should be high"
    assert complexity_estimate({}) == "unknown", "missing SF should be unknown"
    assert complexity_estimate({"approx_sf": "0"}) == "unknown", "zero SF should be unknown"


def main() -> None:
    _check_fast_track()
    _check_fee_range()
    _check_complexity()

    db_path = ROOT / "data" / "avs_intake.sqlite3"
    db.init_db(db_path)

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

    intake_id = db.create_intake(
        db_path,
        inquiry_date="2026-04-10",
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

    row = db.get_intake(db_path, intake_id)
    assert row is not None
    assert row.project_name.startswith("Self-check")

    print("OK")


if __name__ == "__main__":
    main()
