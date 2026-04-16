from __future__ import annotations

from typing import Any, Optional


class RiskAdjustedFeeEstimator:
    # Structural engineering $/SF — calibrated to real-world SE fee benchmarks.
    # These are base rates before material, risk, and location adjustments.
    base_rates: dict[str, float] = {
        "residential": 0.35,
        "commercial":  0.50,
        "medical":     0.85,
        "industrial":  0.40,
        "education":   0.60,
    }

    # Material complexity modifier (relative effort, not a markup multiplier).
    material_multipliers: dict[str, float] = {
        "steel":       1.10,
        "concrete":    1.20,
        "timber":      1.00,
        "masonry":     1.08,
        "cold-formed": 1.12,
    }

    # Risk surcharges — each True flag adds this fraction to the base fee.
    # Kept intentionally modest; real risk is priced via scope negotiation.
    risk_weights: dict[str, float] = {
        "unclear_scope":           0.08,
        "high_liability_medical":  0.15,
        "historic_reuse":          0.12,
        "compressed_schedule":     0.08,
        "hard_stop_deadline":      0.05,
        "no_site_access":          0.05,
        "missing_docs_or_geotech": 0.08,
        "unresponsive_architect":  0.10,
        "no_clear_decision_maker": 0.07,
    }

    # Location cost-of-business modifier (licensing, seismic, local market).
    location_mods: dict[str, float] = {
        "CA": 1.15,
        "WA": 1.10,
        "NY": 1.12,
        "FL": 1.08,
    }

    def calculate_fee(self, data: dict[str, Any]) -> dict[str, Any]:
        sq_ft    = int(data.get("sq_ft") or 0)
        p_type   = (data.get("project_type") or "commercial").lower()
        material = (data.get("material") or "steel").lower()

        base_rate    = self.base_rates.get(p_type, 2.00)
        material_mod = self.material_multipliers.get(material, 1.00)
        subtotal     = sq_ft * base_rate * material_mod

        total_risk_multiplier = 1.0
        for factor, weight in self.risk_weights.items():
            if data.get(factor):
                total_risk_multiplier += weight

        loc_factor = self.location_mods.get(data.get("location", ""), 1.00)

        final_fee = subtotal * total_risk_multiplier * loc_factor

        warnings = [k for k in self.risk_weights if data.get(k) is True]

        return {
            "project_name":           data.get("name", "Unknown Project"),
            "estimated_base":         round(subtotal, 2),
            "risk_adjustment_factor": round(total_risk_multiplier, 2),
            "location_factor":        loc_factor,
            "suggested_fee_range": {
                "low":  round(final_fee * 0.9, -2),
                "high": round(final_fee * 1.2, -2),
            },
            "warnings": warnings,
        }


# ── Building-type → estimator project_type ──────────────────────────────────
_BUILDING_TYPE_MAP: dict[str, str] = {
    "healthcare":  "medical",
    "education":   "education",
    "warehouse":   "industrial",
    "data_center": "industrial",
    "retail":      "commercial",
    "mixed_use":   "commercial",
    "other":       "commercial",
}


def _map_project_type(answers: dict[str, Any]) -> str:
    bt = (answers.get("building_type") or "").strip()
    return _BUILDING_TYPE_MAP.get(bt, "commercial")


def _map_risk_flags(answers: dict[str, Any]) -> dict[str, bool]:
    quick = set(answers.get("quick_flags") or [])
    hard_stops = answers.get("hard_stop_deadlines") or []

    def _doc_missing_geotech() -> bool:
        pt = (answers.get("project_type") or "").strip()
        if pt == "new_construction":
            return not answers.get("doc_geotech")
        if pt in {"build_to_suit_retrofit", "tenant_improvement",
                  "addition_expansion", "one_off_unique"}:
            return not answers.get("doc_existing_struct_drawings")
        return False

    return {
        "unclear_scope": (
            answers.get("scope_definition") in {"undefined", "evolving"}
            or "quick_scope_unclear" in quick
        ),
        "high_liability_medical": (
            answers.get("building_type") == "healthcare"
            or answers.get("scope_risk_type") == "ti_high_liability"
            or "quick_ti_high_liability" in quick
        ),
        "historic_reuse": (
            answers.get("scope_risk_type") == "adaptive_reuse"
            or "quick_historic_adaptive_reuse" in quick
        ),
        "compressed_schedule": (
            answers.get("schedule_realism") in {"compressed", "unrealistic"}
            or "quick_schedule_compressed" in quick
        ),
        "hard_stop_deadline": (
            bool([x for x in hard_stops if x])
            or "quick_hard_stop_deadline" in quick
        ),
        "no_site_access": (
            answers.get("site_access") == "no"
            or "quick_no_site_access" in quick
        ),
        "missing_docs_or_geotech": (
            _doc_missing_geotech()
            or "quick_missing_geotech_or_drawings" in quick
        ),
        "unresponsive_architect": (
            answers.get("architect_responsiveness") == "unresponsive"
            or answers.get("architect_status") in {"new", "unknown"}
            or "quick_architect_unresponsive" in quick
        ),
        "no_clear_decision_maker": (
            answers.get("decision_maker_clarity") in {"unclear", "none"}
            or "quick_no_decision_maker" in quick
        ),
    }


def cognasync_estimate_from_answers(
    project_name: str,
    answers: dict[str, Any],
) -> Optional[dict[str, Any]]:
    """
    Bridge between AVS intake answers and RiskAdjustedFeeEstimator.
    Returns None when sq_ft is absent or zero.
    """
    try:
        sq_ft = int(str(answers.get("approx_sf") or "0").strip())
    except (ValueError, TypeError):
        sq_ft = 0

    if not sq_ft:
        return None

    material = (answers.get("primary_material") or "steel").lower()
    location = (answers.get("state") or "").strip().upper()

    payload: dict[str, Any] = {
        "name":         project_name,
        "sq_ft":        sq_ft,
        "project_type": _map_project_type(answers),
        "material":     material,
        "location":     location,
        **_map_risk_flags(answers),
    }

    return RiskAdjustedFeeEstimator().calculate_fee(payload)
