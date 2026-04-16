from __future__ import annotations

from typing import Any, Optional

# ── Rate card ──────────────────────────────────────────────────────────────────
# Keyed by (delivery_bucket, building_type) → (rate_low $/SF, rate_high $/SF)
# delivery_bucket: "new" | "ti" | "bts"

_RATE_CARD: dict[str, dict[str, tuple[float, float]]] = {
    "new": {
        "retail":      (0.75, 0.98),
        "warehouse":   (0.55, 0.75),
        "healthcare":  (1.75, 2.50),
        "education":   (1.25, 1.75),
        "data_center": (1.75, 2.50),
        "mixed_use":   (1.00, 1.40),
        "other":       (0.75, 0.98),   # default → retail
    },
    "ti": {
        "retail":      (0.45, 0.75),
        "warehouse":   (0.30, 0.55),
        "healthcare":  (1.50, 2.25),   # ← key fix
        "education":   (1.00, 1.50),
        "data_center": (1.50, 2.25),
        "mixed_use":   (0.75, 1.20),
        "other":       (0.45, 0.75),
    },
}
# BTS = 80 % of TI rates
_RATE_CARD["bts"] = {
    bt: (round(lo * 0.8, 3), round(hi * 0.8, 3))
    for bt, (lo, hi) in _RATE_CARD["ti"].items()
}

# ── Project-type → delivery bucket ─────────────────────────────────────────────
_DELIVERY_BUCKET: dict[str, str] = {
    "new_construction":       "new",
    "repeating_program":      "new",
    "tenant_improvement":     "ti",
    "addition_expansion":     "ti",
    "one_off_unique":         "ti",
    "build_to_suit_retrofit": "bts",
}

# ── Complexity multipliers ──────────────────────────────────────────────────────
_COMPLEXITY_MULT: dict[str, float] = {
    "low":    1.00,
    "medium": 1.25,
    "high":   1.65,
}

# ── Risk multipliers (by flag count) ───────────────────────────────────────────
def _risk_mult(flag_count: int) -> float:
    if flag_count <= 1: return 1.00
    if flag_count <= 3: return 1.15
    if flag_count <= 5: return 1.35
    return 1.55

# ── Minimum floor fees ─────────────────────────────────────────────────────────
def _floor_fee(building_type: str, delivery_bucket: str) -> float:
    if building_type in {"healthcare", "data_center"}:
        return 15_000.0
    if delivery_bucket == "new":
        return 10_000.0
    return 7_500.0


# ── Core estimator ─────────────────────────────────────────────────────────────

class RiskAdjustedFeeEstimator:
    """
    Inputs (via calculate_fee):
        sq_ft           – gross square footage (int)
        building_type   – intake field: healthcare | retail | warehouse |
                          education | data_center | mixed_use | other
        delivery_bucket – "new" | "ti" | "bts"  (derived from project_type)
        complexity      – "low" | "medium" | "high"  (from complexity_estimate)
        flags           – dict[str, bool]  (9 risk flags from _map_risk_flags)
        project_name    – display string

    Calculation order:
        1. $/SF lookup  → base_fee_range
        2. × complexity → complexity_adjusted_range
        3. × risk       → risk_adjusted_range
        4. floor check  → suggested_fee_range  (floor applied if needed)
    """

    def _check_review_conditions(self, data: dict[str, Any], sq_ft: int, flag_count: int) -> tuple[bool, str]:
        """
        Evaluates the 6 conditions that require Mo review before the fee is used.
        Returns (fee_requires_review, review_reason).
        Multiple triggered conditions are joined into a single reason string.
        """
        answers       = data.get("_answers", {})
        building_type = (data.get("building_type") or "").lower()
        complexity    = (data.get("complexity") or "low").lower()
        scope_def     = (answers.get("scope_definition") or "").strip()
        scope_risk    = (answers.get("scope_risk_type")  or "").strip()
        specialist    = (answers.get("specialist_support") or "").strip()
        est_cost_raw  = answers.get("est_construction_cost")
        try:
            est_cost = int(str(est_cost_raw or "0").strip())
        except (ValueError, TypeError):
            est_cost = 0

        reasons: list[str] = []

        # 1. SF missing / zero / below 1,000
        if 0 < sq_ft < 1_000:
            reasons.append(f"Square footage ({sq_ft:,} SF) is below the 1,000 SF minimum for reliable estimation")

        # 2. Healthcare / data center + high complexity
        if building_type in {"healthcare", "data_center"} and complexity == "high":
            reasons.append(
                f"{'Healthcare' if building_type == 'healthcare' else 'Data center'} project "
                f"with high complexity requires engineering judgment"
            )

        # 3. Scope is undefined / evolving
        if scope_def in {"undefined", "evolving"}:
            reasons.append("Scope is undefined or evolving — fee cannot be reliably estimated until scope is locked")

        # 4. Six or more red flags
        if flag_count >= 6:
            reasons.append(f"{flag_count} red flags exceed reliable estimation threshold")

        # 5. High-liability TI with unknown / absent specialist support
        if scope_risk == "ti_high_liability" and specialist in {"unknown", "no", ""}:
            reasons.append("High-liability TI with unknown or absent specialist support")

        # 6. No SF AND no construction cost (nothing to anchor estimate)
        if sq_ft == 0 and est_cost == 0:
            reasons.append("No square footage or construction cost — nothing to anchor the estimate")

        if reasons:
            return True, "; ".join(reasons)
        return False, ""

    def calculate_fee(self, data: dict[str, Any]) -> dict[str, Any]:
        sq_ft         = int(data.get("sq_ft") or 0)
        building_type = (data.get("building_type") or "other").lower().strip()
        delivery      = (data.get("delivery_bucket") or "ti").lower().strip()
        complexity    = (data.get("complexity") or "low").lower().strip()
        flags: dict[str, bool] = data.get("flags") or {}

        # ── 1. Base $/SF lookup ───────────────────────────────────────────────
        bucket_rates = _RATE_CARD.get(delivery, _RATE_CARD["ti"])
        rate_low, rate_high = bucket_rates.get(building_type, bucket_rates["other"])

        base_low  = sq_ft * rate_low
        base_high = sq_ft * rate_high

        # ── 2. Complexity multiplier ──────────────────────────────────────────
        cx_mult = _COMPLEXITY_MULT.get(complexity, 1.0)
        cx_low  = base_low  * cx_mult
        cx_high = base_high * cx_mult

        # ── 3. Risk multiplier (flag count) ──────────────────────────────────
        active_flags  = [k for k, v in flags.items() if v]
        flag_count    = len(active_flags)
        risk_mult_val = _risk_mult(flag_count)
        risk_low      = cx_low  * risk_mult_val
        risk_high     = cx_high * risk_mult_val

        # ── 4. Floor fee ──────────────────────────────────────────────────────
        floor = _floor_fee(building_type, delivery)
        floor_applied = risk_low < floor
        if floor_applied:
            # Scale both ends up proportionally so the range shape is preserved
            scale    = floor / risk_low if risk_low > 0 else 1.0
            risk_low  = floor
            risk_high = risk_high * scale

        # Round to nearest $100 for display
        def r(x: float) -> float:
            return round(x / 100) * 100

        return {
            "project_name":             data.get("project_name", "Unknown Project"),
            "sq_ft":                    sq_ft,
            "building_type":            building_type,
            "delivery_bucket":          delivery,
            "effective_rate_low":       rate_low,
            "effective_rate_high":      rate_high,
            # Step 1
            "base_fee_range":           {"low": r(base_low),  "high": r(base_high)},
            # Step 2
            "complexity_multiplier":    cx_mult,
            "complexity_adjusted_range":{"low": r(cx_low),   "high": r(cx_high)},
            # Step 3
            "risk_multiplier":          risk_mult_val,
            "flag_count":               flag_count,
            "risk_adjusted_range":      {"low": r(risk_low),  "high": r(risk_high)},
            # Step 4 (suggested = final after floor)
            "floor_fee":                floor,
            "floor_applied":            floor_applied,
            "suggested_fee_range":      {"low": r(risk_low),  "high": r(risk_high)},
            # Warnings
            "warnings":                 active_flags,
            "needs_manual_review":      False,
            # Review flag
            **dict(zip(
                ("fee_requires_review", "review_reason"),
                self._check_review_conditions(data, sq_ft, flag_count),
            )),
        }


# ── Intake-field mapping helpers ───────────────────────────────────────────────

def _delivery_bucket(answers: dict[str, Any]) -> str:
    pt = (answers.get("project_type") or "").strip()
    return _DELIVERY_BUCKET.get(pt, "ti")   # default: treat unknown as TI


def _building_type(answers: dict[str, Any]) -> str:
    bt = (answers.get("building_type") or "other").strip().lower()
    return bt if bt in _RATE_CARD["ti"] else "other"


def _map_risk_flags(answers: dict[str, Any]) -> dict[str, bool]:
    quick      = set(answers.get("quick_flags") or [])
    hard_stops = answers.get("hard_stop_deadlines") or []

    def _missing_geotech() -> bool:
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
            _missing_geotech()
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


def check_fee_review_required(answers: dict[str, Any], complexity: str) -> tuple[bool, str]:
    """
    Lightweight standalone check — safe to call on every row in a list endpoint
    without running the full fee calculation.
    Returns (fee_requires_review: bool, review_reason: str).
    """
    try:
        sq_ft = int(str(answers.get("approx_sf") or "0").strip())
    except (ValueError, TypeError):
        sq_ft = 0

    flags      = _map_risk_flags(answers)
    flag_count = sum(1 for v in flags.values() if v)
    bt         = _building_type(answers)

    proxy_data = {
        "building_type": bt,
        "complexity":    complexity,
        "_answers":      answers,
    }
    return RiskAdjustedFeeEstimator()._check_review_conditions(proxy_data, sq_ft, flag_count)


def cognasync_estimate_from_answers(
    project_name: str,
    answers: dict[str, Any],
) -> dict[str, Any]:
    """
    Bridge between AVS intake answers and RiskAdjustedFeeEstimator.

    Inputs read from answers:
        approx_sf               → sq_ft
        project_type            → delivery_bucket (new / ti / bts)
        building_type           → building_type (healthcare, retail, …)
        answers["_complexity"]  → complexity  (injected by main.py from
                                    complexity_estimate() to avoid import cycle)
        quick_flags + detailed  → flags (9 risk booleans)

    Always returns a dict. When sq_ft is 0 and no construction cost, the range
    fields are None and fee_requires_review is True.
    """
    try:
        sq_ft = int(str(answers.get("approx_sf") or "0").strip())
    except (ValueError, TypeError):
        sq_ft = 0

    complexity = (answers.get("_complexity") or "low").lower()

    # Hard no-data case — can't compute a range at all
    if sq_ft == 0:
        _, reason = RiskAdjustedFeeEstimator()._check_review_conditions(
            {"building_type": _building_type(answers), "complexity": complexity, "_answers": answers},
            sq_ft=0,
            flag_count=0,
        )
        return {
            "project_name":        project_name,
            "needs_manual_review": True,
            "fee_requires_review": True,
            "review_reason":       reason or "Square footage is missing — fee estimation requires manual review.",
            "suggested_fee_range": None,
        }

    payload: dict[str, Any] = {
        "project_name":    project_name,
        "sq_ft":           sq_ft,
        "building_type":   _building_type(answers),
        "delivery_bucket": _delivery_bucket(answers),
        "complexity":      complexity,
        "flags":           _map_risk_flags(answers),
        "_answers":        answers,   # passed through for condition checks
    }

    return RiskAdjustedFeeEstimator().calculate_fee(payload)
