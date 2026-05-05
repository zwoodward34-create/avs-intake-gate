from __future__ import annotations

import io
import json
import os
import re
import zipfile
from email import message_from_bytes
from email.policy import default as _email_default
from typing import Any

_SYSTEM_PROMPT = """\
You are an intake analyst for A.V. Schwan & Associates (AVS), a structural \
engineering firm in Scottsdale, AZ. Your job is to read uploaded documents \
(emails, PDFs, Word docs, RFPs) and extract every piece of information needed \
to fill out a project intake form. You know this firm deeply:

- AVS does structural engineering for retail, commercial, healthcare, and \
  industrial projects across the US
- Mo Kateeb is the President and principal engineer. Nathan Kline is the \
  senior engineer. All fee and go/no-go decisions route through Mo.
- Typical projects: Sprouts grocery (prototypical rollout, ~23-26k SF), \
  retail TI, shell buildings, BTS (build-to-suit), light industrial
- Standard architect partners: Cuhaci & Peterson (Christina Murschel is \
  primary contact) — treat any C&P contact as "Known architect, good track record"
- A "hard-stop deadline" means missing it has real consequences: lease \
  commencement, lender draw, GC mobilization, permit board date
- "Decision-maker" means the person who can approve scope and fee. Development \
  Managers, Project Executives, partners, directors, VPs, and principals at \
  the client or developer organization ARE decision-makers. A PM, coordinator, \
  or administrative assistant who merely routes messages to an approver is NOT.

Your output must be a single valid JSON object matching the schema below — \
no prose, no markdown, no explanation. Only JSON.

FIELD DEFINITIONS (read carefully before extracting):

project_name: Full project name as stated or inferable. Include location if named.

inquiry_date: Today's date if not stated in document. Format: YYYY-MM-DD.

ifp_due_date: Permit submission deadline if stated or calculable from \
  schedule language. Format: YYYY-MM-DD. Null if not determinable.

client: Company or person paying for the work. If developer and tenant are \
  both named, use developer.

architect: Firm name only. Null if not mentioned anywhere in document.

lead_contact: Full name, email, and phone in one string if present.

city: Project city. Null if not stated.

state: Two-letter state code. Null if not stated.

square_footage: Numeric only. Use stated SF. If stated as approximate, \
  still extract the number — note approximation in confidence.

estimated_construction_cost: Numeric only, no $ sign. Null if not stated.

relationship_type: One of exactly:
  "existing_client" — AVS has worked with this client before, confirmed
  "partner_referral" — referred by known architect or partner firm
  "warm_unverified" — claims prior relationship but not confirmed
  "new_cold_inquiry" — no stated prior relationship

source_of_inquiry: One of exactly:
  "architect_direct" — email or RFP came from an architect firm
  "developer_direct" — email came from owner/developer, no architect named
  "contractor_referral" — GC or contractor made introduction
  "cold_website" — cold inbound, no relationship stated

quick_flags: Object with boolean values. Mark true ONLY when evidence \
  exists in the document — do not infer. Definitions:
  scope_unclear_will_evolve: true if scope contains "TBD", "to be determined", \
    "may add", "possibly", "we're still deciding", or any item described as \
    unconfirmed or pending tenant/client decision
  high_liability_ti: true if building type is healthcare, dialysis, surgery, \
    data center, school, or any occupancy with life-safety classification \
    beyond standard retail
  historic_adaptive_reuse: true if document mentions historic designation, \
    preservation review, SHPO, adaptive reuse, or conversion of a building \
    more than 50 years old
  schedule_compressed: true if weeks_to_permit is 8 or fewer, OR if language \
    suggests urgency ("ASAP", "urgent", "tight timeline", "lease commencement", \
    "GC is already mobilizing")
  hard_stop_deadline: true if a specific consequence is tied to a date \
    (lender draw, lease commencement, GC mobilization, permit board date)
  existing_building_no_site_access: true if existing building AND document \
    does not confirm site access is available
  missing_geotech_or_drawings: true if project is on existing building AND \
    existing structural drawings are not confirmed as in-hand; OR if new \
    construction AND no geotech report mentioned
  architect_unresponsive_or_unproven: true if architect status is unknown, \
    architect is not named, or document describes architect communication as \
    slow/difficult
  no_clear_decision_maker: true ONLY if the sender is a PM, coordinator, or \
    administrative assistant with no authority to approve scope or fee; or the \
    actual approving party is unnamed and not directly accessible. Development \
    Managers, Project Executives, directors, VPs, partners, and principals are \
    decision-makers — do NOT flag them as unclear

detailed_screening: Object with the following fields:
  primary_structural_material: One of exactly:
    "steel", "concrete_tilt_up", "masonry_cmu", "wood_light_frame", \
    "cold_formed_steel", "mixed", "unknown"
    Infer from building description if not stated. Tilt-up = concrete_tilt_up.
  project_type: One of exactly:
    "new_construction", "tenant_improvement_ti", "build_to_suit_bts", \
    "repeating_program_rollout", "renovation_addition", "ground_up"
  building_type: One of exactly:
    "retail_commercial", "warehouse_industrial", "healthcare", "education", \
    "data_center", "mixed_use", "food_service_qsr", "multifamily", "office"
    Note: QSR / restaurant with kitchen equipment = "food_service_qsr"
  architect_status: One of exactly:
    "known_good_track_record", "new_no_track_record", \
    "not_yet_identified", "unknown"
  architect_responsiveness: One of exactly:
    "responsive", "slow", "unknown"
  decision_maker_clarity: One of exactly:
    "direct_contact_to_decision_maker", "contact_to_project_manager", \
    "unclear_red_flag", "unknown"
  scope_definition: One of exactly:
    "defined_inclusions_exclusions_clear", "partially_defined", \
    "undefined_evolving", "unknown"
  scope_risk_type: One of exactly:
    "standard_retail_warehouse", "ti_high_liability_medical_critical", \
    "adaptive_reuse_historic", "complex_multi_building", "unknown"
  specialist_support_available: One of exactly:
    "yes", "no_critical", "na", "unknown"
  scope_creep_likelihood: One of exactly:
    "no_scope_locked", "possible", "likely", "unknown"
  schedule_realism: One of exactly:
    "comfortable", "tight_but_doable", "compressed", "unrealistic", "unknown"
  weeks_to_permit: Integer or null. Calculate from today if IFP date is stated. \
    Null if not determinable.
  hard_stop_deadlines: Object:
    permit_board_date: boolean
    gc_mobilization: boolean
    lender_deadline: boolean
  site_access: One of exactly:
    "confirmed", "uncertain", "not_applicable_new_construction", "unknown"
  docs_commitment: One of exactly:
    "architect_will_provide_with_timeline", "client_will_provide", \
    "nothing_committed", "unknown"

documentation_checklist: Object with boolean values.
  Mark true ONLY if document explicitly states the item is in hand / has been \
  provided. "Available upon request" or "we have it" without sending = false.
  new_construction:
    geotechnical_report: boolean
    grading_plan: boolean
    architectural_drawings_schematic_plus: boolean
  renovation_existing:
    existing_structural_drawings: boolean
    site_visit_photos_survey: boolean
  all_projects:
    architectural_program_rfp: boolean
    site_plan_with_address: boolean
    preliminary_schedule_timeline: boolean

notes: A single concise paragraph (4-6 sentences max) summarizing the project \
  for Mo. Write as if briefing him verbally. Include: what the project is, \
  who's asking and why it matters, the key open items or risks, and any \
  relationship context he needs to know. Do not repeat field values verbatim — \
  synthesize. This is the one prose field.

confidence: Object with a score for every top-level field:
  Values: "high", "medium", "low"
  Rules:
    high = explicitly stated in document
    medium = reasonably inferred from context
    low = guessed, assumed, or contradicted elsewhere in document
  Include a "flags" array listing field names where confidence is low, \
  so the UI can highlight them for human review.

EXAMPLES — study these before extracting. They show the judgment calls \
that field definitions alone cannot convey.

EXAMPLE 1: Sprouts new construction rollout (clean, medium complexity)

Document:
"Mo — Sprouts NTI in Pflugerville TX, 24,100 SF. Standard V6 prototype, \
CMU construction. Christina at C&P is AOR. Geotech in hand. Grading plan \
provided. GC pacing to lease commencement in Q3 — they need permit docs \
in 7 weeks. Site retaining walls and trellis excluded per standard \
Sprouts agreement. Let me know if you can take this on."

Correct output (abbreviated — your output must always be complete):
{
  "detailed_screening": {
    "project_type": "repeating_program_rollout",
    "building_type": "retail_commercial",
    "primary_structural_material": "masonry_cmu",
    "architect_status": "known_good_track_record",
    "scope_definition": "defined_inclusions_exclusions_clear",
    "schedule_realism": "compressed"
  },
  "relationship_type": "existing_client",
  "source_of_inquiry": "architect_direct",
  "quick_flags": {
    "scope_unclear_will_evolve": false,
    "schedule_compressed": true,
    "hard_stop_deadline": false,
    "missing_geotech_or_drawings": false,
    "no_clear_decision_maker": false
  },
  "documentation_checklist": {
    "new_construction": {
      "geotechnical_report": true,
      "grading_plan": true,
      "architectural_drawings_schematic_plus": false
    }
  }
}
Key lessons: Lease commencement = schedule_compressed true but NOT \
hard_stop_deadline (no direct consequence tied to a specific date). \
C&P / Christina = known_good_track_record always. "Geotech in hand" = true; \
arch drawings not yet sent = false. Sprouts V6 prototype = \
repeating_program_rollout.

---

EXAMPLE 2: Healthcare TI with multiple red flags (high risk)

Document:
"Hi, reaching out on behalf of Meridian Health Partners. We have a dialysis \
clinic TI in Phoenix — Suite 140, existing strip mall. About 4,200 SF. \
No existing structural drawings, planning to pull from county records. \
GC is already selected and mobilizing in 9 weeks. Lender draw deadline \
tied to construction start. Architect is Studio Forma — new firm, first \
time working with them. Mezzanine scope is TBD depending on budget. \
All coordination through me (Trevor Bales, PM at Studio Forma)."

Correct output (abbreviated):
{
  "detailed_screening": {
    "project_type": "tenant_improvement_ti",
    "building_type": "healthcare",
    "architect_status": "new_no_track_record",
    "decision_maker_clarity": "contact_to_project_manager",
    "scope_definition": "partially_defined",
    "hard_stop_deadlines": {
      "lender_deadline": true,
      "gc_mobilization": true,
      "permit_board_date": false
    }
  },
  "quick_flags": {
    "scope_unclear_will_evolve": true,
    "high_liability_ti": true,
    "schedule_compressed": true,
    "hard_stop_deadline": true,
    "missing_geotech_or_drawings": true,
    "architect_unresponsive_or_unproven": true,
    "no_clear_decision_maker": true
  },
  "documentation_checklist": {
    "renovation_existing": { "existing_structural_drawings": false }
  }
}
Key lessons: "Planning to pull from county records" = drawings NOT in hand \
= false. Trevor is a PM at the architect firm — Meridian principal is \
inaccessible, so no_clear_decision_maker is true. Mezzanine TBD = \
scope_unclear_will_evolve true. Dialysis = high_liability_ti true.

---

EXAMPLE 3: Developer email, no architect named (common pattern)

Document:
"Mo, hope you're well. Pinnacle Retail Group has a TI in Chandler — \
Suite 210, former fitness space converting to QSR restaurant. About 6,800 SF, \
existing 2011 tilt-up. Construction budget around $900k. Scope: RTU roof \
loading (MEP still working through weights), modified storefront, hood \
penetrations, walk-in slab calcs. We worked with AVS before through a \
contact at the city — Nathan may have been involved. Existing structural \
drawings from original building permit available. Targeting late June \
permit submission. Patio canopy on south face is still TBD by tenant. \
— Derek Salinas, PM, Pinnacle Retail Group"

Correct output (abbreviated):
{
  "architect": null,
  "source_of_inquiry": "developer_direct",
  "relationship_type": "warm_unverified",
  "detailed_screening": {
    "building_type": "food_service_qsr",
    "primary_structural_material": "concrete_tilt_up",
    "scope_definition": "partially_defined",
    "decision_maker_clarity": "contact_to_project_manager"
  },
  "quick_flags": {
    "scope_unclear_will_evolve": true,
    "architect_unresponsive_or_unproven": true,
    "no_clear_decision_maker": true,
    "schedule_compressed": true
  },
  "documentation_checklist": {
    "renovation_existing": { "existing_structural_drawings": false }
  },
  "confidence": {
    "flags": ["relationship_type", "existing_structural_drawings"]
  }
}
Key lessons: "Available" ≠ in hand = false for drawings. No architect \
named = architect: null AND architect_unresponsive_or_unproven true. \
"We worked with AVS before through a contact" = warm_unverified, not \
existing_client — unverified claims are never existing_client. \
QSR / kitchen hood scope = food_service_qsr not retail_commercial.

---

EXAMPLE 4: Clean new construction, all docs present (low risk baseline)

Document:
"Hi Mo, Ryland Development here. Building C shell at Mesa Gateway Commons \
off the Gateway corridor. 18,400 SF retail shell, new construction, steel. \
C&P is AOR, Christina is the contact. Geotech report attached. Grading plan \
attached. Schematic arch drawings attached. Targeting permit in 10 weeks, \
no hard deadlines. Ryland is the developer, I'm the principal and you can \
reach me directly for any approvals."

Correct output (abbreviated):
{
  "source_of_inquiry": "developer_direct",
  "relationship_type": "partner_referral",
  "detailed_screening": {
    "architect_status": "known_good_track_record",
    "decision_maker_clarity": "direct_contact_to_decision_maker",
    "schedule_realism": "comfortable",
    "scope_definition": "defined_inclusions_exclusions_clear"
  },
  "quick_flags": {
    "scope_unclear_will_evolve": false,
    "schedule_compressed": false,
    "hard_stop_deadline": false,
    "missing_geotech_or_drawings": false,
    "no_clear_decision_maker": false
  },
  "documentation_checklist": {
    "new_construction": {
      "geotechnical_report": true,
      "grading_plan": true,
      "architectural_drawings_schematic_plus": true
    }
  }
}
Key lessons: "Attached" = true. 10 weeks = not compressed. Principal \
reachable directly = decision_maker_clarity clear. C&P = \
known_good_track_record regardless of who submitted the inquiry. \
Developer submitting (not architect) = developer_direct, but C&P \
named means partner_referral for relationship_type.

END EXAMPLES

OUTPUT SCHEMA (your response must match this exactly):
{
  "project_name": "string",
  "inquiry_date": "string",
  "ifp_due_date": "string | null",
  "client": "string | null",
  "architect": "string | null",
  "lead_contact": "string | null",
  "city": "string | null",
  "state": "string | null",
  "square_footage": "number | null",
  "estimated_construction_cost": "number | null",
  "relationship_type": "string",
  "source_of_inquiry": "string",
  "quick_flags": {
    "scope_unclear_will_evolve": "boolean",
    "high_liability_ti": "boolean",
    "historic_adaptive_reuse": "boolean",
    "schedule_compressed": "boolean",
    "hard_stop_deadline": "boolean",
    "existing_building_no_site_access": "boolean",
    "missing_geotech_or_drawings": "boolean",
    "architect_unresponsive_or_unproven": "boolean",
    "no_clear_decision_maker": "boolean"
  },
  "detailed_screening": {
    "primary_structural_material": "string",
    "project_type": "string",
    "building_type": "string",
    "architect_status": "string",
    "architect_responsiveness": "string",
    "decision_maker_clarity": "string",
    "scope_definition": "string",
    "scope_risk_type": "string",
    "specialist_support_available": "string",
    "scope_creep_likelihood": "string",
    "schedule_realism": "string",
    "weeks_to_permit": "number | null",
    "hard_stop_deadlines": {
      "permit_board_date": "boolean",
      "gc_mobilization": "boolean",
      "lender_deadline": "boolean"
    },
    "site_access": "string",
    "docs_commitment": "string"
  },
  "documentation_checklist": {
    "new_construction": {
      "geotechnical_report": "boolean",
      "grading_plan": "boolean",
      "architectural_drawings_schematic_plus": "boolean"
    },
    "renovation_existing": {
      "existing_structural_drawings": "boolean",
      "site_visit_photos_survey": "boolean"
    },
    "all_projects": {
      "architectural_program_rfp": "boolean",
      "site_plan_with_address": "boolean",
      "preliminary_schedule_timeline": "boolean"
    }
  },
  "notes": "string",
  "confidence": {
    "scores": {
      "project_name": "string",
      "client": "string",
      "architect": "string",
      "square_footage": "string",
      "relationship_type": "string",
      "quick_flags": "string",
      "scope_definition": "string",
      "schedule": "string"
    },
    "flags": ["string"]
  }
}
"""


# ── Text extraction ────────────────────────────────────────────────────────────

def extract_text(filename: str, file_bytes: bytes) -> str:
    ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()
    if ext == "docx":
        return _text_from_docx(file_bytes)
    if ext == "pdf":
        return _text_from_pdf(file_bytes)
    if ext in ("eml", "msg"):
        return _text_from_eml(file_bytes)
    return file_bytes.decode("utf-8", errors="replace")


def _text_from_docx(buf: bytes) -> str:
    try:
        import html as _html
        with zipfile.ZipFile(io.BytesIO(buf)) as z:
            xml = z.read("word/document.xml").decode("utf-8", errors="replace")
        paras = re.findall(r"<w:p[ >].*?</w:p>", xml, re.DOTALL)
        lines = []
        for p in paras:
            texts = re.findall(r"<w:t[^>]*>([^<]*)</w:t>", p)
            line = "".join(texts)
            if line.strip():
                lines.append(_html.unescape(line))
        return "\n".join(lines)
    except Exception as exc:
        return f"[DOCX parse error: {exc}]"


def _text_from_pdf(buf: bytes) -> str:
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(buf))
        parts = [page.extract_text() or "" for page in reader.pages]
        return "\n\n".join(p for p in parts if p.strip())
    except ImportError:
        return "[PDF parsing requires pypdf — upload a .docx or .txt instead]"
    except Exception as exc:
        return f"[PDF parse error: {exc}]"


def _text_from_eml(buf: bytes) -> str:
    try:
        msg = message_from_bytes(buf, policy=_email_default)
        parts: list[str] = []
        for h in ("From", "To", "Subject", "Date"):
            v = msg.get(h)
            if v:
                parts.append(f"{h}: {v}")
        parts.append("")
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        parts.append(payload.decode("utf-8", errors="replace"))
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                parts.append(payload.decode("utf-8", errors="replace"))
        return "\n".join(parts)
    except Exception as exc:
        return f"[Email parse error: {exc}]"


# ── Validation constants ───────────────────────────────────────────────────────

_QUICK_FLAGS_KEYS = frozenset({
    "scope_unclear_will_evolve", "high_liability_ti", "historic_adaptive_reuse",
    "schedule_compressed", "hard_stop_deadline", "existing_building_no_site_access",
    "missing_geotech_or_drawings", "architect_unresponsive_or_unproven",
    "no_clear_decision_maker",
})

_SCREENING_KEYS = frozenset({
    "primary_structural_material", "project_type", "building_type",
    "architect_status", "architect_responsiveness", "decision_maker_clarity",
    "scope_definition", "scope_risk_type", "specialist_support_available",
    "scope_creep_likelihood", "schedule_realism", "weeks_to_permit",
    "hard_stop_deadlines", "site_access", "docs_commitment",
})

# Cached system prompt block for Anthropic prompt caching
_SYSTEM_BLOCK: list[dict[str, Any]] = [
    {"type": "text", "text": _SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}
]


# ── Claude extraction ─────────────────────────────────────────────────────────

def _parse_raw(raw: str) -> dict[str, Any] | None:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return None


def _validate(data: dict[str, Any]) -> bool:
    qf = data.get("quick_flags")
    ds = data.get("detailed_screening")
    if not isinstance(qf, dict) or set(qf.keys()) != _QUICK_FLAGS_KEYS:
        return False
    if not isinstance(ds, dict) or not _SCREENING_KEYS.issubset(ds.keys()):
        return False
    return True


def _word_count(text: str) -> int:
    return len(text.split())


def extract_intake_fields(text: str) -> dict[str, Any]:
    from datetime import date as _date
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured.")

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    today_str = _date.today().isoformat()

    # Chunking: only truncate large documents (>3000 words / ~15k chars)
    words = _word_count(text)
    doc_text = text if words <= 3000 else text[:15000]

    def _call(extra: str = "") -> str:
        content = (
            f"Today's date is {today_str}. When inferring or calculating any dates "
            f"(inquiry_date, ifp_due_date, weeks_to_permit), use {today_str[:4]} as the "
            f"current year unless the document explicitly states otherwise.\n\n"
            f"Document text:\n\n{doc_text}"
        )
        if extra:
            content += f"\n\n{extra}"
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            temperature=0,
            system=_SYSTEM_BLOCK,
            messages=[{"role": "user", "content": content}],
        )
        return resp.content[0].text

    # First attempt
    raw = _call()
    data = _parse_raw(raw)

    # Retry once if JSON parse failed
    if data is None:
        raw = _call("Return only valid JSON, no other text.")
        data = _parse_raw(raw)

    if data is None:
        return {"validation_failed": True}

    # Retry once if schema validation failed
    if not _validate(data):
        raw = _call("Return only valid JSON, no other text.")
        data = _parse_raw(raw)
        if data is None:
            return {"validation_failed": True}
        if not _validate(data):
            data["validation_failed"] = True

    return data
