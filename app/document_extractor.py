from __future__ import annotations

import io
import json
import os
import re
import zipfile
from email import message_from_bytes
from email.policy import default as _email_default
from typing import Any

_EXTRACTION_PROMPT = """\
You are extracting intake data from a structural engineering project RFP, email, or proposal document \
for A.V. Schwan & Associates (AVS), a structural engineering firm.

Extract the following fields and return ONLY a valid JSON object (no markdown, no commentary):

{
  "project_name": "Full project name, e.g. 'Sprouts Farmers Market – Kissimmee FL'",
  "client_name": "End client/tenant, e.g. 'Sprouts Farmers Market'",
  "architect_name": "Architect contact full name",
  "architect_firm": "Architect firm name",
  "lead_contact": "Architect email or phone",
  "location_region": "City and state, e.g. 'Kissimmee, FL'",
  "project_address": "Full street address if present, else null",
  "approx_sf": 25000,
  "project_type": "new_construction | build_to_suit_retrofit | tenant_improvement | addition_expansion | repeating_program | one_off_unique",
  "building_type": "retail | warehouse | office | healthcare | education | mixed_use | data_center | other",
  "scope_description": "1-3 sentence scope summary",
  "structural_system": "Structural system description if mentioned, else null",
  "deadline_date": "YYYY-MM-DD if any deadline mentioned, else null",
  "est_construction_cost": null
}

Rules:
- approx_sf must be an integer or null (no commas or units)
- est_construction_cost must be a number (no $ or commas) or null
- project_type: 'build_to_suit_retrofit' for BTS/retrofit projects; 'tenant_improvement' for TI; \
'new_construction' for ground-up; 'repeating_program' for standard client rollouts (e.g. standard Sprouts prototype)
- Use null for any field not found

Document text follows:
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


# ── Claude extraction ─────────────────────────────────────────────────────────

def extract_intake_fields(text: str) -> dict[str, Any]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured.")

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    truncated = text[:15000] if len(text) > 15000 else text

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": _EXTRACTION_PROMPT + truncated}],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            return json.loads(m.group())
        return {}
