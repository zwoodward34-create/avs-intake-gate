from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Optional


_SYSTEM = (
    "You write formal structural engineering proposal letters for A.V. Schwan & Associates (AVS). "
    "Output is plain text only — no markdown, no asterisks, no bullet symbols. "
    "Follow the template and instructions exactly."
)

_DASHES = "─" * 52


def _today() -> str:
    return datetime.now().strftime("%B %-d, %Y")


def generate_proposal(
    *,
    project_name: str,
    project_type: str,
    location: str,
    building_type: str,
    approx_sf: Optional[int],
    structural_system: str,
    scope_description: str,
    architect_name: str,
    architect_firm: str,
    architect_email: str,
    fee_amount: float,
    complexity: str,
    mo_conditions: str,
    mo_notes: str,
) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured.")

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)

    today = _today()
    year = datetime.now().year
    is_bts = project_type in (
        "build_to_suit_retrofit", "tenant_improvement", "addition_expansion"
    )
    scope_last = "G" if complexity == "low" else "H"
    sf_str = f"{int(approx_sf):,} SF" if approx_sf else "TBD"
    arch_first = (architect_name or "").split()[0] if architect_name else "Sir/Madam"

    scope_lower = (scope_description or "").lower()
    if "shell" in scope_lower and ("ti" in scope_lower or "tenant" in scope_lower):
        fee_label = "Shell + TI"
    elif "shell" in scope_lower:
        fee_label = "Shell Building"
    else:
        fee_label = "Structural Engineering Services"

    contact_block = "\n".join(
        line for line in [architect_name, architect_firm, architect_email] if line
    )

    scope_instruction = (
        f"After the scope items, write a numbered modification list starting exactly with:\n"
        f"'The structural scope of this TI includes the following:'\n"
        f"then 3-8 specific structural items numbered as '1- [item].' based on this scope: {scope_description}.\n"
        f"If scope is vague, list reasonable generic BTS/TI modifications for a retail project."
    ) if is_bts else (
        f"After the scope items, write ONE paragraph describing the building's structural systems "
        f"based on: {structural_system or f'standard retail construction, {sf_str}'}. "
        f"Cover: roof structure, wall system, floor slab, and foundation. "
        f"If a prototype name or deviation clause applies, include it."
    )

    prompt = f"""Write a complete AVS structural engineering proposal letter. Plain text only.

DATE: {today}

ARCHITECT CONTACT BLOCK:
{contact_block}

SUBJECT:
Subject: {project_name}
{location}

GREETING:
{arch_first}:

OPENING:
{'For BTS/TI: Begin "We are pleased to offer the following Structural Engineering Services for the subject project, including [key modifications from scope description]." Match the Kissimmee Sprouts BTS example format.' if is_bts else 'For new construction: Begin "We are pleased to offer the following Structural Engineering Services for the subject project." Add exclusion clause only if scope mentions site walls, trellises, or other common exclusions.'}

SCOPE ITEMS (A through {scope_last}, use this exact text):
A) Preparation of all required Structural drawings and details to be incorporated into the final project set.
B) Preparation of all required Structural Engineering Calculations reflecting the final design.
C) Proofreading of all Structural sections of the Specifications (prepared by the architect of record) for conformance with the design.
D) Complete review of all final Architectural drawings with Structural drawings for Structural correctness.
E) Review of shop drawings and material submittals pertinent to the Structural Engineering for conformance with the Structural construction documents.
F) Responding to city comments as required.
G) Answering RFI's and issuing field sketches as required.
{"H) Drawings will be completed using AutoCad/Revit." if scope_last == "H" else ""}

AFTER SCOPE ITEMS:
{scope_instruction}

FEE SECTION (include this header and use exactly this dash character ─):
The fee for these Structural Engineering Services will be as follows:

{fee_label} {_DASHES} ${fee_amount:,.0f}

BOILERPLATE (copy word for word):
All of the aforementioned services to be performed to our interpretation of the structural provisions(s) to the International Building Code and the Structural Design Standards of the materials used.
Billing for all services will be monthly with payment due Net 30-days. We thank you for this opportunity to be of service and hope this proposal meets with your acceptance.

SIGNATURE BLOCK:
Respectfully submitted,

Mo Kateeb, P.E.
President

ACCEPTANCE LINE:
ACCEPTED this __________ day of ____________________, {year}
by _____________________________________ for ______________________________________

CONTEXT (do NOT include in output):
Building type: {building_type}, SF: {sf_str}, Complexity: {complexity}
Mo conditions: {mo_conditions or 'none'}
Mo notes: {mo_notes or 'none'}

Write the complete letter now, starting with the date line."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )

    return response.content[0].text.strip()
