import json
import os

import anthropic
import streamlit as st
from supabase import create_client

st.set_page_config(page_title="Backfill Project History", layout="wide")
st.title("Backfill Project History")
st.caption("Describe a past project in plain English, then verify and save the extracted details.")

# ── Clients ─────────────────────────────────────────────────────────────────

@st.cache_resource
def _supabase():
    url = os.environ["SUPABASE_URL"]
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]
    return create_client(url, key)

@st.cache_resource
def _anthropic():
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── State ────────────────────────────────────────────────────────────────────

if "extracted" not in st.session_state:
    st.session_state.extracted = None

# ── Step 1: Description input ────────────────────────────────────────────────

st.subheader("1. Describe the project")
description = st.text_area(
    "Project description",
    height=220,
    placeholder=(
        "E.g. 'The Broadway Mixed-Use Conversion, completed in 2019 in Phoenix AZ. "
        "5-story wood-frame over concrete podium, gable roof, lateral system was "
        "wood shear walls, AHJ was City of Phoenix Building Services.'"
    ),
)

if st.button("Analyze with Claude", type="primary", disabled=not description.strip()):
    with st.spinner("Extracting technical details…"):
        prompt = f"""Extract technical details from the following project description.
Return ONLY a JSON object with these exact keys (use null if unknown):
  project_name, location, year_completed, project_type,
  material, roof, lfrs, ahj, notes

Description:
{description.strip()}"""

        msg = _anthropic().messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        try:
            st.session_state.extracted = json.loads(raw.strip())
        except json.JSONDecodeError:
            st.error(f"Claude returned non-JSON output:\n\n{raw}")
            st.stop()

# ── Step 2: Editable form ────────────────────────────────────────────────────

if st.session_state.extracted:
    st.divider()
    st.subheader("2. Verify and edit extracted details")
    ex = st.session_state.extracted

    col1, col2 = st.columns(2)
    with col1:
        project_name   = st.text_input("Project Name",    value=ex.get("project_name") or "")
        location       = st.text_input("Location",        value=ex.get("location") or "")
        year_completed = st.text_input("Year Completed",  value=str(ex.get("year_completed") or ""))
        project_type   = st.text_input("Project Type",    value=ex.get("project_type") or "")
    with col2:
        material       = st.text_input("Material",        value=ex.get("material") or "")
        roof           = st.text_input("Roof System",     value=ex.get("roof") or "")
        lfrs           = st.text_input("LFRS",            value=ex.get("lfrs") or "")
        ahj            = st.text_input("AHJ",             value=ex.get("ahj") or "")

    notes = st.text_area("Notes", value=ex.get("notes") or "", height=100)

    st.divider()
    st.subheader("3. Save to Supabase")

    if st.button("Save to Official History", type="primary"):
        try:
            year_val = int(year_completed) if year_completed.strip().isdigit() else None
        except (ValueError, AttributeError):
            year_val = None

        record = {
            "project_name":    project_name.strip() or None,
            "location":        location.strip() or None,
            "year_completed":  year_val,
            "project_type":    project_type.strip() or None,
            "material":        material.strip() or None,
            "roof":            roof.strip() or None,
            "lfrs":            lfrs.strip() or None,
            "ahj":             ahj.strip() or None,
            "notes":           notes.strip() or None,
            "raw_description": description.strip() or None,
        }

        with st.spinner("Saving…"):
            resp = _supabase().table("historical_projects").insert(record).execute()

        if resp.data:
            st.success(f"Saved! Record ID: {resp.data[0].get('id', '—')}")
            st.session_state.extracted = None
        else:
            st.error("Insert returned no data. Check RLS policies or migration status.")
