# AVS Intake Gate  
  
## What This Is  
Internal intake and qualification gate for A.V. Schwan & Associates,   
an 11-person structural engineering firm in Scottsdale, AZ. This tool   
codifies Mo Kateeb's (President) project screening judgment into a   
structured, repeatable system.  
  
## Tech Stack  
- FastAPI + Jinja2 templates + SQLite  
- Vanilla CSS (no framework) — dark theme, custom properties in app.css  
- No JavaScript framework — vanilla JS in app.js  
- Python 3.10+  
  
## File Map  
- app/main.py — FastAPI routes (dashboard, intake CRUD, Mo review)  
- app/decision.py — Red flag computation engine (the brain)  
- app/db.py — SQLite data layer  
- app/templates/ — Jinja2 HTML templates  
- app/static/app.css — All styles  
- app/static/app.js — Minimal client-side JS  
- scripts/self_check.py — Smoke test for decision logic + DB  
  
## Run Commands  
- Start server: python3 -m uvicorn app.main:app --reload --port 8000  
- Self-check: python3 scripts/self_check.py  
- Reset data: delete ./data/avs_intake.sqlite3 and restart  
  
## Design Context  
- Target users: Zach (Operations Manager, primary), Mo (President, reviewer)  
- Use case: Screen every incoming project inquiry before proposal work begins  
- Aesthetic: Professional, utilitarian, information-dense. Think Bloomberg   
  terminal meets project management — not a marketing site. Clarity over   
  decoration. The UI should feel like a tool built by someone who uses it   
  every day.  
- The decision engine in decision.py is the core value — the UI serves it  
  
## Business Logic  
- Mo's red flags: TI with high liability, unresponsive architects,   
  compressed timelines, missing geotech/grading docs, no site access  
- Severity tiers: critical > high > medium > low  
- Recommendation outcomes: PROCEED_TO_PROPOSAL, NEEDS_MO_REVIEW,   
  CLARIFY_FIRST, LIKELY_DECLINE  
- Mo can override any recommendation  
- "Proceed to Proposal" should auto-clear without Mo review  
- Everything else needs Mo's eyes  
  
## Coding Standards  
- Keep decision.py pure (no DB or HTTP dependencies)  
- All new form fields must flow through:   
  template → _parse_intake_form() → compute_decision() → DB  
- Always run self_check.py after modifying decision logic  
- Use CSS custom properties for all colors — no hardcoded hex in templates  
