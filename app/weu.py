"""
Weighted Effort Unit (WEU) calculation engine for AVS Operations Intelligence.

Formula chain:
  Base Load       = Tier × Phase_Coeff × QA_Buffer
  Effective Load  = Base_Load × Person_Multiplier
  Context Tax     = sum(effective) × (1 + tax_rate × (n_projects + 1))
  EIT Shadow      → added to Mentor's raw load before their own tax pass
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# 100 WEU = full capacity for a 1.0× multiplier person
CAPACITY_BASE = 10.0

TEAM_CONFIG: dict[str, dict] = {
    "MK": {"role": "President, PE",  "pool": "engineering", "multiplier": 0.2, "tax": 0.05, "is_eit": False, "mentor": None},
    "NK": {"role": "Principal, PE",  "pool": "engineering", "multiplier": 1.0, "tax": 0.05, "is_eit": False, "mentor": None},
    "RO": {"role": "Eng/PM, PE",     "pool": "engineering", "multiplier": 1.0, "tax": 0.05, "is_eit": False, "mentor": None},
    "JW": {"role": "Project Eng",    "pool": "engineering", "multiplier": 1.0, "tax": 0.05, "is_eit": False, "mentor": None},
    "RS": {"role": "CAD Mgr",        "pool": "drafting",    "multiplier": 1.0, "tax": 0.05, "is_eit": False, "mentor": None},
    "SW": {"role": "Sr CAD",         "pool": "drafting",    "multiplier": 1.0, "tax": 0.05, "is_eit": False, "mentor": None},
    "JP": {"role": "CAD Designer",   "pool": "drafting",    "multiplier": 0.8, "tax": 0.07, "is_eit": False, "mentor": None},
    "JR": {"role": "EIT",            "pool": "engineering", "multiplier": 0.8, "tax": 0.10, "is_eit": True,  "mentor": "NK"},
    "JK": {"role": "EIT",            "pool": "engineering", "multiplier": 0.8, "tax": 0.10, "is_eit": True,  "mentor": "RO"},
}

PHASE_COEFF: dict[str, float] = {
    "50%": 1.0,
    "75%": 1.2,
    "90%": 1.5,
    "IFP": 0.4,
    "RFP": 0.3,
    "DD":  0.6,
    "CA":  0.8,
    "CD":  1.0,
    "REV": 0.5,
    "SD":  0.2,
}

# Shadow load added to Mentor per EIT project phase
EIT_SHADOW_RATE: dict[str, float] = {
    "50%": 0.05,
    "75%": 0.15,
    "90%": 0.30,
}

STATUS_LABELS = {
    "best":          "Best Candidate",
    "approaching":   "Approaching",
    "at_capacity":   "At Capacity",
    "over_allocated": "Over-Allocated",
}

STATUS_COLORS = {
    "best":          "#16a34a",
    "approaching":   "#d97706",
    "at_capacity":   "#dc2626",
    "over_allocated": "#7f1d1d",
}


@dataclass
class ProjectLoad:
    event_id: str
    title: str
    phase: str
    tier: int
    base_load: float
    effective_load: float
    phase_jump: bool = False
    start_date: str = ""
    end_date: str = ""
    client: str = ""
    location: str = ""
    project_number: str = ""
    project_type: str = ""
    team: list = field(default_factory=list)


@dataclass
class PersonLoad:
    initials: str
    role: str
    pool: str
    multiplier: float
    tax_rate: float
    is_eit: bool
    mentor: Optional[str]
    capacity: float
    projects: list[ProjectLoad] = field(default_factory=list)
    shadow_load: float = 0.0  # WEU received from EIT mentees

    @property
    def active_project_count(self) -> int:
        return len(self.projects)

    @property
    def raw_load(self) -> float:
        return sum(p.effective_load for p in self.projects) + self.shadow_load

    @property
    def context_taxed_load(self) -> float:
        if self.raw_load == 0:
            return 0.0
        n = self.active_project_count
        return self.raw_load * (1.0 + self.tax_rate * (n + 1))

    @property
    def load_pct(self) -> float:
        if self.capacity == 0:
            return 0.0
        return (self.context_taxed_load / self.capacity) * 100.0

    @property
    def status(self) -> str:
        pct = self.load_pct
        if pct < 70:    return "best"
        if pct < 85:    return "approaching"
        if pct <= 100:  return "at_capacity"
        return "over_allocated"

    def to_dict(self) -> dict:
        return {
            "initials":            self.initials,
            "role":                self.role,
            "pool":                self.pool,
            "multiplier":          self.multiplier,
            "capacity":            round(self.capacity, 2),
            "active_project_count": self.active_project_count,
            "raw_load":            round(self.raw_load, 2),
            "shadow_load":         round(self.shadow_load, 2),
            "context_taxed_load":  round(self.context_taxed_load, 2),
            "load_pct":            round(self.load_pct, 1),
            "status":              self.status,
            "status_label":        STATUS_LABELS[self.status],
            "status_color":        STATUS_COLORS[self.status],
            "projects": [
                {
                    "event_id":       p.event_id,
                    "title":          p.title,
                    "phase":          p.phase,
                    "tier":           p.tier,
                    "base_load":      round(p.base_load, 2),
                    "effective_load": round(p.effective_load, 2),
                    "phase_jump":     p.phase_jump,
                    "start_date":     p.start_date,
                    "end_date":       p.end_date,
                    "client":         p.client,
                    "location":       p.location,
                    "project_number": p.project_number,
                    "project_type":   p.project_type,
                    "team":           p.team,
                }
                for p in self.projects
            ],
        }


def compute_weu(events: list[dict]) -> dict[str, PersonLoad]:
    """Return a PersonLoad for every team member given a list of active event dicts."""
    people: dict[str, PersonLoad] = {
        initials: PersonLoad(
            initials=initials,
            role=cfg["role"],
            pool=cfg["pool"],
            multiplier=cfg["multiplier"],
            tax_rate=cfg["tax"],
            is_eit=cfg["is_eit"],
            mentor=cfg.get("mentor"),
            capacity=CAPACITY_BASE * cfg["multiplier"],
        )
        for initials, cfg in TEAM_CONFIG.items()
    }

    for ev in events:
        tier = ev.get("tier")
        if not tier:
            continue  # skip events with no tier — can't calculate load

        phase = ev.get("phase") or ""
        phase_jump = bool(ev.get("phase_jump", False))
        qa_buffer = 1.15 if phase_jump else 1.0
        base_load = tier * PHASE_COEFF.get(phase, 1.0) * qa_buffer
        shadow_rate = EIT_SHADOW_RATE.get(phase, 0.0)

        title = ev.get("title") or (ev.get("project_number", "") + "-" + ev.get("client", ""))
        event_id = ev.get("id", "")

        for member in (ev.get("team") or []):
            if member not in people:
                continue
            cfg = TEAM_CONFIG[member]
            effective = base_load * cfg["multiplier"]

            people[member].projects.append(ProjectLoad(
                event_id=event_id,
                title=title,
                phase=phase,
                tier=tier,
                base_load=base_load,
                effective_load=effective,
                phase_jump=phase_jump,
                start_date=(ev.get("start_date") or "")[:10],
                end_date=(ev.get("end_date") or "")[:10],
                client=ev.get("client") or "",
                location=ev.get("location") or "",
                project_number=ev.get("project_number") or "",
                project_type=ev.get("project_type") or "",
                team=ev.get("team") or [],
            ))

            # EIT → Mentor shadow load
            if cfg["is_eit"] and cfg.get("mentor") and shadow_rate > 0:
                mentor = cfg["mentor"]
                if mentor in people:
                    people[mentor].shadow_load += base_load * shadow_rate

    return people


def get_capacity_snapshot(events: list[dict]) -> dict:
    """High-level snapshot used by the /api/capacity endpoint."""
    people = compute_weu(events)
    engineering = [p.to_dict() for k, p in people.items() if p.pool == "engineering"]
    drafting    = [p.to_dict() for k, p in people.items() if p.pool == "drafting"]

    engineering.sort(key=lambda x: -x["load_pct"])
    drafting.sort(key=lambda x: -x["load_pct"])

    return {
        "engineering": engineering,
        "drafting":    drafting,
        "summary": {
            "total_active_events": len(events),
            "events_without_tier": sum(1 for e in events if not e.get("tier")),
        },
    }
