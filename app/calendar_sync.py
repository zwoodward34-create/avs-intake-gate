from __future__ import annotations

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

_GRAPH = "https://graph.microsoft.com/v1.0"
_TOKEN_ENDPOINT = "https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}
_events_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_CACHE_TTL = 300.0  # 5 minutes


def has_config() -> bool:
    return all(
        os.environ.get(k)
        for k in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "OUTLOOK_CALENDAR_USER")
    )


def _get_token() -> str:
    now = time.monotonic()
    if _token_cache["token"] and now < _token_cache["expires_at"]:
        return _token_cache["token"]

    url = _TOKEN_ENDPOINT.format(tenant=os.environ["AZURE_TENANT_ID"])
    data = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "client_id":     os.environ["AZURE_CLIENT_ID"],
        "client_secret": os.environ["AZURE_CLIENT_SECRET"],
        "scope":         "https://graph.microsoft.com/.default",
    }).encode()

    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = json.loads(resp.read())

    _token_cache["token"] = body["access_token"]
    _token_cache["expires_at"] = now + float(body.get("expires_in", 3600)) - 60
    return _token_cache["token"]


def _graph_get(url: str, token: str) -> dict:
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Prefer":        'outlook.timezone="UTC"',
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def _fetch_ifp_events(days_ahead: int) -> dict[str, list[str]]:
    token = _get_token()
    user  = os.environ["OUTLOOK_CALENDAR_USER"]

    now   = datetime.now(timezone.utc)
    start = now.strftime("%Y-%m-%dT00:00:00Z")
    end   = (now + timedelta(days=days_ahead)).strftime("%Y-%m-%dT23:59:59Z")

    params = urllib.parse.urlencode({
        "startDateTime": start,
        "endDateTime":   end,
        "$select":       "subject,start,isAllDay",
        "$top":          "1000",
    })
    url = f"{_GRAPH}/users/{user}/calendarView?{params}"

    events_by_date: dict[str, list[str]] = {}
    while url:
        body = _graph_get(url, token)
        for ev in body.get("value", []):
            subject = str(ev.get("subject") or "")
            if "IFP" not in subject.upper():
                continue
            start_obj = ev.get("start") or {}
            # all-day events use "date"; timed events use "dateTime"
            date_str = start_obj.get("date") or (start_obj.get("dateTime") or "")[:10]
            if not date_str:
                continue
            events_by_date.setdefault(date_str, []).append(subject)
        url = body.get("@odata.nextLink")

    return events_by_date


def get_ifp_events(days_ahead: int = 180) -> dict[str, list[str]]:
    """Return {YYYY-MM-DD: [event subjects]} for all IFP calendar events."""
    now = time.monotonic()
    if _events_cache["data"] is not None and (now - _events_cache["ts"]) < _CACHE_TTL:
        return _events_cache["data"]

    if not has_config():
        return {}

    data = _fetch_ifp_events(days_ahead)
    _events_cache["data"] = data
    _events_cache["ts"]   = now
    return data


def invalidate_cache() -> None:
    _events_cache["data"] = None
    _events_cache["ts"]   = 0.0
