#!/usr/bin/env python3
"""
Upload the local AVS Project Database Excel file to Supabase Storage.

Usage:
    python scripts/upload_project_db.py
    python scripts/upload_project_db.py /path/to/AVS_Project_Database.xlsx

Credentials are read from a .env file in the project root, or from
environment variables already set in your shell.
"""

import os
import sys
from pathlib import Path

# ── Load .env from project root (no third-party deps needed) ─────────────────

def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
_load_dotenv(PROJECT_ROOT / ".env")

# ── Resolve file path ─────────────────────────────────────────────────────────

def _find_excel(arg) -> Path:
    if arg:
        p = Path(arg).expanduser().resolve()
        if not p.exists():
            sys.exit(f"ERROR: File not found: {p}")
        return p

    # Default: look in project root for any .xlsx matching the expected name
    candidates = sorted(PROJECT_ROOT.glob("AVS_Project_Database*.xlsx"))
    if candidates:
        return candidates[0]

    sys.exit(
        "ERROR: No Excel file found in project root.\n"
        "Pass the path explicitly:\n"
        "  python scripts/upload_project_db.py /path/to/file.xlsx"
    )


# ── Upload ────────────────────────────────────────────────────────────────────

def main() -> None:
    local_path = _find_excel(sys.argv[1] if len(sys.argv) > 1 else None)

    # Validate env vars
    missing = [
        v for v in ("SUPABASE_URL", "AVS_PROJECT_FINDER_BUCKET", "AVS_PROJECT_FINDER_OBJECT_PATH")
        if not os.environ.get(v)
    ]
    if not os.environ.get("SUPABASE_SERVICE_KEY") and not os.environ.get("SUPABASE_KEY"):
        missing.append("SUPABASE_SERVICE_KEY (or SUPABASE_KEY)")
    if missing:
        sys.exit(
            "ERROR: Missing environment variables:\n  " + "\n  ".join(missing) +
            "\n\nCreate a .env file in the project root — see .env.example"
        )

    url    = os.environ["SUPABASE_URL"]
    key    = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]
    bucket = os.environ["AVS_PROJECT_FINDER_BUCKET"]
    dest   = os.environ["AVS_PROJECT_FINDER_OBJECT_PATH"]

    from supabase import create_client

    print(f"  File   : {local_path}")
    print(f"  Bucket : {bucket}")
    print(f"  Dest   : {dest}")
    print()

    data = local_path.read_bytes()
    client = create_client(url, key)

    # Try update first; if it fails (file doesn't exist yet) fall back to upload
    try:
        client.storage.from_(bucket).update(
            dest, data,
            file_options={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                          "upsert": "true"}
        )
    except Exception:
        client.storage.from_(bucket).upload(
            dest, data,
            file_options={"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        )

    size_kb = len(data) / 1024
    print(f"Uploaded {size_kb:.1f} KB successfully.")
    print("The web app will reflect the update within 5 minutes.")


if __name__ == "__main__":
    main()
