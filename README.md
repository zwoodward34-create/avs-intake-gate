# AVS Intake Gate
Internal intake form + decision gate for project inquiries (go / no-go / clarify / Mo review), with a live dashboard of every inquiry and decision.

## Quick start
1. Create a virtualenv (recommended):
   - `python3 -m venv .venv`
   - `source .venv/bin/activate`
2. Install dependencies:
   - `python3 -m pip install -r requirements.txt`
3. Run the server:
   - `python3 -m uvicorn app.main:app --reload --port 8000`
4. Open:
   - `http://127.0.0.1:8000`

## Running on the local office network (so Mo can access it)

### 1. Start the server bound to all network interfaces

Instead of the default `127.0.0.1` (localhost only), bind to `0.0.0.0`:

```bash
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

> Note: Drop `--reload` when running for others to access — it's for development only.

### 2. Find your machine's local IP address

**Mac:**
```bash
ipconfig getifaddr en0
```
Or open **System Settings → Network** and look at the IP next to your active connection. It will look like `192.168.x.x` or `10.x.x.x`.

**Windows:**
```cmd
ipconfig
```
Look for **IPv4 Address** under your active adapter (Wi-Fi or Ethernet).

### 3. Mo's browser URL

Once you have your IP, Mo types this in any browser on the same network:

```
http://192.168.x.x:8000
```

Replace `192.168.x.x` with your actual IP from step 2.

**Quick health check** — verify the server is reachable from any machine:
```
http://192.168.x.x:8000/health
```
Should return: `{"status": "ok"}`

### 4. Keep the server running after closing the terminal

**Mac — using `nohup`:**
```bash
nohup python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
echo $!   # prints the process ID so you can stop it later
```
To stop it later: `kill <PID>` (use the PID printed above, or find it with `lsof -i :8000`).

**Mac — using `screen`** (lets you detach and reattach):
```bash
screen -S avs
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
# Press Ctrl+A then D to detach (server keeps running)
# To reattach later: screen -r avs
```

**Windows — run in a background window:**
```powershell
Start-Process python -ArgumentList "-m uvicorn app.main:app --host 0.0.0.0 --port 8000" -WindowStyle Hidden
```
Or simply open a separate PowerShell/CMD window, start the server, and leave that window open.

Data is stored in `./data/avs_intake.sqlite3`.
To reset all data: delete `./data/avs_intake.sqlite3` and restart.

## Self-check (no server)
Runs a quick sanity check of the decision logic + database layer:
- `python3 scripts/self_check.py`

## Workflow
- Anyone fills out the intake form.
- The system computes red flags + an auto recommendation:
  - `PROCEED_TO_PROPOSAL`
  - `NEEDS_MO_REVIEW`
  - `CLARIFY_FIRST`
  - `LIKELY_DECLINE` (Mo can override)
- If flagged, Mo records the final decision (`PROCEED`, `PROCEED_WITH_CONDITIONS`, `REQUEST_CLARIFICATION`, `DECLINE`).

## Optional: simple Mo passcode
Set an environment variable to protect the Mo review page:
- `export AVS_MO_PASSCODE='some-shared-code'`

If not set, Mo review is open to anyone on the network.
