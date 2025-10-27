# main.py (FastAPI entrypoint, Python 3.9+ compatible)
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Query, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from dateutil import tz

# Aggregator (from pf_updates.py)
from pf_updates import fetch_updates_for_date

# Debug helpers (from pf_updates.py)
from pf_updates import _pf_get, PF_SCR_URL, PF_COND_URL

app = FastAPI(title="PF Updates Aggregator", version="1.3")

MEL_TZ = tz.gettz("Australia/Melbourne")

# ---------------------------------------------------------------------
# CORS (open; lock down allow_origins if needed)
# ---------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------
class Scratching(BaseModel):
    runner_number: Optional[int] = None
    horse_name: Optional[str] = None
    runner_id: Optional[int] = None
    updated_at: Optional[str] = None

class RaceOut(BaseModel):
    race_number: int
    scratchings: List[Scratching]

class ConditionsOut(BaseModel):
    weather: Optional[str] = None
    track_condition: Optional[str] = None
    rail: Optional[str] = None
    updated_at: Optional[str] = None

class MeetingOut(BaseModel):
    meeting_id: int
    venue: Optional[str] = None
    state: Optional[str] = None
    conditions: Optional[ConditionsOut] = None
    races: List[RaceOut]

class UpdatesOut(BaseModel):
    date: str
    meetings: List[MeetingOut]

# ---------------------------------------------------------------------
# Simple in-memory snapshot cache + helpers
# ---------------------------------------------------------------------
SNAPSHOT_CACHE: Dict[str, Dict[str, Any]] = {}     # date -> payload
PREWARM_MARKS: Dict[str, Dict[str, bool]] = {}     # date -> {"08": bool, "13": bool}

def mel_now() -> datetime:
    return datetime.now(MEL_TZ)

def mel_today_str() -> str:
    return mel_now().strftime("%Y-%m-%d")

def mel_hour_str() -> str:
    return mel_now().strftime("%H")

async def build_and_cache(date_str: str) -> Dict[str, Any]:
    data = await fetch_updates_for_date(date_str)
    SNAPSHOT_CACHE[date_str] = data
    return data

async def get_snapshot(date_str: str) -> Dict[str, Any]:
    if date_str in SNAPSHOT_CACHE:
        return SNAPSHOT_CACHE[date_str]
    return await build_and_cache(date_str)

# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.get("/")
async def root():
    return {"ok": True, "hint": "See /docs for available endpoints."}

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": mel_now().isoformat()}

@app.get("/updates/daily", response_model=UpdatesOut)
async def updates_daily(
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    meeting_id: Optional[int] = Query(None),
):
    """Live fetch of scratchings + conditions by meeting/race for a date."""
    try:
        data = await fetch_updates_for_date(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF fetch failed: {e}")

    if meeting_id is not None:
        data["meetings"] = [m for m in data["meetings"] if m.get("meeting_id") == meeting_id]
    return data

@app.get("/debug/raw")
async def debug_raw(which: str = Query(..., pattern="^(scratchings|conditions)$")):
    """Return raw PF payload so we can see exactly what PF sends back."""
    url = PF_SCR_URL if which == "scratchings" else PF_COND_URL
    data = await _pf_get(url)
    return {"count": len(data), "sample": data[:3]}

@app.get("/debug/counts")
async def debug_counts(date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """Quick sanity: how many meetings/races/scratchings did we aggregate?"""
    data = await fetch_updates_for_date(date)
    mtgs = len(data["meetings"])
    races = sum(len(m["races"]) for m in data["meetings"])
    scratches = sum(len(r["scratchings"]) for m in data["meetings"] for r in m["races"])
    return {"meetings": mtgs, "races": races, "scratchings": scratches}

# ---------------------------------------------------------------------
# Prewarm + snapshot (for twice-daily "crawl")
# ---------------------------------------------------------------------
@app.post("/tasks/prewarm")
async def prewarm(date: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """Build & cache a snapshot for a date (or today if omitted)."""
    d = date or mel_today_str()
    try:
        data = await build_and_cache(d)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF fetch failed: {e}")
    return {"stored": d, "meetings": len(data.get("meetings", []))}

@app.post("/tasks/prewarm/auto")
async def prewarm_auto():
    """
    Safe to call every 15 minutes. Only runs at 08:00 and 13:00 Melbourne time,
    once per slot per day.
    """
    d = mel_today_str()
    h = mel_hour_str()  # "00".."23"
    PREWARM_MARKS.setdefault(d, {"08": False, "13": False})

    ran = False
    if h in {"08", "13"} and not PREWARM_MARKS[d][h]:
        try:
            await build_and_cache(d)
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"PF fetch failed: {e}")
        PREWARM_MARKS[d][h] = True
        ran = True

    # tidy old marks (keep a couple of days)
    keep = sorted(PREWARM_MARKS.keys())[-3:]
    for k in list(PREWARM_MARKS.keys()):
        if k not in keep:
            PREWARM_MARKS.pop(k, None)

    return {"date": d, "hour": h, "ran": ran, "done_today": PREWARM_MARKS.get(d)}

@app.get("/snapshot/{date}")
async def snapshot_date(date: str = Path(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """Return cached snapshot for a date; if missing, compute and cache it."""
    try:
        return await get_snapshot(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF fetch failed: {e}")

@app.get("/snapshot/today")
async def snapshot_today():
    try:
        return await get_snapshot(mel_today_str())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF fetch failed: {e}")
