# main.py (FastAPI entrypoint, Python 3.9 compatible)

from typing import List, Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from datetime import datetime
from dateutil import tz

# Aggregator
from pf_updates import fetch_updates_for_date

# Debug helpers
from pf_updates import _pf_get, PF_SCR_URL, PF_COND_URL

app = FastAPI(title="PF Updates Aggregator", version="1.2")

MEL_TZ = tz.gettz("Australia/Melbourne")

# ----------------------------
# Pydantic response models
# ----------------------------

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

# ----------------------------
# Routes
# ----------------------------

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(MEL_TZ).isoformat()}

@app.get("/updates/daily", response_model=UpdatesOut)
async def updates_daily(
    date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    meeting_id: Optional[int] = Query(None),
):
    """Daily scratchings + conditions by meeting/race."""
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
