# main.py — Scratchings + Conditions snippets (FastAPI)

from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from datetime import datetime
from dateutil import tz

from pf_updates import (
    get_scratchings_flat,
    get_scratchings_grouped,
    get_conditions_flat,
)

app = FastAPI(title="PF Scratchings & Conditions API", version="0.2.0")
MEL_TZ = tz.gettz("Australia/Melbourne")

# ---- Schemas (for docs clarity) ----
class ScratchRow(BaseModel):
    meetingDate: str   # PF DD-MM-YYYY
    track: str
    raceNo: int
    tabNo: int

class ScratchFlatOut(BaseModel):
    date: str          # YYYY-MM-DD
    rows: list[ScratchRow]

class RaceOut(BaseModel):
    raceNo: int
    scratchings: list[int]

class MeetingOut(BaseModel):
    track: str
    races: list[RaceOut]

class ScratchGroupedOut(BaseModel):
    date: str
    meetings: list[MeetingOut]

class CondRow(BaseModel):
    meetingDate: str   # PF ISO, e.g. 2025-10-28T00:00:00
    track: str
    trackCondition: str
    trackConditionNumber: str

class CondFlatOut(BaseModel):
    date: str
    rows: list[CondRow]

@app.get("/")
async def root():
    return {
        "ok": True,
        "endpoints": [
            "/healthz",
            "/scratchings/flat?date=YYYY-MM-DD",
            "/scratchings/grouped?date=YYYY-MM-DD",
            "/conditions/flat?date=YYYY-MM-DD",
            "/docs",
        ],
    }

@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(MEL_TZ).isoformat()}

@app.get("/scratchings/flat", response_model=ScratchFlatOut)
async def scratchings_flat(date: Optional[str] = Query(None, description="YYYY-MM-DD; default: today (Melbourne)")):
    try:
        return await get_scratchings_flat(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF scratchings fetch failed: {e}")

@app.get("/scratchings/grouped", response_model=ScratchGroupedOut)
async def scratchings_grouped(date: Optional[str] = Query(None, description="YYYY-MM-DD; default: today (Melbourne)")):
    try:
        return await get_scratchings_grouped(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF scratchings fetch failed: {e}")

@app.get("/conditions/flat", response_model=CondFlatOut)
async def conditions_flat(date: Optional[str] = Query(None, description="YYYY-MM-DD; default: today (Melbourne)")):
    try:
        return await get_conditions_flat(date)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"PF conditions fetch failed: {e}")
