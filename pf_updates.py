# pf_updates.py
# Minimal PF Updates: Scratchings + Conditions snippets.
# - Scratchings returns only: meetingDate (PF DD-MM-YYYY), track, raceNo, tabNo
# - Conditions returns only: meetingDate (PF ISO), track, trackCondition, trackConditionNumber
# - Dates are filtered by YYYY-MM-DD (Melbourne "today" if not provided)

import os
import httpx
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from dateutil import tz

PF_API_KEY = os.getenv("PF_API_KEY", "C4DA191C-3155-4333-88DA-EDB0D92A40D6")
PF_SCR_URL = "https://api.puntingform.com.au/v2/Updates/Scratchings"
PF_COND_URL = "https://api.puntingform.com.au/v2/Updates/Conditions"

MEL_TZ = tz.gettz("Australia/Melbourne")

# ----------------------------
# Date helpers
# ----------------------------
def _today_mel() -> str:
    return datetime.now(MEL_TZ).strftime("%Y-%m-%d")

def _parse_pf_meeting_date_scratch(md: Optional[str]) -> Optional[str]:
    """Scratchings meetingDate is DD-MM-YYYY. Return YYYY-MM-DD for filtering."""
    if not md:
        return None
    md = md.strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(md, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return None

def _parse_pf_meeting_date_cond(md: Optional[str]) -> Optional[str]:
    """Conditions meetingDate is ISO (e.g., 2025-10-28T00:00:00). Return YYYY-MM-DD."""
    if not md:
        return None
    try:
        # Trim timezone if present; we only need the date portion.
        dt = datetime.fromisoformat(md.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        # Fallback: take first 10 chars if it looks like YYYY-MM-DD...
        if len(md) >= 10 and md[4] == "-" and md[7] == "-":
            return md[:10]
        return None

# ----------------------------
# HTTP
# ----------------------------
async def _pf_get_json(url: str, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """PF fetch with both header & query auth styles tried."""
    if not PF_API_KEY:
        raise RuntimeError("PF_API_KEY not set")
    params = extra_params.copy() if extra_params else {}
    attempts: List[Tuple[Dict[str, str], Dict[str, Any]]] = [
        ({"accept": "application/json"}, {"apiKey": PF_API_KEY, **params}),
        ({"X-Api-Key": PF_API_KEY, "accept": "application/json"}, {**params}),
        ({"x-api-key": PF_API_KEY, "accept": "application/json"}, {**params}),
        ({"apiKey": PF_API_KEY, "accept": "application/json"}, {**params}),
    ]
    last_err: Optional[str] = None
    async with httpx.AsyncClient(timeout=25.0) as client:
        for headers, qs in attempts:
            try:
                r = await client.get(url, headers=headers, params=qs)
                if r.status_code == 200:
                    payload = r.json()
                    if isinstance(payload, list):
                        return payload
                    if isinstance(payload, dict):
                        for k in ("payLoad", "data", "items", "result"):
                            v = payload.get(k)
                            if isinstance(v, list):
                                return v
                        return [payload]
                else:
                    if r.status_code in (401, 403):
                        last_err = f"{r.status_code} {r.text[:200]}"
                        continue
                    r.raise_for_status()
            except Exception as e:
                last_err = str(e)
                continue
    raise httpx.HTTPStatusError(f"PF fetch failed for {url}: {last_err}", request=None, response=None)

# ----------------------------
# Scratchings (minimal)
# ----------------------------
def _scr_row_min(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    PF scratchings row -> {meetingDate, track, raceNo, tabNo}
    Keep PF meetingDate format (DD-MM-YYYY) in output, as requested.
    """
    md = row.get("meetingDate")
    track = row.get("track")
    race_no = row.get("raceNo")
    tab_no = row.get("tabNo")
    if md is None or track is None or race_no is None or tab_no is None:
        return None
    try:
        race_no = int(race_no)
        tab_no = int(tab_no)
    except Exception:
        return None
    return {
        "meetingDate": md,
        "track": str(track),
        "raceNo": race_no,
        "tabNo": tab_no,
    }

def _filter_scratchings_by_date(rows: List[Dict[str, Any]], target_yyyy_mm_dd: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        md_norm = _parse_pf_meeting_date_scratch(r.get("meetingDate"))
        if md_norm == target_yyyy_mm_dd:
            out.append(r)
    return out

def _group_scratchings(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # meetings dict: track -> raceNo -> set(tabNo)
    meetings: Dict[str, Dict[int, set]] = {}
    for r in rows:
        track = r["track"].strip()
        rn = int(r["raceNo"])
        tn = int(r["tabNo"])
        meetings.setdefault(track, {}).setdefault(rn, set()).add(tn)

    out_meetings: List[Dict[str, Any]] = []
    for track, races in meetings.items():
        out_meetings.append({
            "track": track,
            "races": [
                {"raceNo": race_no, "scratchings": sorted(list(tabs))}
                for race_no, tabs in sorted(races.items(), key=lambda x: x[0])
            ]
        })
    return {"meetings": out_meetings}

async def get_scratchings_flat(date_yyyy_mm_dd: Optional[str]) -> Dict[str, Any]:
    target = date_yyyy_mm_dd or _today_mel()
    raw = await _pf_get_json(PF_SCR_URL)
    minimal = [m for m in (_scr_row_min(x) for x in raw) if m is not None]
    minimal = _filter_scratchings_by_date(minimal, target)
    return {"date": target, "rows": minimal}

async def get_scratchings_grouped(date_yyyy_mm_dd: Optional[str]) -> Dict[str, Any]:
    target = date_yyyy_mm_dd or _today_mel()
    raw = await _pf_get_json(PF_SCR_URL)
    minimal = [m for m in (_scr_row_min(x) for x in raw) if m is not None]
    minimal = _filter_scratchings_by_date(minimal, target)
    grouped = _group_scratchings(minimal)
    grouped["date"] = target
    return grouped

# ----------------------------
# Conditions (minimal)
# ----------------------------
def _cond_row_min(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    PF conditions row -> {meetingDate, track, trackCondition, trackConditionNumber}
    Keep PF meetingDate ISO string as-is in output.
    """
    md = row.get("meetingDate")
    track = row.get("track")
    tc = row.get("trackCondition")
    tcn = row.get("trackConditionNumber")
    if md is None or track is None or tc is None or tcn is None:
        return None
    # ensure number is a string (PF often gives "4" already)
    tcn_str = str(tcn).strip()
    if not tcn_str:
        return None
    return {
        "meetingDate": md,
        "track": str(track),
        "trackCondition": str(tc),
        "trackConditionNumber": tcn_str,
    }

def _filter_conditions_by_date(rows: List[Dict[str, Any]], target_yyyy_mm_dd: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        md_norm = _parse_pf_meeting_date_cond(r.get("meetingDate"))
        if md_norm == target_yyyy_mm_dd:
            out.append(r)
    return out

async def get_conditions_flat(date_yyyy_mm_dd: Optional[str]) -> Dict[str, Any]:
    target = date_yyyy_mm_dd or _today_mel()
    raw = await _pf_get_json(PF_COND_URL)
    minimal = [m for m in (_cond_row_min(x) for x in raw) if m is not None]
    minimal = _filter_conditions_by_date(minimal, target)
    return {"date": target, "rows": minimal}
