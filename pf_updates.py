# pf_updates.py
# Aggregates PF Updates (Conditions + Scratchings) by meeting/race.
# - Prefer freshest track_condition via Updates/Conditions?meetingId (overrides stale values)
# - Augment scratchings from fields using broad status detection (Scratched/Withdrawn/etc.)
# - Name-based identity within (meeting_id, race_number)
# - runner_number optional (backfilled when available)
# Python 3.9 compatible.

import os
import re
import csv
import io
import unicodedata
import asyncio
import httpx
from datetime import datetime, timezone
from dateutil import tz
from typing import Any, Dict, List, Optional, Set, Tuple

# Updates
PF_SCR_URL = "https://api.puntingform.com.au/v2/Updates/Scratchings"
PF_COND_URL = "https://api.puntingform.com.au/v2/Updates/Conditions"

# Fields / meeting-level (for optional backfill)
PF_FIELDS_JSON_URL = "https://api.puntingform.com.au/v2/form/fields"
PF_FIELDS_CSV_URL  = "https://api.puntingform.com.au/v2/form/fields/csv"
PF_MEETING_CSV_URL = "https://api.puntingform.com.au/v2/form/meeting/csv"
PF_FORM_URL        = "https://api.puntingform.com.au/v2/form/form"

PF_API_KEY = os.getenv("PF_API_KEY")
MEL_TZ = tz.gettz("Australia/Melbourne")

# ----------------------------
# Helpers: normalisation, parsing
# ----------------------------

def _snakify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.strip().lower())

FIELD_ALIASES = {
    # meeting
    "meeting_id": {"meetingid", "meeting_id", "meet_id", "meeting"},
    "meeting_date": {"meetingdate", "meeting_date", "date"},
    "venue": {"venue", "track", "course", "meeting_name"},
    "state": {"state", "jurisdiction", "country"},
    # race
    "race_id": {"raceid", "race_id"},
    "race_number": {"racenumber", "race_number", "raceno", "race_no", "race"},
    # runner
    "runner_id": {"runnerid", "runner_id"},
    "runner_number": {
        "runnernumber", "runner_number", "number", "no",
        "saddlenumber", "saddle_number", "saddle_no", "saddle",
        "programnumber", "program_number", "progno", "prog_no",
        "book_number", "numberinbook", "number_in_book",
        "tabno", "tab_number", "cloth", "cloth_number",
    },
    "horse_name": {"horse", "horse_name", "runnername", "name"},
    # conditions (do NOT include generic "track")
    "track_condition": {"track_condition", "trackrating", "track_rating", "rating", "condition", "rating_code"},
    "weather": {"weather", "weather_desc", "weatherdescription"},
    "rail": {"rail", "rail_position", "railposition"},
    # status / timestamps
    "scratched": {"scratched", "is_scratched", "scratch"},
    "updated_at": {"updated", "updated_at", "modified", "lastupdated"},
}

def _canonise(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    lower = {_snakify(k): v for k, v in d.items()}
    for canon, aliases in FIELD_ALIASES.items():
        for a in aliases:
            if a in lower:
                out[canon] = lower[a]
                break
    for k, v in lower.items():
        if k not in out:
            out[k] = v
    return out

def _parse_int(x: Any) -> Optional[int]:
    try:
        v = int(x)
        return v if v != 0 else None
    except Exception:
        return None

def _parse_bool(x: Any) -> Optional[bool]:
    if x is None: return None
    if isinstance(x, bool): return x
    s = str(x).strip().lower()
    if s in {"1", "true", "y", "yes", "t"}: return True
    if s in {"0", "false", "n", "no", "f"}: return False
    return None

def _norm_venue(v: Optional[str]) -> Optional[str]:
    if not v: return None
    s = str(v)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\b(racecourse|racetrack)\b", "", s, flags=re.I)
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s or None

def _to_mel_dt(x: Any) -> Optional[datetime]:
    if not x:
        return None
    try:
        dt = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return dt.astimezone(MEL_TZ)

def _display_track_condition(raw: Optional[str]) -> Optional[str]:
    if not raw: return None
    s = str(raw).strip()
    s = re.sub(r"\([^)]*\)", "", s).strip()
    if re.fullmatch(r"(?i)(synthetic|polytrack|tapeta|all\s*weather|aw|syn)", s):
        return "Synthetic"
    m_code = re.fullmatch(r"\s*([GFHDSLgfhdsl])\s*([0-9]{1,2})\s*", s)
    if m_code:
        table = {"g": "Good", "s": "Soft", "h": "Heavy", "f": "Firm", "d": "Dead", "l": "Slow"}
        label = table.get(m_code.group(1).lower())
        if label:
            return f"{label}{m_code.group(2)}"
    m = re.search(r"(?i)\b(Good|Soft|Heavy|Firm|Dead|Slow)\s*([0-9]{1,2})\b", s)
    if m:
        return f"{m.group(1).capitalize()}{m.group(2)}"
    return None

def _extract_track_condition(cond: Dict[str, Any], venue: Optional[str]) -> Optional[str]:
    v_norm = (str(venue or "").strip().lower() or None)
    candidates = [
        cond.get("track_condition"),
        cond.get("trackrating"),
        cond.get("track_rating"),
        cond.get("rating"),
        cond.get("condition"),
        cond.get("rating_code"),
        cond.get("track_condition_code"),
        cond.get("rating_short"),
        cond.get("going"),
        cond.get("going_desc"),
        cond.get("going_description"),
        cond.get("surface"),
    ]
    for c in candidates:
        if not c: continue
        if v_norm and str(c).strip().lower() == v_norm:
            continue
        tc = _display_track_condition(c)
        if tc: return tc
    return None

def _norm_name(name: Optional[str]) -> Optional[str]:
    if not name: return None
    s = unicodedata.normalize("NFKD", str(name))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s or None

# ----------------------------
# HTTP (JSON & CSV)
# ----------------------------

async def _http_get(client: httpx.AsyncClient, url: str, headers: Dict[str, str], params: Dict[str, Any]) -> httpx.Response:
    return await client.get(url, headers=headers, params=params)

async def _pf_get_json(url: str, timeout: float = 20.0, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    key = PF_API_KEY
    if not key:
        raise RuntimeError("PF_API_KEY not set")
    qparams = extra_params.copy() if extra_params else {}
    async with httpx.AsyncClient(timeout=timeout) as client:
        attempts = [
            ({"accept": "application/json"}, {"apiKey": key, **qparams}),
            ({"X-Api-Key": key, "accept": "application/json"}, {**qparams}),
            ({"x-api-key": key, "accept": "application/json"}, {**qparams}),
            ({"apiKey": key,   "accept": "application/json"}, {**qparams}),
        ]
        last_err: Optional[str] = None
        for headers, params in attempts:
            try:
                r = await _http_get(client, url, headers, params)
                if r.status_code == 200:
                    payload = r.json()
                    if isinstance(payload, list): return payload
                    if isinstance(payload, dict):
                        if "statusCode" in payload and payload.get("statusCode") not in (200, 201, None):
                            last_err = f"{payload.get('statusCode')} {payload.get('error')}"
                            continue
                        for k in ("payLoad", "data", "items", "result"):
                            if k in payload and isinstance(payload[k], list):
                                return payload[k]
                        return [payload]
                else:
                    if r.status_code in (401, 403):
                        last_err = f"{r.status_code} {r.text[:200]}"
                        continue
                    r.raise_for_status()
            except Exception as e:
                last_err = str(e)
                continue
        raise httpx.HTTPStatusError(f"PF JSON auth/parse failed for {url}: {last_err}", request=None, response=None)

async def _pf_get_csv(url: str, timeout: float = 20.0, extra_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    key = PF_API_KEY
    if not key:
        raise RuntimeError("PF_API_KEY not set")
    qparams = extra_params.copy() if extra_params else {}
    async with httpx.AsyncClient(timeout=timeout) as client:
        attempts = [
            ({"accept": "text/csv"}, {"apiKey": key, **qparams}),
            ({"X-Api-Key": key, "accept": "text/csv"}, {**qparams}),
            ({"x-api-key": key, "accept": "text/csv"}, {**qparams}),
            ({"apiKey": key,   "accept": "text/csv"}, {**qparams}),
        ]
        last_err: Optional[str] = None
        for headers, params in attempts:
            try:
                r = await _http_get(client, url, headers, params)
                if r.status_code == 200:
                    text = r.text or ""
                    if not text.strip():
                        return []
                    buff = io.StringIO(text.strip("\ufeff\r\n"))
                    reader = csv.DictReader(buff)
                    return [dict(row) for row in reader]
                else:
                    if r.status_code in (401, 403):
                        last_err = f"{r.status_code} {r.text[:200]}"
                        continue
                    r.raise_for_status()
            except Exception as e:
                last_err = str(e)
                continue
        return []

# Back-compat alias (older main.py debug imports)
async def _pf_get(url: str, timeout: float = 20.0, extra_params: Optional[Dict[str, Any]] = None):
    return await _pf_get_json(url, timeout=timeout, extra_params=extra_params)

# ----------------------------
# Scratch status extraction from fields
# ----------------------------

_SCR_KEYS_HINTS = {
    "scratched", "is_scratched", "scratch", "scratching",
    "status", "runner_status", "status_desc", "runner_status_desc",
    "runnerstate", "runner_state", "withdrawn", "wd", "wdr"
}

def _is_scratched_value(v: Any) -> Optional[bool]:
    """Parse booleans and common text codes for scratched/withdrawn."""
    b = _parse_bool(v)
    if b is not None:
        return b
    if v is None:
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    # obvious words
    if "scratch" in s:
        return True
    if s in {"withdrawn", "wd", "wdr", "late wd", "late w/d", "w/d"}:
        return True
    if s in {"non-runner", "non runner", "nr"}:
        return True
    # common opposite codes
    if s in {"em", "emer", "emergency"}:
        return False
    return None

def _extract_scratched_from_row(row: Dict[str, Any]) -> Optional[bool]:
    # preferred explicit keys
    for k in row.keys():
        if not isinstance(k, str):
            continue
        kk = k.strip().lower().replace(" ", "_")
        if kk in _SCR_KEYS_HINTS:
            v = row[k]
            got = _is_scratched_value(v)
            if got is not None:
                return got
    # generic status probe: scan short string fields
    for v in row.values():
        if isinstance(v, str) and len(v) <= 64:
            got = _is_scratched_value(v)
            if got is True:
                return True
    return None

# ----------------------------
# Backfill caches (fields index)
# ----------------------------

_FIELDS_ID_INDEX: Dict[int, Dict[int, Dict[str, Any]]] = {}
_FIELDS_NAME_INDEX: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}

def _index_add(meeting_id: int, row: Dict[str, Any]):
    row = _canonise(row)
    rid = _parse_int(row.get("runner_id"))
    rn  = _parse_int(row.get("runner_number"))
    nm  = row.get("horse_name")
    rno = _parse_int(row.get("race_number"))
    scr = _extract_scratched_from_row(row)
    if scr is None:
        scr = _parse_bool(row.get("scratched"))

    rec = {
        "runner_number": rn,
        "horse_name": nm,
        "race_number": rno,
        "scratched": scr,
        "runner_id": rid,
    }
    if rid is not None:
        _FIELDS_ID_INDEX.setdefault(meeting_id, {})[rid] = rec
    nkey = _norm_name(nm)
    if nkey:
        _FIELDS_NAME_INDEX.setdefault(meeting_id, {}).setdefault(nkey, []).append(rec)

async def _fetch_meeting_fields(meeting_id: int, venue: Optional[str], meeting_date: Optional[str]) -> None:
    if meeting_id in _FIELDS_ID_INDEX:
        return

    # 1) meeting/csv by meetingId
    try:
        rows = await _pf_get_csv(PF_MEETING_CSV_URL, extra_params={"meetingId": meeting_id})
        for raw in rows or []:
            _index_add(meeting_id, raw)
    except Exception:
        pass

    # 2) form/form by meetingId (raceNumber=0 -> all races)
    if meeting_id not in _FIELDS_ID_INDEX:
        try:
            rows = await _pf_get_json(PF_FORM_URL, extra_params={"meetingId": meeting_id, "raceNumber": 0})
            flat: List[Dict[str, Any]] = []
            if rows and any("runner_id" in _canonise(x) for x in rows):
                flat = rows
            else:
                for obj in rows or []:
                    o = _canonise(obj)
                    for nest_key in ("runners", "fields", "entries", "acceptances"):
                        nested = o.get(nest_key)
                        if isinstance(nested, list):
                            rno = _parse_int(o.get("race_number"))
                            for n in nested:
                                rr = _canonise(n)
                                if rno is not None and "race_number" not in rr:
                                    rr["race_number"] = rno
                                flat.append(rr)
            for raw in flat:
                _index_add(meeting_id, raw)
        except Exception:
            pass

    # 3/4/5) date+venue fallbacks
    if meeting_id not in _FIELDS_ID_INDEX and venue and meeting_date:
        date_keys = ["meetingDate", "meeting_date", "date"]
        venue_keys = ["venue", "track", "course", "meeting"]
        vn_list: List[Optional[str]] = []
        seen = set()
        for v in [venue, _norm_venue(venue)]:
            key = (v or "").lower()
            if key and key not in seen:
                vn_list.append(v); seen.add(key)

        # 3) fields JSON by date+venue
        for dk in date_keys:
            for vtry in vn_list:
                for vk in venue_keys:
                    try:
                        rows = await _pf_get_json(PF_FIELDS_JSON_URL, extra_params={dk: meeting_date, vk: vtry})
                        for raw in rows or []:
                            _index_add(meeting_id, raw)
                        if meeting_id in _FIELDS_ID_INDEX:
                            break
                    except Exception:
                        continue
                if meeting_id in _FIELDS_ID_INDEX:
                    break
            if meeting_id in _FIELDS_ID_INDEX:
                break

        # 4) meeting CSV by date+venue
        if meeting_id not in _FIELDS_ID_INDEX:
            for dk in date_keys:
                for vtry in vn_list:
                    for vk in venue_keys:
                        try:
                            rows = await _pf_get_csv(PF_MEETING_CSV_URL, extra_params={dk: meeting_date, vk: vtry})
                            for raw in rows or []:
                                _index_add(meeting_id, raw)
                            if meeting_id in _FIELDS_ID_INDEX:
                                break
                        except Exception:
                            continue
                    if meeting_id in _FIELDS_ID_INDEX:
                        break
                if meeting_id in _FIELDS_ID_INDEX:
                    break

        # 5) fields CSV by date+venue
        if meeting_id not in _FIELDS_ID_INDEX:
            for dk in date_keys:
                for vtry in vn_list:
                    for vk in venue_keys:
                        try:
                            rows = await _pf_get_csv(PF_FIELDS_CSV_URL, extra_params={dk: meeting_date, vk: vtry})
                            for raw in rows or []:
                                _index_add(meeting_id, raw)
                            if meeting_id in _FIELDS_ID_INDEX:
                                break
                        except Exception:
                            continue
                    if meeting_id in _FIELDS_ID_INDEX:
                        break
                if meeting_id in _FIELDS_ID_INDEX:
                    break

async def _build_runner_index_for_meeting(meeting_id: int, races_needed: Set[int], venue: Optional[str], meeting_date: Optional[str]) -> Dict[int, Dict[str, Any]]:
    await _fetch_meeting_fields(meeting_id, venue, meeting_date)
    have_idx = _FIELDS_ID_INDEX.get(meeting_id, {})
    missing_races: List[int] = []
    for rno in sorted(races_needed):
        if not any(v.get("race_number") == rno for v in have_idx.values() if isinstance(v, dict)):
            missing_races.append(rno)

    if missing_races:
        results = await asyncio.gather(*(
            _pf_get_json(PF_FIELDS_JSON_URL, extra_params={"meetingId": meeting_id, "raceNumber": r})
            for r in missing_races
        ), return_exceptions=True)
        for rows in results:
            if isinstance(rows, Exception): continue
            for raw in rows or []:
                _index_add(meeting_id, raw)

    have_idx = _FIELDS_ID_INDEX.get(meeting_id, {})
    still_missing: List[int] = []
    for rno in sorted(missing_races):
        if not any(v.get("race_number") == rno for v in have_idx.values() if isinstance(v, dict)):
            still_missing.append(rno)
    if still_missing:
        results = await asyncio.gather(*(
            _pf_get_csv(PF_FIELDS_CSV_URL, extra_params={"meetingId": meeting_id, "raceNumber": r})
            for r in still_missing
        ), return_exceptions=True)
        for rows in results:
            if isinstance(rows, Exception): continue
            for raw in rows or []:
                _index_add(meeting_id, raw)

    return _FIELDS_ID_INDEX.get(meeting_id, {})

# ----------------------------
# Date filter
# ----------------------------

def _same_day_mel(dt_str: Optional[str], target_date: str) -> bool:
    if not target_date: return True
    if dt_str and re.match(r"^\d{4}-\d{2}-\d{2}$", str(dt_str)):
        return str(dt_str) == target_date
    return True

# ----------------------------
# Conditions backfill (prefer freshest + override)
# ----------------------------

def _scan_for_rating(value: Any, venue: Optional[str]) -> Optional[str]:
    v_norm = (str(venue or "").strip().lower() or None)

    def _check_str(s: str) -> Optional[str]:
        s2 = s.strip()
        if not s2: return None
        if v_norm and s2.lower() == v_norm: return None
        return _display_track_condition(s2)

    if isinstance(value, str):
        return _check_str(value)
    if isinstance(value, dict):
        for _k, v in value.items():
            if isinstance(v, str):
                tc = _check_str(v)
                if tc: return tc
            tc = _scan_for_rating(v, venue)
            if tc: return tc
        return None
    if isinstance(value, list):
        for it in value:
            tc = _scan_for_rating(it, venue)
            if tc: return tc
        return None
    return None

async def _try_meeting_condition_from_updates(mid: int) -> Optional[str]:
    """Try Updates/Conditions scoped to meetingId (usually freshest)."""
    try:
        rows = await _pf_get_json(PF_COND_URL, extra_params={"meetingId": mid})
        for r in rows or []:
            c = _canonise(r)
            tc = _extract_track_condition(c, c.get("venue"))
            if tc:
                return tc
    except Exception:
        pass
    return None

async def _try_meeting_condition_from_json(meeting_id: int, venue: Optional[str], meeting_date: Optional[str]) -> Optional[str]:
    try:
        rows = await _pf_get_json(PF_FORM_URL, extra_params={"meetingId": meeting_id, "raceNumber": 0})
        for obj in rows or []:
            if isinstance(obj, dict):
                tc = _scan_for_rating(obj, venue)
                if tc: return tc
    except Exception:
        pass
    if meeting_date and venue:
        vn_list, seen = [], set()
        for v in [venue, _norm_venue(venue)]:
            key = (v or "").lower()
            if key and key not in seen:
                vn_list.append(v); seen.add(key)
        for dk in ["meetingDate", "meeting_date", "date"]:
            for vtry in vn_list:
                for vk in ["venue", "track", "course", "meeting"]:
                    try:
                        rows = await _pf_get_json(PF_FIELDS_JSON_URL, extra_params={dk: meeting_date, vk: vtry})
                        for obj in rows or []:
                            tc = _scan_for_rating(obj, venue)
                            if tc: return tc
                    except Exception:
                        continue
    return None

async def _try_meeting_condition_from_csv(meeting_id: int, venue: Optional[str], meeting_date: Optional[str]) -> Optional[str]:
    try:
        rows = await _pf_get_csv(PF_MEETING_CSV_URL, extra_params={"meetingId": meeting_id})
        for row in rows or []:
            tc = _scan_for_rating(row, venue)
            if tc: return tc
    except Exception:
        pass
    if meeting_date and venue:
        vn_list, seen = [], set()
        for v in [venue, _norm_venue(venue)]:
            key = (v or "").lower()
            if key and key not in seen:
                vn_list.append(v); seen.add(key)
        for dk in ["meetingDate", "meeting_date", "date"]:
            for vtry in vn_list:
                for vk in ["venue", "track", "course", "meeting"]:
                    try:
                        rows = await _pf_get_csv(PF_MEETING_CSV_URL, extra_params={dk: meeting_date, vk: vtry})
                        for row in rows or []:
                            tc = _scan_for_rating(row, venue)
                            if tc: return tc
                    except Exception:
                        continue
                    try:
                        rows = await _pf_get_csv(PF_FIELDS_CSV_URL, extra_params={dk: meeting_date, vk: vtry})
                        for row in rows or []:
                            tc = _scan_for_rating(row, venue)
                            if tc: return tc
                    except Exception:
                        continue
    return None

async def _backfill_track_condition_for_meetings(meetings: Dict[int, Dict[str, Any]], target_date: str) -> None:
    """
    Prefer Updates(meetingId) and allow it to override an existing rating.
    Then JSON → CSV for any meeting still missing a rating.
    """
    # 0) Always try Updates(meetingId) and override if different
    mids = list(meetings.keys())
    upd_results = await asyncio.gather(*(
        _try_meeting_condition_from_updates(mid) for mid in mids
    ), return_exceptions=True)

    for mid, res in zip(mids, upd_results):
        if isinstance(res, Exception) or not res:
            continue
        m = meetings[mid]
        if not m.get("conditions"):
            m["conditions"] = {"weather": None, "track_condition": res, "rail": None, "updated_at": None}
        else:
            # override if different (assume Updates is freshest)
            if m["conditions"].get("track_condition") != res:
                m["conditions"]["track_condition"] = res

    # 1) For any still missing, try JSON
    need_json = [mid for mid, m in meetings.items() if not (m.get("conditions") and m["conditions"].get("track_condition"))]
    if need_json:
        json_results = await asyncio.gather(*(
            _try_meeting_condition_from_json(mid, meetings[mid].get("venue"), meetings[mid].get("meeting_date") or target_date)
            for mid in need_json
        ), return_exceptions=True)
        for mid, res in zip(need_json, json_results):
            if isinstance(res, Exception) or not res:
                continue
            m = meetings[mid]
            if not m.get("conditions"):
                m["conditions"] = {"weather": None, "track_condition": res, "rail": None, "updated_at": None}
            else:
                m["conditions"]["track_condition"] = res

    # 2) Finally CSV
    need_csv = [mid for mid, m in meetings.items() if not (m.get("conditions") and m["conditions"].get("track_condition"))]
    if need_csv:
        csv_results = await asyncio.gather(*(
            _try_meeting_condition_from_csv(mid, meetings[mid].get("venue"), meetings[mid].get("meeting_date") or target_date)
            for mid in need_csv
        ), return_exceptions=True)
        for mid, res in zip(need_csv, csv_results):
            if isinstance(res, Exception) or not res:
                continue
            m = meetings[mid]
            if not m.get("conditions"):
                m["conditions"] = {"weather": None, "track_condition": res, "rail": None, "updated_at": None}
            else:
                m["conditions"]["track_condition"] = res

# ----------------------------
# Fetch helpers with multi-parameter tries (reduce partial days)
# ----------------------------

async def _fetch_updates_scratchings_for_date(target_date: str) -> List[Dict[str, Any]]:
    tries = [
        {},
        {"date": target_date},
        {"meetingDate": target_date},
        {"date_from": target_date, "date_to": target_date},
        {"startDate": target_date, "endDate": target_date},
    ]
    seen = set()
    out: List[Dict[str, Any]] = []
    for p in tries:
        try:
            rows = await _pf_get_json(PF_SCR_URL, extra_params=p)
            for r in rows or []:
                c = _canonise(r)
                key = (c.get("meeting_id"), c.get("race_number"), c.get("runner_id"), _norm_name(c.get("horse_name")))
                if key not in seen:
                    seen.add(key)
                    out.append(c)
        except Exception:
            continue
    return out

async def _fetch_updates_conditions_for_date(target_date: str) -> List[Dict[str, Any]]:
    tries = [
        {},
        {"date": target_date},
        {"meetingDate": target_date},
        {"date_from": target_date, "date_to": target_date},
        {"startDate": target_date, "endDate": target_date},
    ]
    seen = set()
    out: List[Dict[str, Any]] = []
    for p in tries:
        try:
            rows = await _pf_get_json(PF_COND_URL, extra_params=p)
            for r in rows or []:
                c = _canonise(r)
                key = (c.get("meeting_id"), c.get("updated_at"), c.get("track_condition"), c.get("rating"), c.get("condition"))
                if key not in seen:
                    seen.add(key)
                    out.append(c)
        except Exception:
            continue
    return out

# ----------------------------
# Augment scratchings from fields (ensures ALL scratched runners)
# ----------------------------

def _ensure_race(meeting_obj: Dict[str, Any], rno: int) -> Dict[str, Any]:
    race = meeting_obj["races"].get(rno)
    if not race:
        race = {"race_number": rno, "scratchings": []}
        meeting_obj["races"][rno] = race
    return race

def _exists_in_scratch_list(lst: List[Dict[str, Any]], rid: Optional[int], nm: Optional[str]) -> bool:
    nkey = _norm_name(nm) or ""
    for e in lst:
        if rid and e.get("runner_id") == rid:
            return True
        if (nkey and _norm_name(e.get("horse_name") or "") == nkey):
            return True
    return False

async def _augment_scratchings_from_fields(meetings: Dict[int, Dict[str, Any]], target_date: str) -> None:
    for mid, m in meetings.items():
        await _fetch_meeting_fields(mid, m.get("venue"), m.get("meeting_date") or target_date)
        id_idx = _FIELDS_ID_INDEX.get(mid, {})
        # derive race set
        race_numbers = set(m["races"].keys()) | {
            info.get("race_number") for info in id_idx.values() if info.get("race_number") is not None
        }
        for rno in sorted(r for r in race_numbers if r is not None):
            race = _ensure_race(m, rno)
            for rid, info in id_idx.items():
                if info.get("race_number") != rno:
                    continue
                if info.get("scratched") is True:
                    nm = info.get("horse_name")
                    rn = info.get("runner_number")
                    if not _exists_in_scratch_list(race["scratchings"], rid, nm):
                        race["scratchings"].append({
                            "runner_number": rn,
                            "horse_name": nm,
                            "runner_id": rid,
                            "updated_at": None,
                        })
            race["scratchings"].sort(key=lambda x: (x["runner_number"] is None, x["runner_number"] or 0, (x["horse_name"] or "").lower()))

# ----------------------------
# Public: main fetch
# ----------------------------

async def fetch_updates_for_date(target_date: str) -> Dict[str, Any]:
    scratches = await _fetch_updates_scratchings_for_date(target_date)
    conditions = await _fetch_updates_conditions_for_date(target_date)

    scratches_f = [s for s in scratches if _same_day_mel(s.get("meeting_date"), target_date)]
    cond_f      = [c for c in conditions if _same_day_mel(c.get("meeting_date"), target_date)]

    meetings: Dict[int, Dict[str, Any]] = {}

    # Conditions (initial pass)
    for c in cond_f:
        mid = _parse_int(c.get("meeting_id"))
        if mid is None:
            continue
        m = meetings.setdefault(mid, {
            "meeting_id": mid,
            "venue": c.get("venue"),
            "state": c.get("state"),
            "meeting_date": c.get("meeting_date"),
            "conditions": None,
            "races": {},
        })
        updated = _to_mel_dt(c.get("updated_at"))
        tc = _extract_track_condition(c, m.get("venue"))
        cond_obj = {
            "weather": c.get("weather"),
            "track_condition": tc,
            "rail": c.get("rail"),
            "updated_at": updated.isoformat() if updated else None,
        }
        if not m["conditions"]:
            m["conditions"] = cond_obj
        else:
            prev_iso = m["conditions"].get("updated_at")
            if not prev_iso or (updated and updated.isoformat() > prev_iso):
                m["conditions"] = cond_obj
        if not m.get("venue"): m["venue"] = c.get("venue")
        if not m.get("state"): m["state"] = c.get("state")
        if not m.get("meeting_date"): m["meeting_date"] = c.get("meeting_date")

    # Scratchings from Updates
    for s in scratches_f:
        mid = _parse_int(s.get("meeting_id"))
        if mid is None:
            continue
        race_no = _parse_int(s.get("race_number"))
        if race_no is None:
            continue

        m = meetings.setdefault(mid, {
            "meeting_id": mid,
            "venue": s.get("venue"),
            "state": s.get("state"),
            "meeting_date": s.get("meeting_date"),
            "conditions": None,
            "races": {},
        })
        race = m["races"].setdefault(race_no, {"race_number": race_no, "scratchings": []})

        is_scr = s.get("scratched")
        if isinstance(is_scr, str):
            is_scr = is_scr.lower() in {"1", "true", "y", "yes"}
        if is_scr is False:
            continue

        upd = _to_mel_dt(s.get("updated_at"))
        entry = {
            "runner_number": _parse_int(s.get("runner_number")),
            "horse_name": s.get("horse_name"),
            "runner_id": _parse_int(s.get("runner_id")),
            "updated_at": upd.isoformat() if upd else None,
        }

        if not entry["horse_name"] and not entry["runner_id"]:
            continue

        name_key = (race_no, (_norm_name(entry["horse_name"]) or ""))
        idx_map = {(race_no, (_norm_name(e.get("horse_name")) or "")): i
                   for i, e in enumerate(race["scratchings"])}
        if name_key in idx_map:
            race["scratchings"][idx_map[name_key]] = entry
        else:
            if entry["runner_id"] is not None:
                idx_by_id = {e.get("runner_id"): i for i, e in enumerate(race["scratchings"])}
                if entry["runner_id"] in idx_by_id:
                    race["scratchings"][idx_by_id[entry["runner_id"]]] = entry
                else:
                    race["scratchings"].append(entry)
            else:
                race["scratchings"].append(entry)

        if not m.get("venue"): m["venue"] = s.get("venue")
        if not m.get("state"): m["state"] = s.get("state")
        if not m.get("meeting_date"): m["meeting_date"] = s.get("meeting_date")

    # Ensure ALL scratchings (augment from fields)
    await _augment_scratchings_from_fields(meetings, target_date)

    # Prefer fresh rating via Updates(meetingId), then JSON → CSV (override if different)
    await _backfill_track_condition_for_meetings(meetings, target_date)

    # Materialise
    meetings_out: List[Dict[str, Any]] = []
    for mid, m in meetings.items():
        races = sorted(m["races"].values(), key=lambda r: r["race_number"])
        meetings_out.append({
            "meeting_id": mid,
            "venue": m.get("venue"),
            "state": m.get("state"),
            "conditions": m.get("conditions"),
            "races": races,
        })

    return {"date": target_date, "meetings": meetings_out}
