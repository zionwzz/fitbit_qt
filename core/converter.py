from __future__ import annotations

import csv
import io
import json
import re
import tempfile
import zipfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


# -----------------------------------------------------------------------------
# Output schema constants
# -----------------------------------------------------------------------------

ACTIVITY_OUTPUT_COLUMNS: List[str] = [
    "Date",
    "Calories Burned",
    "Steps",
    "Distance",
    "Floors",
    "Minutes Sedentary",
    "Minutes Lightly Active",
    "Minutes Fairly Active",
    "Minutes Very Active",
    "Activity Calories",
]

SLEEP_OUTPUT_COLUMNS: List[str] = [
    "Start Time",
    "End Time",
    "Minutes Asleep",
    "Minutes Awake",
    "Number of Awakenings",
    "Time in Bed",
    "Minutes REM Sleep",
    "Minutes Light Sleep",
    "Minutes Deep Sleep",
]


# -----------------------------------------------------------------------------
# Small date/time helpers
# -----------------------------------------------------------------------------

def _to_date(value: Any) -> Optional[date]:
    """Best-effort conversion to a date."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        dt = _parse_datetime(value)
        return dt.date() if dt else None
    return None


def _normalize_date_range(
    user_start: Any,
    user_end: Any,
) -> Optional[Tuple[date, date]]:
    """Convert user-provided range inputs to inclusive Python dates."""
    if user_start is None or user_end is None:
        return None
    start = _to_date(user_start)
    end = _to_date(user_end)
    if start is None or end is None:
        return None
    return (start, end)


def _parse_datetime(value: Any) -> Optional[datetime]:
    """Best-effort parser for Fitbit/ISO-like timestamps."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    s = str(value).strip()
    if not s:
        return None

    # Common Fitbit format from metric time-series files
    for fmt in (
        "%m/%d/%y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    # ISO-ish fallback
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _fmt_dt_sleep(dt: datetime) -> str:
    """Format sleep timestamps like YYYY-MM-DD H:MMAM/PM."""
    return dt.strftime("%Y-%m-%d %I:%M%p").replace(" 0", " ")


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def cm_to_miles(x: float) -> float:
    return (x / 100.0) / 1609.344


# -----------------------------------------------------------------------------
# Core parsing utilities
# -----------------------------------------------------------------------------

def _is_macosx_path(p: Path) -> bool:
    return "__MACOSX" in p.parts


def safe_load_json(fp: Path) -> Optional[Any]:
    try:
        txt = fp.read_text(encoding="utf-8").strip()
        if not txt:
            return None
        obj = json.loads(txt)
        return obj if obj else None
    except Exception:
        return None


def find_global_export_data_folder(extract_root: Path) -> Path:
    extract_root = extract_root.resolve()

    candidates = [
        p
        for p in (
            list(extract_root.rglob("Fitbit/Global Export Data"))
            + list(extract_root.rglob("Fitbit/GlobalExportData"))
        )
        if p.is_dir() and not _is_macosx_path(p)
    ]

    if not candidates:
        for p in extract_root.rglob("*"):
            if not p.is_dir() or _is_macosx_path(p):
                continue
            nm = p.name.lower().replace(" ", "")
            if nm == "globalexportdata" and any(
                pp.name.lower() == "fitbit" for pp in p.parents
            ):
                candidates.append(p)

    if not candidates:
        raise FileNotFoundError("Fitbit/Global Export Data folder not found in this zip.")

    def score(folder: Path) -> int:
        return len(
            [x for x in folder.glob("*.json") if x.is_file() and not _is_macosx_path(x)]
        )

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def collect_prefix_files(folder: Path, prefix: str) -> List[Path]:
    folder = folder.resolve()

    direct = [
        fp
        for fp in folder.glob(f"{prefix}*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    if direct:
        return sorted(set(direct))

    deep = [
        fp
        for fp in folder.rglob(f"{prefix}*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    return sorted(set(deep))


def collect_sleep_files(folder: Path) -> List[Path]:
    folder = folder.resolve()

    direct = [
        fp
        for fp in folder.glob("sleep*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    direct += [
        fp
        for fp in folder.glob("Sleep*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    if direct:
        return sorted(set(direct))

    deep = [
        fp
        for fp in folder.rglob("sleep*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    deep += [
        fp
        for fp in folder.rglob("Sleep*.json")
        if fp.is_file() and not _is_macosx_path(fp)
    ]
    return sorted(set(deep))


# -----------------------------------------------------------------------------
# Daily metric aggregation
# -----------------------------------------------------------------------------

DailyRows = List[Dict[str, Any]]
SleepRows = List[Dict[str, Any]]


def metric_daily_sum(
    files: List[Path],
    out_col: str,
    round_int: bool = False,
    value_transform: Optional[Callable[[float], float]] = None,
) -> DailyRows:
    daily_totals: Dict[date, float] = defaultdict(float)

    for fp in files:
        obj = safe_load_json(fp)
        if obj is None:
            continue

        records = obj if isinstance(obj, list) else [obj]
        for rec in records:
            if not isinstance(rec, dict):
                continue
            dt = _parse_datetime(rec.get("dateTime"))
            val = _coerce_float(rec.get("value"))
            if dt is None or val is None:
                continue
            if value_transform is not None:
                val = value_transform(val)
            daily_totals[dt.date()] += val

    out: DailyRows = []
    for d in sorted(daily_totals):
        value = daily_totals[d]
        if round_int:
            value = int(round(value))
        out.append({"date": d, out_col: value})
    return out


def build_daily_calories_and_activity(global_folder: Path) -> DailyRows:
    files = collect_prefix_files(global_folder, "calories")
    if not files:
        return []

    per_day_values: Dict[date, List[float]] = defaultdict(list)

    for fp in files:
        obj = safe_load_json(fp)
        if obj is None:
            continue

        records = obj if isinstance(obj, list) else [obj]
        for rec in records:
            if not isinstance(rec, dict):
                continue
            dt = _parse_datetime(rec.get("dateTime"))
            kcal = _coerce_float(rec.get("value"))
            if dt is None or kcal is None:
                continue
            per_day_values[dt.date()].append(kcal)

    out: DailyRows = []
    for d in sorted(per_day_values):
        vals = per_day_values[d]
        if not vals:
            continue
        total = sum(vals)
        baseline = min(vals)
        n = len(vals)
        activity = max(0.0, total - baseline * n)
        out.append(
            {
                "date": d,
                "Calories Burned": int(round(total)),
                "Activity Calories": int(round(activity)),
            }
        )

    return out


# -----------------------------------------------------------------------------
# Sleep parsing
# -----------------------------------------------------------------------------

def _extract_sleep_row(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    start = _parse_datetime(ev.get("startTime"))
    end = _parse_datetime(ev.get("endTime"))
    if start is None or end is None:
        return None

    levels = ev.get("levels") or {}
    summary = levels.get("summary") or {}

    wake_count = (summary.get("wake") or {}).get("count", None)
    if wake_count is None:
        awake_count = int((summary.get("awake") or {}).get("count") or 0)
        restless_count = int((summary.get("restless") or {}).get("count") or 0)
        num_awakenings = awake_count + restless_count
    else:
        num_awakenings = int(wake_count)

    sleep_type = str(ev.get("type") or "").strip().lower()
    is_staged = sleep_type in {"stages", "staged"}
    if not sleep_type:
        is_staged = any(k in summary for k in ["rem", "light", "deep"])

    if is_staged:
        rem_min = int((summary.get("rem") or {}).get("minutes") or 0)
        light_min = int((summary.get("light") or {}).get("minutes") or 0)
        deep_min = int((summary.get("deep") or {}).get("minutes") or 0)
    else:
        rem_min = "N/A"
        light_min = "N/A"
        deep_min = "N/A"

    return {
        "Start Time": _fmt_dt_sleep(start),
        "End Time": _fmt_dt_sleep(end),
        "Minutes Asleep": int(ev.get("minutesAsleep") or 0),
        "Minutes Awake": int(ev.get("minutesAwake") or 0),
        "Number of Awakenings": int(num_awakenings),
        "Time in Bed": int(ev.get("timeInBed") or 0),
        "Minutes REM Sleep": rem_min,
        "Minutes Light Sleep": light_min,
        "Minutes Deep Sleep": deep_min,
        "_start_dt": start,
        "_logId": ev.get("logId", None),
    }


def build_sleep_table(global_folder: Path) -> SleepRows:
    rows: SleepRows = []

    for fp in collect_sleep_files(global_folder):
        obj = safe_load_json(fp)
        if obj is None:
            continue

        events = obj if isinstance(obj, list) else [obj]
        for ev in events:
            if not isinstance(ev, dict):
                continue
            if "startTime" not in ev or "endTime" not in ev:
                continue
            row = _extract_sleep_row(ev)
            if row is not None:
                rows.append(row)

    if not rows:
        return []

    has_any_logid = any(r.get("_logId") is not None for r in rows)
    seen = set()
    deduped: SleepRows = []

    for row in rows:
        if has_any_logid:
            key = row.get("_logId")
        else:
            key = (row.get("Start Time"), row.get("End Time"))

        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    deduped.sort(key=lambda r: r["_start_dt"])
    return deduped


# -----------------------------------------------------------------------------
# Date-range helpers
# -----------------------------------------------------------------------------

def _range_from_daily(rows: DailyRows, date_col: str = "date") -> Optional[Tuple[date, date]]:
    if not rows:
        return None

    vals = [r.get(date_col) for r in rows if isinstance(r.get(date_col), date)]
    if not vals:
        return None
    return (min(vals), max(vals))


def _intersect_ranges(
    ranges: List[Tuple[date, date]]
) -> Optional[Tuple[date, date]]:
    if not ranges:
        return None
    start = max(r[0] for r in ranges)
    end = min(r[1] for r in ranges)
    return None if start > end else (start, end)


def _filter_daily_by_range(
    rows: DailyRows,
    start: date,
    end: date,
    date_col: str = "date",
) -> DailyRows:
    if not rows:
        return rows
    return [
        r.copy()
        for r in rows
        if isinstance(r.get(date_col), date) and start <= r[date_col] <= end
    ]


def _filter_sleep_by_range(rows: SleepRows, start: date, end: date) -> SleepRows:
    if not rows:
        return rows
    return [
        r.copy()
        for r in rows
        if isinstance(r.get("_start_dt"), datetime)
        and start <= r["_start_dt"].date() <= end
    ]


# -----------------------------------------------------------------------------
# Output assembly
# -----------------------------------------------------------------------------

def _rows_to_date_map(rows: DailyRows, value_cols: List[str]) -> Dict[date, Dict[str, Any]]:
    out: Dict[date, Dict[str, Any]] = {}
    for row in rows:
        d = row.get("date")
        if not isinstance(d, date):
            continue
        bucket = out.setdefault(d, {})
        for c in value_cols:
            if c in row:
                bucket[c] = row[c]
    return out


def build_outputs(
    global_folder: Path,
    intersect_dates: bool = True,
    user_start: Any = None,
    user_end: Any = None,
) -> Tuple[DailyRows, SleepRows, Optional[Tuple[date, date]]]:
    steps = metric_daily_sum(
        collect_prefix_files(global_folder, "steps"),
        "Steps",
        round_int=True,
    )
    cal_act = build_daily_calories_and_activity(global_folder)
    dist = metric_daily_sum(
        collect_prefix_files(global_folder, "distance"),
        "Distance",
        value_transform=cm_to_miles,
    )
    sed = metric_daily_sum(
        collect_prefix_files(global_folder, "sedentary_minutes"),
        "Minutes Sedentary",
        round_int=True,
    )
    light = metric_daily_sum(
        collect_prefix_files(global_folder, "lightly_active_minutes"),
        "Minutes Lightly Active",
        round_int=True,
    )
    mod = metric_daily_sum(
        collect_prefix_files(global_folder, "moderately_active_minutes"),
        "Minutes Fairly Active",
        round_int=True,
    )
    vig = metric_daily_sum(
        collect_prefix_files(global_folder, "very_active_minutes"),
        "Minutes Very Active",
        round_int=True,
    )
    sleep_raw = build_sleep_table(global_folder)

    user_range = _normalize_date_range(user_start, user_end)

    date_range: Optional[Tuple[date, date]] = None

    if intersect_dates:
        ranges: List[Tuple[date, date]] = []

        for rows in [steps, cal_act, dist, sed, light, mod, vig]:
            r = _range_from_daily(rows, "date")
            if r:
                ranges.append(r)

        if sleep_raw:
            sleep_dates = [r["_start_dt"].date() for r in sleep_raw if r.get("_start_dt")]
            if sleep_dates:
                ranges.append((min(sleep_dates), max(sleep_dates)))

        date_range = _intersect_ranges(ranges)
        if date_range is None:
            return [], [], None

        if user_range is not None:
            date_range = _intersect_ranges([date_range, user_range])
            if date_range is None:
                return [], [], None

        start, end = date_range
        steps = _filter_daily_by_range(steps, start, end)
        cal_act = _filter_daily_by_range(cal_act, start, end)
        dist = _filter_daily_by_range(dist, start, end)
        sed = _filter_daily_by_range(sed, start, end)
        light = _filter_daily_by_range(light, start, end)
        mod = _filter_daily_by_range(mod, start, end)
        vig = _filter_daily_by_range(vig, start, end)
        sleep_raw = _filter_sleep_by_range(sleep_raw, start, end)

    else:
        if user_range is not None:
            start, end = user_range
            steps = _filter_daily_by_range(steps, start, end)
            cal_act = _filter_daily_by_range(cal_act, start, end)
            dist = _filter_daily_by_range(dist, start, end)
            sed = _filter_daily_by_range(sed, start, end)
            light = _filter_daily_by_range(light, start, end)
            mod = _filter_daily_by_range(mod, start, end)
            vig = _filter_daily_by_range(vig, start, end)
            sleep_raw = _filter_sleep_by_range(sleep_raw, start, end)
            date_range = user_range

    # Merge activity domains by date
    by_date: Dict[date, Dict[str, Any]] = defaultdict(dict)

    for row_map in [
        _rows_to_date_map(steps, ["Steps"]),
        _rows_to_date_map(cal_act, ["Calories Burned", "Activity Calories"]),
        _rows_to_date_map(dist, ["Distance"]),
        _rows_to_date_map(sed, ["Minutes Sedentary"]),
        _rows_to_date_map(light, ["Minutes Lightly Active"]),
        _rows_to_date_map(mod, ["Minutes Fairly Active"]),
        _rows_to_date_map(vig, ["Minutes Very Active"]),
    ]:
        for d, vals in row_map.items():
            by_date[d].update(vals)

    activity_rows: DailyRows = []
    for d in sorted(by_date):
        vals = by_date[d]
        row = {
            "Date": d.isoformat(),
            "Calories Burned": int(round(vals.get("Calories Burned", 0))),
            "Steps": int(round(vals.get("Steps", 0))),
            "Distance": float(vals.get("Distance", 0.0)),
            "Floors": 0,
            "Minutes Sedentary": int(round(vals.get("Minutes Sedentary", 0))),
            "Minutes Lightly Active": int(round(vals.get("Minutes Lightly Active", 0))),
            "Minutes Fairly Active": int(round(vals.get("Minutes Fairly Active", 0))),
            "Minutes Very Active": int(round(vals.get("Minutes Very Active", 0))),
            "Activity Calories": int(round(vals.get("Activity Calories", 0))),
        }
        activity_rows.append(row)

    sleep_rows: SleepRows = []
    for row in sorted(sleep_raw, key=lambda r: r["_start_dt"]):
        clean = {col: row.get(col, "") for col in SLEEP_OUTPUT_COLUMNS}
        sleep_rows.append(clean)

    return activity_rows, sleep_rows, date_range


# -----------------------------------------------------------------------------
# CSV output
# -----------------------------------------------------------------------------

def write_combined_csv_bytes(activity_rows: DailyRows, sleep_rows: SleepRows) -> bytes:
    if not activity_rows and not sleep_rows:
        return b""

    buf = io.StringIO()

    # Activities section
    buf.write("Activities\n")
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(ACTIVITY_OUTPUT_COLUMNS)
    for row in activity_rows:
        w.writerow(
            [
                row.get("Date", ""),
                row.get("Calories Burned", 0),
                row.get("Steps", 0),
                f'{float(row.get("Distance", 0.0)):.2f}',
                row.get("Floors", 0),
                row.get("Minutes Sedentary", 0),
                row.get("Minutes Lightly Active", 0),
                row.get("Minutes Fairly Active", 0),
                row.get("Minutes Very Active", 0),
                row.get("Activity Calories", 0),
            ]
        )

    # Sleep section
    buf.write("\nSleep\n")
    buf.write(",".join(SLEEP_OUTPUT_COLUMNS) + "\n")
    sw = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for row in sleep_rows:
        sw.writerow([row.get(c, "") for c in SLEEP_OUTPUT_COLUMNS])

    return buf.getvalue().encode("utf-8")


# -----------------------------------------------------------------------------
# Zip handling / main entry point
# -----------------------------------------------------------------------------

def _safe_extractall(zf: zipfile.ZipFile, dest_dir: Path) -> None:
    dest_dir = dest_dir.resolve()
    for member in zf.infolist():
        target_path = (dest_dir / member.filename).resolve()
        if dest_dir not in target_path.parents and target_path != dest_dir:
            raise ValueError(f"Unsafe path in zip: {member.filename}")
    zf.extractall(dest_dir)


def convert_takeout_zip_bytes(
    zip_bytes: bytes,
    intersect_dates: bool = True,
    user_start: Any = None,
    user_end: Any = None,
) -> Tuple[bytes, DailyRows, SleepRows, Optional[Tuple[date, date]]]:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            _safe_extractall(zf, root)

        global_folder = find_global_export_data_folder(root)
        activity_rows, sleep_rows, date_range = build_outputs(
            global_folder,
            intersect_dates=intersect_dates,
            user_start=user_start,
            user_end=user_end,
        )
        out_bytes = write_combined_csv_bytes(activity_rows, sleep_rows)
        return out_bytes, activity_rows, sleep_rows, date_range


# -----------------------------------------------------------------------------
# Filename helpers
# -----------------------------------------------------------------------------

def _sanitize_filename_part(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "", s)
    return s or "UNKNOWN"


def sanitize_filename_part(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s or "participant"
