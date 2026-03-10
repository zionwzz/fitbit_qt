from __future__ import annotations

import csv
import io
import json
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

# -----------------------------------------------------------------------------
# Output schema constants
# -----------------------------------------------------------------------------

# These column names are used by the downstream dashboard/importer this project was
# built for. If you need a different schema, adjust these lists and the corresponding
# merge/casting logic in ``build_outputs``.
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


# -----------------------
# Core parsing utilities
# -----------------------

def _is_macosx_path(p: Path) -> bool:
    """Return whether ``p`` appears to be a macOS zip artifact.

    macOS (Finder) often adds an extra ``__MACOSX/`` folder plus metadata files when
    creating ZIP archives. These entries are not part of the Fitbit export and should
    be ignored during discovery.

    Args:
        p: Any filesystem path.

    Returns:
        True if the path contains a ``__MACOSX`` directory component.
    """

    return "__MACOSX" in p.parts


def safe_load_json(fp: Path) -> Optional[Any]:
    """Read and parse a JSON file, returning ``None`` on any failure.

    This helper is deliberately forgiving because Takeout exports can include empty
    files, partial downloads, or schema variants. Callers typically iterate over many
    files and should not fail the whole run because a single file is malformed.

    Args:
        fp: Path to a JSON file. The file is read as UTF-8.

    Returns:
        The parsed JSON object (dict, list, etc.), or ``None`` if:
        - the file is empty/whitespace
        - decoding/parsing fails
        - the JSON parses but evaluates to an "empty" object

    Limitations:
        All exceptions are swallowed. If you need strict behavior, replace this with
        ``json.loads(fp.read_text(...))`` and let errors propagate.
    """

    try:
        txt = fp.read_text(encoding="utf-8").strip()
        if not txt:
            return None
        obj = json.loads(txt)
        return obj if obj else None
    except Exception:
        return None


def parse_fitbit_datetime(series: pd.Series) -> pd.Series:
    """Parse Fitbit Takeout ``dateTime`` fields into pandas timestamps.

    Fitbit's "Global Export Data" JSON files commonly encode timestamps using
    ``"%m/%d/%y %H:%M:%S"`` (e.g., ``"03/01/24 00:01:00"``). Some exports, however,
    use ISO-8601 or other formats.

    This function:
    1) Tries the known Fitbit format first.
    2) Falls back to pandas' general parser for values that failed.

    Args:
        series: A pandas Series of timestamp-like strings.

    Returns:
        A pandas Series of ``datetime64[ns]`` values. Unparseable rows become ``NaT``.

    Notes:
        - Returned timestamps are timezone-naive.
        - The logic is "best effort" and trades strictness for robustness.
    """

    dt = pd.to_datetime(series, format="%m/%d/%y %H:%M:%S", errors="coerce")
    if dt.isna().all():
        # Nothing matched the Fitbit format; let pandas infer.
        return pd.to_datetime(series, errors="coerce", infer_datetime_format=True)

    # Only re-parse the failures.
    miss = dt.isna()
    if miss.any():
        dt.loc[miss] = pd.to_datetime(
            series[miss], errors="coerce", infer_datetime_format=True
        )
    return dt


def cm_to_miles(s: pd.Series) -> pd.Series:
    """Convert a distance series from centimeters to miles.

    Args:
        s: Numeric series in **centimeters**.

    Returns:
        Numeric series in **miles**.

    Assumptions:
        Fitbit Takeout's ``distance*.json`` values are treated as centimeters.

    Limitations:
        If your export stores distance in meters/kilometers, adjust this conversion.
    """

    # 1 mile = 1609.344 meters; 1 meter = 100 centimeters.
    return (s / 100.0) / 1609.344


def find_global_export_data_folder(extract_root: Path) -> Path:
    """Locate Fitbit's "Global Export Data" folder inside an extracted Takeout ZIP.

    Google Takeout exports usually place the Fitbit data under:

    - ``Fitbit/Global Export Data`` (with spaces)
    - ``Fitbit/GlobalExportData`` (no spaces)

    This function searches for those directories *anywhere* under ``extract_root``.
    If multiple candidates are found (e.g., multiple Takeout exports were zipped
    together), it selects the directory that contains the largest number of JSON files.

    Args:
        extract_root: Root directory where the ZIP has been extracted.

    Returns:
        The resolved path to the "Global Export Data" folder.

    Raises:
        FileNotFoundError: If no matching directory is found.

    Limitations:
        The "most JSON files" heuristic may choose the wrong folder if your ZIP contains
        multiple exports with overlapping structures.
    """

    extract_root = extract_root.resolve()

    # Fast paths: typical Takeout folder names.
    candidates = [
        p
        for p in (
            list(extract_root.rglob("Fitbit/Global Export Data"))
            + list(extract_root.rglob("Fitbit/GlobalExportData"))
        )
        if p.is_dir() and not _is_macosx_path(p)
    ]

    # Slow path: tolerate minor variations in spacing/casing.
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
        raise FileNotFoundError(
            "Fitbit/Global Export Data folder not found in this zip."
        )

    def score(folder: Path) -> int:
        # More JSON files usually means the folder is the actual export payload.
        return len(
            [
                x
                for x in folder.glob("*.json")
                if x.is_file() and not _is_macosx_path(x)
            ]
        )

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def collect_prefix_files(folder: Path, prefix: str) -> List[Path]:
    """Collect JSON files from ``folder`` that start with ``prefix``.

    Fitbit's exported JSON files are commonly named with a metric prefix, e.g.
    ``steps-2024-03-01.json`` or ``distance-2024-03-01.json``.

    The search is performed in two phases:
    1) Look directly under ``folder`` (fast, typical Takeout layout).
    2) If none are found, search recursively under ``folder`` (supports nested exports).

    Args:
        folder: Directory that contains the export JSON files.
        prefix: Filename prefix to match (case-sensitive), e.g. ``"steps"``.

    Returns:
        Sorted list of matching file paths.
    """

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
    """Collect sleep JSON files from an export directory.

    Fitbit sleep logs appear under filenames like ``sleep-YYYY-MM-DD.json``. Some
    exports use different casing (e.g., ``Sleep-...``), so we match both.

    Args:
        folder: Directory that contains the export JSON files.

    Returns:
        Sorted list of sleep JSON paths.
    """

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


def metric_daily_sum(
    files: List[Path],
    out_col: str,
    round_int: bool = False,
    value_transform: Optional[Callable[[pd.Series], pd.Series]] = None,
) -> pd.DataFrame:
    """Aggregate a Fitbit metric to daily totals.

    Many Fitbit Takeout metrics are stored as time series with schema:

    - ``dateTime``: timestamp string
    - ``value``: numeric value at that time

    This function loads each JSON file, parses timestamps, coerces values to numbers,
    optionally transforms the values (unit conversions), and finally sums within each
    calendar date.

    Args:
        files: List of JSON file paths to read.
        out_col: Name of the output value column.
        round_int: If True, round the daily totals and cast to int.
        value_transform: Optional transform applied to the numeric series before
            aggregation (e.g., centimeters → miles).

    Returns:
        DataFrame with columns:
        - ``date``: Python ``datetime.date``
        - ``out_col``: daily sum

    Assumptions:
        - JSON payload is either a list of objects or a single object.
        - Relevant keys are named exactly ``dateTime`` and ``value``.

    Limitations:
        - Rows with unparseable timestamps or values are dropped.
        - If multiple files contain overlapping timestamps for the same day, their
          contributions are summed (which may double-count if files overlap).
    """

    dfs: List[pd.DataFrame] = []

    for fp in files:
        obj = safe_load_json(fp)
        if obj is None:
            continue

        df = pd.DataFrame(obj) if isinstance(obj, list) else pd.DataFrame([obj])
        if df.empty or "dateTime" not in df.columns or "value" not in df.columns:
            continue

        dt = parse_fitbit_datetime(df["dateTime"])
        val = pd.to_numeric(df["value"], errors="coerce")
        if value_transform is not None:
            val = value_transform(val)

        tmp = pd.DataFrame({"date": dt.dt.date, "value": val}).dropna()
        if tmp.empty:
            continue

        if round_int:
            tmp["value"] = tmp["value"].round().astype(int)

        daily = tmp.groupby("date", as_index=False)["value"].sum()
        daily.rename(columns={"value": out_col}, inplace=True)
        dfs.append(daily)

    if not dfs:
        return pd.DataFrame(columns=["date", out_col])

    # Combine results from all files and re-sum by day.
    return pd.concat(dfs, ignore_index=True).groupby("date", as_index=False).sum()


def build_daily_calories_and_activity(global_folder: Path) -> pd.DataFrame:
    """Build daily total calories burned and estimated activity calories.

    Fitbit Takeout often includes a ``calories*.json`` time series that represents
    calories burned over time (commonly minute-level). This function constructs:

    - **Calories Burned** (kcal/day): sum of all samples for that day
    - **Activity Calories** (kcal/day): an estimate of calories above baseline

    Activity-calorie heuristic
    -------------------------
    For each day, we compute:

    - ``total`` = sum of per-sample kcal for the day
    - ``baseline`` = minimum per-sample kcal for the day
    - ``n`` = number of samples for the day
    - ``activity`` = max(0, total - baseline * n)

    Interpretation: The smallest observed per-sample burn is treated as a proxy for
    resting calories per sample (often per minute). Subtracting that baseline from
    all samples yields an approximate "active" component.

    Args:
        global_folder: Path to the Fitbit "Global Export Data" folder.

    Returns:
        DataFrame with columns:
        - ``date``: Python ``datetime.date``
        - ``Calories Burned``: int kcal/day
        - ``Activity Calories``: int kcal/day

    Limitations:
        - If the calories series is not evenly sampled (not minute-level) or contains
          gaps/zeros, ``baseline`` may be a poor estimate and activity calories may
          be biased.
        - The heuristic is meant for reasonable approximations, not clinical accuracy.
    """

    files = collect_prefix_files(global_folder, "calories")
    if not files:
        return pd.DataFrame(columns=["date", "Calories Burned", "Activity Calories"])

    parts: List[pd.DataFrame] = []

    for fp in files:
        obj = safe_load_json(fp)
        if obj is None:
            continue

        df = pd.DataFrame(obj) if isinstance(obj, list) else pd.DataFrame([obj])
        if df.empty or "dateTime" not in df.columns or "value" not in df.columns:
            continue

        dt = parse_fitbit_datetime(df["dateTime"])
        kcal = pd.to_numeric(df["value"], errors="coerce")
        tmp = pd.DataFrame({"date": dt.dt.date, "kcal": kcal}).dropna()
        if not tmp.empty:
            parts.append(tmp)

    if not parts:
        return pd.DataFrame(columns=["date", "Calories Burned", "Activity Calories"])

    all_min = pd.concat(parts, ignore_index=True)

    # Group by calendar date.
    g = all_min.groupby("date")["kcal"]

    total = g.sum()
    baseline = g.min()
    n = g.count()

    # Heuristic described in the docstring: subtract baseline from each sample.
    activity = (total - baseline * n).clip(lower=0)

    out = pd.DataFrame(
        {
            "date": total.index,
            "Calories Burned": total.values,
            "Activity Calories": activity.values,
        }
    )
    out["Calories Burned"] = out["Calories Burned"].round().astype(int)
    out["Activity Calories"] = out["Activity Calories"].round().astype(int)
    return out


def _fmt_dt_sleep(dt: pd.Timestamp) -> str:
    """Format a sleep timestamp string for the output CSV.

    The downstream importer this project targets expects a human-readable timestamp
    similar to ``YYYY-MM-DD H:MMAM/PM`` (12-hour clock, no leading zero).

    Args:
        dt: Parsed timestamp.

    Returns:
        Formatted string.
    """

    s = dt.strftime("%Y-%m-%d %I:%M%p")
    # `%I` produces `01`, `02`, ...; remove the leading zero.
    return s.replace(" 0", " ")


def _extract_sleep_row(ev: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Convert a raw Fitbit sleep event into one output row.

    Args:
        ev: A dict representing a single sleep log entry from Fitbit Takeout.
            Expected keys include ``startTime`` and ``endTime``.

    Returns:
        A dict with the output fields in :data:`SLEEP_OUTPUT_COLUMNS` plus:
        - ``_start_dt``: parsed start timestamp (for sorting/filtering)
        - ``_logId``: Fitbit sleep log identifier (if present)

        Returns ``None`` if required timestamps cannot be parsed.

    Heuristics:
        - **Number of awakenings**: Fitbit exports have multiple schema variants.
          If ``levels.summary.wake.count`` exists, it is used directly. Otherwise we
          approximate by summing ``awake.count`` and ``restless.count``.
        - **Staged vs classic sleep**: When sleep type is "stages"/"staged" (or if we
          infer stage keys exist), we populate REM/light/deep minutes. Otherwise these
          fields are set to ``"N/A"``.

    Limitations:
        This function only uses summary fields. It does not expand the detailed per-
        epoch sleep levels.
    """

    start = pd.to_datetime(ev.get("startTime"), errors="coerce")
    end = pd.to_datetime(ev.get("endTime"), errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return None

    levels = ev.get("levels") or {}
    summary = levels.get("summary") or {}

    # Awake count handling differs between "classic" and "staged" schemas.
    wake_count = (summary.get("wake") or {}).get("count", None)
    if wake_count is None:
        awake_count = int((summary.get("awake") or {}).get("count") or 0)
        restless_count = int((summary.get("restless") or {}).get("count") or 0)
        num_awakenings = awake_count + restless_count
    else:
        num_awakenings = int(wake_count)

    # ----- staged vs classic handling -----
    sleep_type = str(ev.get("type") or "").strip().lower()
    is_staged = sleep_type in {"stages", "staged"}

    # If type missing, infer from keys.
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


def build_sleep_table(global_folder: Path) -> pd.DataFrame:
    """Build a sleep table from Fitbit Takeout sleep JSON files.

    Args:
        global_folder: Path to the Fitbit "Global Export Data" folder.

    Returns:
        DataFrame containing :data:`SLEEP_OUTPUT_COLUMNS` plus an internal
        ``_start_dt`` column used for sorting and date-range filtering.

    Deduplication:
        - If any rows contain a non-null ``logId``, we deduplicate on that identifier.
        - Otherwise, we deduplicate by ``(Start Time, End Time)``.

    Limitations:
        - Output is based on summary fields and may differ from Fitbit UI totals.
        - Stage minutes are only populated for staged sleep logs; otherwise "N/A".
    """

    rows: List[Dict[str, Any]] = []

    for fp in collect_sleep_files(global_folder):
        obj = safe_load_json(fp)
        if obj is None:
            continue

        events = obj if isinstance(obj, list) else [obj]
        for ev in events:
            if not isinstance(ev, dict) or "startTime" not in ev or "endTime" not in ev:
                continue
            row = _extract_sleep_row(ev)
            if row is not None:
                rows.append(row)

    if not rows:
        return pd.DataFrame(columns=SLEEP_OUTPUT_COLUMNS)

    df = pd.DataFrame(rows)

    if df["_logId"].notna().any():
        df = df.drop_duplicates(subset=["_logId"], keep="first")
    else:
        df = df.drop_duplicates(subset=["Start Time", "End Time"], keep="first")

    df = df.sort_values("_start_dt", ascending=True).drop(columns=["_logId"])

    # Keep `_start_dt` for callers that need date-range filtering.
    return df[SLEEP_OUTPUT_COLUMNS + ["_start_dt"]]


def _range_from_daily(
    df: pd.DataFrame, date_col: str = "date"
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Compute the inclusive (min, max) date range for a daily metric DataFrame.

    Args:
        df: DataFrame that has a date-like column.
        date_col: Column name containing date-like values.

    Returns:
        Tuple of (start, end) timestamps normalized to midnight, or ``None`` if the
        DataFrame is empty or the column cannot be parsed.
    """

    if df is None or df.empty or date_col not in df.columns:
        return None

    d = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if d.empty:
        return None

    return (d.min().normalize(), d.max().normalize())


def _intersect_ranges(
    ranges: List[Tuple[pd.Timestamp, pd.Timestamp]]
) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Intersect multiple inclusive date ranges.

    Args:
        ranges: List of inclusive (start, end) tuples.

    Returns:
        The overlapping inclusive (start, end) range, or ``None`` if there is no
        overlap.
    """

    if not ranges:
        return None

    start = max(r[0] for r in ranges)
    end = min(r[1] for r in ranges)
    return None if start > end else (start, end)


def _filter_daily_by_range(
    df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, date_col: str = "date"
) -> pd.DataFrame:
    """Filter a daily metric DataFrame to an inclusive date range.

    Args:
        df: DataFrame with a date-like column.
        start: Inclusive start date (normalized to midnight recommended).
        end: Inclusive end date (normalized to midnight recommended).
        date_col: Column name containing date-like values.

    Returns:
        A copy of ``df`` containing only rows within the date range. If ``df`` is
        empty, it is returned as-is.
    """

    if df is None or df.empty:
        return df

    d = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    return df.loc[(d >= start) & (d <= end)].copy()


def build_outputs(
    global_folder: Path,
    intersect_dates: bool = True,
    user_start: Optional[pd.Timestamp] = None,
    user_end: Optional[pd.Timestamp] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Optional[Tuple[pd.Timestamp, pd.Timestamp]]]:
    """Build the activity and sleep output tables from an export directory.

    Args:
        global_folder: Path to the Fitbit "Global Export Data" folder.
        intersect_dates: If True, restrict output to the **intersection** of date
            ranges across all available domains (steps, calories, distance, minutes,
            sleep). This avoids days where one domain is missing entirely.
        user_start: Optional user-provided start date (inclusive).
        user_end: Optional user-provided end date (inclusive).

    Returns:
        Tuple of:
        - ``activity_df``: Daily activity DataFrame in :data:`ACTIVITY_OUTPUT_COLUMNS`.
        - ``sleep_df``: Sleep log DataFrame in :data:`SLEEP_OUTPUT_COLUMNS`.
        - ``date_range``: The final inclusive (start, end) date range used, or
          ``None`` if no overlap exists.

    Output units
    ------------
    - ``Calories Burned`` and ``Activity Calories``: **kcal/day** (int)
    - ``Steps``: count/day (int)
    - ``Distance``: **miles/day** (float)
    - ``Minutes *``: minutes/day (int)
    - ``Floors``: currently always 0

    Notes on date-range handling
    ----------------------------
    - If ``intersect_dates`` is True, we first compute the overlap across all domains
      that have data, then intersect again with the user-provided range (if any).
    - If ``intersect_dates`` is False, we only apply the user-provided range.

    Limitations:
        Even within a chosen range, individual days may still have missing metrics.
        Those missing values are filled with 0 in the combined activity table.
    """

    steps = metric_daily_sum(
        collect_prefix_files(global_folder, "steps"), "Steps", round_int=True
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

    # Normalize user range (inclusive).
    user_range: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
    if user_start is not None and user_end is not None:
        us = pd.to_datetime(user_start, errors="coerce")
        ue = pd.to_datetime(user_end, errors="coerce")
        if not pd.isna(us) and not pd.isna(ue):
            user_range = (us.normalize(), ue.normalize())

    date_range: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None

    if intersect_dates:
        ranges: List[Tuple[pd.Timestamp, pd.Timestamp]] = []

        # Only domains with data contribute to the intersection.
        for d in [steps, cal_act, dist, sed, light, mod, vig]:
            r = _range_from_daily(d, "date")
            if r:
                ranges.append(r)

        if not sleep_raw.empty:
            sd = sleep_raw["_start_dt"].dropna()
            if not sd.empty:
                ranges.append((sd.min().normalize(), sd.max().normalize()))

        date_range = _intersect_ranges(ranges)
        if date_range is None:
            return pd.DataFrame(), pd.DataFrame(), None

        # Intersect with user range if provided.
        if user_range is not None:
            date_range = _intersect_ranges([date_range, user_range])
            if date_range is None:
                return pd.DataFrame(), pd.DataFrame(), None

        start, end = date_range

        steps = _filter_daily_by_range(steps, start, end)
        cal_act = _filter_daily_by_range(cal_act, start, end)
        dist = _filter_daily_by_range(dist, start, end)
        sed = _filter_daily_by_range(sed, start, end)
        light = _filter_daily_by_range(light, start, end)
        mod = _filter_daily_by_range(mod, start, end)
        vig = _filter_daily_by_range(vig, start, end)

        if not sleep_raw.empty:
            sd = sleep_raw["_start_dt"].dt.normalize()
            sleep_raw = sleep_raw.loc[(sd >= start) & (sd <= end)].copy()

    else:
        # Only apply user range (if provided).
        if user_range is not None:
            start, end = user_range

            steps = _filter_daily_by_range(steps, start, end)
            cal_act = _filter_daily_by_range(cal_act, start, end)
            dist = _filter_daily_by_range(dist, start, end)
            sed = _filter_daily_by_range(sed, start, end)
            light = _filter_daily_by_range(light, start, end)
            mod = _filter_daily_by_range(mod, start, end)
            vig = _filter_daily_by_range(vig, start, end)

            if not sleep_raw.empty:
                sd = sleep_raw["_start_dt"].dt.normalize()
                sleep_raw = sleep_raw.loc[(sd >= start) & (sd <= end)].copy()

        date_range = user_range

    # ------------------------------------------------------------------
    # Combine activity domains
    # ------------------------------------------------------------------
    dfs = [steps, cal_act, dist, sed, light, mod, vig]

    combined: Optional[pd.DataFrame] = None
    for d in dfs:
        combined = d if combined is None else pd.merge(combined, d, on="date", how="outer")

    if combined is None or combined.empty:
        activity_df = pd.DataFrame(columns=ACTIVITY_OUTPUT_COLUMNS)
    else:
        # Fill missing metrics with zeros so the output table is rectangular.
        combined.fillna(0, inplace=True)

        # Downstream expects ISO-like date strings.
        combined["Date"] = combined["date"].astype(str)

        # Floors aren't provided by this exporter (placeholder).
        combined["Floors"] = 0

        # Ensure all expected columns exist.
        for c in [
            "Calories Burned",
            "Steps",
            "Distance",
            "Activity Calories",
            "Minutes Sedentary",
            "Minutes Lightly Active",
            "Minutes Fairly Active",
            "Minutes Very Active",
            "Floors",
        ]:
            if c not in combined.columns:
                combined[c] = 0

        # Cast integer-like columns.
        for c in [
            "Steps",
            "Floors",
            "Minutes Sedentary",
            "Minutes Lightly Active",
            "Minutes Fairly Active",
            "Minutes Very Active",
            "Activity Calories",
            "Calories Burned",
        ]:
            combined[c] = combined[c].round().fillna(0).astype(int)

        # Keep distance as float (miles).
        combined["Distance"] = combined["Distance"].astype(float)

        activity_df = combined[ACTIVITY_OUTPUT_COLUMNS].sort_values("Date")

    # ------------------------------------------------------------------
    # Sleep output
    # ------------------------------------------------------------------
    if sleep_raw.empty:
        sleep_df = pd.DataFrame(columns=SLEEP_OUTPUT_COLUMNS)
    else:
        sleep_df = (
            sleep_raw.sort_values("_start_dt", ascending=True)
            .drop(columns=["_start_dt"])
            .reset_index(drop=True)
        )

    return activity_df, sleep_df, date_range


def write_combined_csv_bytes(activity_df: pd.DataFrame, sleep_df: pd.DataFrame) -> bytes:
    """Serialize activity and sleep tables into the combined CSV format.

    The output is a single text file that looks like:

    .. code-block:: text

        Activities
        Date,Calories Burned,Steps,...
        2024-03-01, ...

        Sleep
        Start Time,End Time,...
        "2024-03-01 11:03PM",...

    This is not a standard single-table CSV; it is two labeled CSV blocks.

    Args:
        activity_df: DataFrame in :data:`ACTIVITY_OUTPUT_COLUMNS`.
        sleep_df: DataFrame in :data:`SLEEP_OUTPUT_COLUMNS`.

    Returns:
        UTF-8 encoded bytes. Returns ``b""`` if both inputs are empty.

    Notes:
        - Activities are written using pandas ``to_csv`` with ``float_format='%.2f'``.
        - Sleep rows are written with ``csv.writer`` and ``QUOTE_ALL`` to avoid issues
          when fields contain commas or "N/A" strings.
    """

    if activity_df.empty and sleep_df.empty:
        return b""

    buf = io.StringIO()

    # Section 1: Activities
    buf.write("Activities\n")
    activity_df.to_csv(buf, index=False, float_format="%.2f")

    # Section 2: Sleep
    buf.write("\nSleep\n")
    buf.write(",".join(SLEEP_OUTPUT_COLUMNS) + "\n")

    w = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for _, r in sleep_df.iterrows():
        w.writerow([r.get(c, "") for c in SLEEP_OUTPUT_COLUMNS])

    return buf.getvalue().encode("utf-8")


def _safe_extractall(zf: zipfile.ZipFile, dest_dir: Path) -> None:
    """Safely extract a zip file into ``dest_dir``.

    This prevents the "Zip Slip" path traversal issue where a crafted archive could
    write files outside of the intended extraction directory.

    Args:
        zf: Open ``zipfile.ZipFile``.
        dest_dir: Extraction directory.

    Raises:
        ValueError: If any archive member would extract outside ``dest_dir``.
    """

    dest_dir = dest_dir.resolve()

    for member in zf.infolist():
        # Zip members always use forward slashes, regardless of platform.
        target_path = (dest_dir / member.filename).resolve()
        if dest_dir not in target_path.parents and target_path != dest_dir:
            raise ValueError(f"Unsafe path in zip: {member.filename}")

    zf.extractall(dest_dir)


def convert_takeout_zip_bytes(
    zip_bytes: bytes,
    intersect_dates: bool = True,
    user_start: Optional[pd.Timestamp] = None,
    user_end: Optional[pd.Timestamp] = None,
) -> Tuple[bytes, pd.DataFrame, pd.DataFrame, Optional[Tuple[pd.Timestamp, pd.Timestamp]]]:
    """Convert a Google Takeout Fitbit ZIP (bytes) into the combined CSV output.

    This is the main programmatic entry point. It performs:

    1) ZIP extraction to a temporary directory
    2) Discovery of the Fitbit "Global Export Data" folder
    3) Aggregation into activity and sleep tables
    4) Serialization into CSV bytes

    Args:
        zip_bytes: Raw bytes of a Google Takeout ZIP file.
        intersect_dates: See :func:`build_outputs`.
        user_start: Optional inclusive start date.
        user_end: Optional inclusive end date.

    Returns:
        Tuple of:
        - ``csv_bytes``: Combined CSV bytes (UTF-8)
        - ``activity_df``: Daily activity DataFrame
        - ``sleep_df``: Sleep log DataFrame
        - ``date_range``: Final inclusive date range used (or None)

    Raises:
        zipfile.BadZipFile: If the input is not a valid ZIP.
        FileNotFoundError: If the Fitbit export folder cannot be found.
        ValueError: If the ZIP contains unsafe extraction paths.

    Performance notes:
        The entire archive is extracted to disk. Large Takeout exports may take a
        few seconds and use temporary disk space.
    """

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            _safe_extractall(zf, root)

        global_folder = find_global_export_data_folder(root)

        activity_df, sleep_df, date_range = build_outputs(
            global_folder,
            intersect_dates=intersect_dates,
            user_start=user_start,
            user_end=user_end,
        )

        out_bytes = write_combined_csv_bytes(activity_df, sleep_df)
        return out_bytes, activity_df, sleep_df, date_range


def _sanitize_filename_part(s: str) -> str:
    """Make a string safe to use as part of a filename.

    Args:
        s: Raw string (e.g., participant id).

    Returns:
        A normalized string consisting of ASCII letters/digits plus ``_`` and ``-``.
        Empty/invalid inputs become ``"UNKNOWN"``.
    """

    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-]+", "", s)
    return s or "UNKNOWN"

def sanitize_filename_part(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s)
    return s or "participant"
