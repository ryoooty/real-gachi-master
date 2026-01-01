from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _normalize_local_datetime(time_str: str, timezone: str | None = None) -> dt.datetime:
    tzinfo = ZoneInfo(timezone or "Europe/Moscow")
    now_local = dt.datetime.now(tzinfo)
    candidate = dt.datetime.combine(now_local.date(), dt.time.fromisoformat(time_str), tzinfo)
    if candidate <= now_local:
        candidate += dt.timedelta(days=1)
    return candidate


def convert_local_time_to_utc(time_str: str, timezone: str | None = None) -> dt.datetime:
    """Convert HH:MM in provided timezone to next UTC datetime.

    Returns a timezone-aware datetime to keep the exact date when converting
    across day boundaries (e.g. 23:30 in UTC+12 should become previous UTC day).
    """
    local_dt = _normalize_local_datetime(time_str, timezone)
    return local_dt.astimezone(dt.timezone.utc)


def convert_range_to_utc(start: str, end: str, timezone: str | None = None) -> tuple[dt.datetime, dt.datetime]:
    start_dt = _normalize_local_datetime(start, timezone)
    end_dt = dt.datetime.combine(start_dt.date(), dt.time.fromisoformat(end), start_dt.tzinfo)
    if end_dt <= start_dt:
        end_dt += dt.timedelta(days=1)
    return start_dt.astimezone(dt.timezone.utc), end_dt.astimezone(dt.timezone.utc)


def utc_now_time_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%H:%M")
