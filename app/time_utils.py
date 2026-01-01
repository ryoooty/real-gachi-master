from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo


MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def convert_local_time_to_utc(time_str: str, timezone: str | None = None) -> str:
    """Convert HH:MM in provided timezone to UTC HH:MM string.

    The conversion relies on datetime and ZoneInfo to safely cross day boundaries
    without manual arithmetic.
    """
    tzinfo = ZoneInfo(timezone or "Europe/Moscow")
    local_time = dt.datetime.combine(dt.date.today(), dt.time.fromisoformat(time_str), tzinfo)
    utc_time = local_time.astimezone(dt.timezone.utc)
    return utc_time.strftime("%H:%M")


def convert_range_to_utc(start: str, end: str, timezone: str | None = None) -> tuple[str, str]:
    return convert_local_time_to_utc(start, timezone), convert_local_time_to_utc(end, timezone)


def utc_now_time_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%H:%M")
