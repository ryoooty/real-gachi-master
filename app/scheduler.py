from __future__ import annotations

import datetime as dt
import random
from typing import Awaitable, Callable, Optional

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from app.time_utils import convert_local_time_to_utc, convert_range_to_utc


class WorkoutScheduler:
    def __init__(self, on_trigger: Callable[[int], Awaitable[None]]):
        self.scheduler = AsyncIOScheduler(timezone=pytz.UTC)
        self.on_trigger = on_trigger

    def schedule_fixed(self, chat_id: int, local_time: str, timezone: str | None = None) -> None:
        utc_dt = convert_local_time_to_utc(local_time, timezone)
        trigger = CronTrigger(hour=utc_dt.hour, minute=utc_dt.minute, timezone=pytz.UTC)
        next_run = utc_dt if utc_dt > dt.datetime.now(pytz.UTC) else None
        self.scheduler.add_job(
            self._wrap(chat_id, mode="fixed"),
            trigger,
            id=f"notify-{chat_id}",
            replace_existing=True,
            next_run_time=next_run,
        )

    def _range_job(self, chat_id: int, start_utc: dt.datetime, end_utc: dt.datetime) -> None:
        now = dt.datetime.now(pytz.UTC)
        start_dt = start_utc
        end_dt = end_utc
        if now >= end_dt:
            start_dt += dt.timedelta(days=1)
            end_dt += dt.timedelta(days=1)

        span_seconds = int((end_dt - start_dt).total_seconds())
        fire_dt = start_dt + dt.timedelta(seconds=random.randint(0, span_seconds))
        self.scheduler.add_job(
            self._wrap(chat_id, mode="range", start=start_utc, end=end_utc),
            DateTrigger(run_date=fire_dt),
            id=f"notify-{chat_id}",
            replace_existing=True,
        )

    def schedule_range(self, chat_id: int, start_local: str, end_local: str, timezone: str | None = None) -> None:
        start_utc, end_utc = convert_range_to_utc(start_local, end_local, timezone)
        self._range_job(chat_id, start_utc, end_utc)

    def start(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown()

    def _wrap(
        self, chat_id: int, mode: str, start: Optional[dt.datetime] = None, end: Optional[dt.datetime] = None
    ) -> Callable[[], Awaitable[None]]:
        async def job() -> None:
            await self.on_trigger(chat_id)
            if mode == "range" and start and end:
                self._range_job(chat_id, start, end)

        return job
