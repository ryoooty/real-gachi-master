from __future__ import annotations

import asyncio
import datetime as dt
from typing import Awaitable, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.time_utils import convert_local_time_to_utc


class WorkoutScheduler:
    def __init__(self, on_trigger: Callable[[int], Awaitable[None]]):
        self.scheduler = AsyncIOScheduler(timezone=dt.timezone.utc)
        self.on_trigger = on_trigger

    def schedule_user(self, chat_id: int, local_time: str, timezone: str | None = None) -> None:
        utc_time = convert_local_time_to_utc(local_time, timezone)
        hour, minute = map(int, utc_time.split(":"))
        trigger = CronTrigger(hour=hour, minute=minute, timezone=dt.timezone.utc)
        self.scheduler.add_job(self._wrap(chat_id), trigger, id=f"notify-{chat_id}", replace_existing=True)

    def start(self) -> None:
        if not self.scheduler.running:
            self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown()

    def _wrap(self, chat_id: int) -> Callable[[], Awaitable[None]]:
        async def job() -> None:
            await self.on_trigger(chat_id)

        return job
