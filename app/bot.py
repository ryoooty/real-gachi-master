from __future__ import annotations

import asyncio
import datetime as dt
import os
import sqlite3
from typing import Any, Dict, List, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message, TelegramObject
from aiogram import BaseMiddleware
import pytz
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from app import ai, database
from app.keyboards import (
    DifficultyCallback,
    ExerciseCallback,
    difficulty_keyboard,
    exercises_keyboard,
    main_menu_keyboard,
)
from app.scheduler import WorkoutScheduler
from app.time_utils import convert_local_time_to_utc, convert_range_to_utc

load_dotenv()

router = Router()
MAX_USERS = 2
FALLBACK_WORKOUT = [
    {"name": "Отжимания", "reps": 15},
    {"name": "Приседания", "reps": 25},
    {"name": "Планка", "seconds": 45},
]


class AccessMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: Dict[str, Any]):
        if not hasattr(event, "chat"):
            return await handler(event, data)
        chat_id = event.chat.id
        user = database.get_user(chat_id)
        if user:
            return await handler(event, data)
        if database.get_user_count() >= MAX_USERS:
            if isinstance(event, Message):
                await event.answer("Мест нет. Бот работает только для двух пользователей.")
            return None
        return await handler(event, data)


class ProfileStates(StatesGroup):
    weight = State()
    height = State()
    age = State()
    level = State()
    injuries = State()


class SettingsStates(StatesGroup):
    waiting_mode = State()
    fixed_time = State()
    range_start = State()
    range_end = State()
    timezone = State()


def human_day_name(date: dt.date) -> str:
    return date.strftime("%d.%m.%Y")


def weekday_key(date: dt.date) -> str:
    return date.strftime("%A").lower()


def compose_workout_text(date: dt.date, exercises: List[Dict[str, Any]]) -> str:
    lines = [f"Тренировка на {human_day_name(date)}", ""]
    for exercise in exercises:
        prefix = "✅ " if exercise.get("done") else "[ ] "
        line = prefix + exercise["name"]
        if "reps" in exercise:
            line += f": {exercise['reps']} раз"
        if "seconds" in exercise:
            line += f": {exercise['seconds']} сек"
        if "minutes" in exercise:
            line += f": {exercise['minutes']} мин"
        lines.append(line)
    return "\n".join(lines)


def ensure_profile(message: Message) -> Optional[sqlite3.Row]:
    user = database.get_user(message.chat.id)
    if user:
        return user
    database.upsert_user(message.chat.id)
    return database.get_user(message.chat.id)


def parse_int(text: str) -> Optional[int]:
    try:
        return int(text)
    except ValueError:
        return None


def validate_time(text: str) -> bool:
    try:
        dt.time.fromisoformat(text)
        return True
    except ValueError:
        return False


def profile_ready(user: sqlite3.Row) -> bool:
    record = dict(user)
    return all(record.get(field) is not None for field in ("weight", "height", "age", "level", "injuries"))


def profile_summary(user: sqlite3.Row) -> str:
    return (
        "Твой профиль:\n"
        f"Вес: {user['weight']} кг\n"
        f"Рост: {user['height']} см\n"
        f"Возраст: {user['age']}\n"
        f"Уровень: {user['level']}\n"
        f"Ограничения: {user['injuries'] or 'нет'}"
    )


def calculate_streak(user_id: int) -> int:
    dates = database.completion_dates(user_id)
    if not dates:
        return 0
    streak = 0
    expected = dt.date.today()
    for d in dates:
        current = dt.date.fromisoformat(d)
        if current == expected:
            streak += 1
            expected = expected - dt.timedelta(days=1)
        elif current < expected:
            break
    return streak


def calculate_max_streak(user_id: int) -> int:
    dates = sorted(database.completion_dates(user_id))
    best = 0
    current = 0
    prev: Optional[dt.date] = None
    for d in dates:
        day = dt.date.fromisoformat(d)
        if prev and (day - prev).days == 1:
            current += 1
        else:
            current = 1
        best = max(best, current)
        prev = day
    return best


def close_previous_day_if_pending(user_id: int, today: dt.date) -> None:
    yesterday = today - dt.timedelta(days=1)
    previous_log = database.load_daily_log(user_id=user_id, date=yesterday.isoformat())
    if not previous_log:
        return
    if previous_log.get("points"):
        return
    database.update_daily_log(
        user_id=user_id,
        date=yesterday.isoformat(),
        exercises_done=previous_log.get("exercises_done", []),
        difficulty_rate=previous_log.get("difficulty_rate") or "skipped",
        points=0,
    )


async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs: Any) -> None:
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except TelegramForbiddenError:
        # user blocked the bot, avoid crashing scheduler
        return


@router.message(CommandStart())
async def start(message: Message, state: FSMContext, scheduler: WorkoutScheduler) -> None:
    database.init_db()
    user = database.get_user(message.chat.id)
    if not user:
        if database.get_user_count() >= MAX_USERS:
            await message.answer("Мест нет. Бот работает только для двух пользователей.")
            return
        now_utc = dt.datetime.now(dt.timezone.utc)
        database.upsert_user(
            message.chat.id,
            notify_time_utc=now_utc.strftime("%H:%M"),
            notify_time_utc_iso=now_utc.isoformat(),
        )
    await state.clear()
    await message.answer(
        "Привет! Я помогу планировать тренировки. Используй меню ниже, чтобы начать.",
        reply_markup=main_menu_keyboard(),
    )
    user = database.get_user(message.chat.id)
    if user:
        _schedule_user_from_row(scheduler, user)


@router.message(StateFilter("*"), F.text.in_({"👤 Мой Профиль", "✏️ Изменить профиль"}))
async def edit_profile(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_profile(message)
    if user and profile_ready(user) and message.text != "✏️ Изменить профиль":
        await message.answer(
            profile_summary(user)
            + "\n\nЕсли хочешь обновить данные, нажми '✏️ Изменить профиль' или введи любые новые значения.",
            reply_markup=main_menu_keyboard(),
        )
        return

    await state.set_state(ProfileStates.weight)
    await message.answer("Введи вес (кг):")


@router.message(ProfileStates.weight)
async def set_weight(message: Message, state: FSMContext) -> None:
    weight = parse_int(message.text)
    if weight is None:
        await message.answer("Нужно число. Введи вес (кг):")
        return
    await state.update_data(weight=weight)
    await state.set_state(ProfileStates.height)
    await message.answer("Введи рост (см):")


@router.message(ProfileStates.height)
async def set_height(message: Message, state: FSMContext) -> None:
    height = parse_int(message.text)
    if height is None:
        await message.answer("Нужно число. Введи рост (см):")
        return
    await state.update_data(height=height)
    await state.set_state(ProfileStates.age)
    await message.answer("Введи возраст:")


@router.message(ProfileStates.age)
async def set_age(message: Message, state: FSMContext) -> None:
    age = parse_int(message.text)
    if age is None:
        await message.answer("Нужно число. Введи возраст:")
        return
    await state.update_data(age=age)
    await state.set_state(ProfileStates.level)
    await message.answer("Укажи уровень (Новичок/Про):")


@router.message(ProfileStates.level)
async def set_level(message: Message, state: FSMContext) -> None:
    await state.update_data(level=message.text)
    await state.set_state(ProfileStates.injuries)
    await message.answer("Есть ли травмы или ограничения?")


@router.message(ProfileStates.injuries)
async def finish_profile(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    data["injuries"] = message.text
    database.upsert_user(
        message.chat.id,
        weight=data.get("weight"),
        height=data.get("height"),
        age=data.get("age"),
        level=data.get("level"),
        injuries=data.get("injuries"),
    )
    await state.clear()
    await message.answer("Профиль обновлен!", reply_markup=main_menu_keyboard())


@router.message(StateFilter("*"), F.text == "⚙️ Настройки")
async def settings_entry(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(SettingsStates.waiting_mode)
    await message.answer(
        "Выбери режим уведомлений: напиши 'Точное время' или 'Диапазон'.\n"
        "Или отправь 'Часовой пояс' для смены часового пояса (пример: Europe/Moscow)."
    )


@router.message(SettingsStates.waiting_mode)
async def choose_mode(message: Message, state: FSMContext, scheduler: WorkoutScheduler) -> None:
    text = message.text.lower()
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return
    if "часовой" in text:
        await state.set_state(SettingsStates.timezone)
        await message.answer("Введи таймзону, например Europe/Moscow")
        return
    if "точное" in text:
        await state.set_state(SettingsStates.fixed_time)
        await message.answer("Введи время HH:MM")
        return
    if "диапазон" in text:
        await state.set_state(SettingsStates.range_start)
        await message.answer("Введи начало диапазона HH:MM")
        return
    await message.answer("Не понял. Напиши 'Точное время', 'Диапазон' или 'Часовой пояс'.")


@router.message(SettingsStates.timezone)
async def set_timezone(message: Message, state: FSMContext) -> None:
    tz = message.text.strip()
    try:
        dt.timezone(dt.timedelta())  # dummy to keep static analyzers silent
        convert_local_time_to_utc("00:00", tz)
    except Exception:
        await message.answer("Неверная таймзона. Пример: Europe/Moscow")
        return
    database.upsert_user(message.chat.id, timezone=tz)
    await state.clear()
    await message.answer("Часовой пояс обновлен.", reply_markup=main_menu_keyboard())


@router.message(SettingsStates.fixed_time)
async def set_fixed_time(message: Message, state: FSMContext, scheduler: WorkoutScheduler) -> None:
    if not validate_time(message.text):
        await message.answer("Нужно время в формате HH:MM")
        return
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала профиль.")
        return
    utc_dt = convert_local_time_to_utc(message.text, user["timezone"])
    utc_time = utc_dt.strftime("%H:%M")
    database.upsert_user(
        message.chat.id,
        notify_time_utc=utc_time,
        notify_time_utc_iso=utc_dt.isoformat(),
        notify_mode="fixed",
        notify_range_start_utc=None,
        notify_range_end_utc=None,
        notify_range_start_utc_iso=None,
        notify_range_end_utc_iso=None,
    )
    _schedule_user_from_row(scheduler, database.get_user(message.chat.id))
    await state.clear()
    await message.answer(f"Время сохранено (UTC {utc_time}).", reply_markup=main_menu_keyboard())


@router.message(SettingsStates.range_start)
async def set_range_start(message: Message, state: FSMContext) -> None:
    if not validate_time(message.text):
        await message.answer("Нужно время в формате HH:MM")
        return
    await state.update_data(range_start=message.text)
    await state.set_state(SettingsStates.range_end)
    await message.answer("Теперь введи конец диапазона HH:MM")


@router.message(SettingsStates.range_end)
async def set_range_end(message: Message, state: FSMContext, scheduler: WorkoutScheduler) -> None:
    if not validate_time(message.text):
        await message.answer("Нужно время в формате HH:MM")
        return
    data = await state.get_data()
    start_local = data.get("range_start")
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала профиль.")
        return
    start_utc_dt, end_utc_dt = convert_range_to_utc(start_local, message.text, user["timezone"])
    start_utc = start_utc_dt.strftime("%H:%M")
    end_utc = end_utc_dt.strftime("%H:%M")
    database.upsert_user(
        message.chat.id,
        notify_mode="range",
        notify_range_start_utc=start_utc,
        notify_range_end_utc=end_utc,
        notify_range_start_utc_iso=start_utc_dt.isoformat(),
        notify_range_end_utc_iso=end_utc_dt.isoformat(),
    )
    _schedule_user_from_row(scheduler, database.get_user(message.chat.id))
    await state.clear()
    await message.answer(
        f"Диапазон сохранен (UTC {start_utc}-{end_utc}).",
        reply_markup=main_menu_keyboard(),
    )


@router.message(StateFilter("*"), F.text == "📅 План на сегодня")
async def today_plan(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return

    today = dt.date.today()
    close_previous_day_if_pending(user["id"], today)
    plan = database.get_plan_for_day(user["id"], weekday_key(today))
    exercises = FALLBACK_WORKOUT if plan is None else plan[1]
    is_rest = False if plan is None else plan[0]
    existing_log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    if existing_log and existing_log.get("exercises_done"):
        exercises = existing_log["exercises_done"]
    if existing_log and existing_log.get("points"):
        await message.answer("Тренировка за сегодня уже сохранена.", reply_markup=main_menu_keyboard())
        return
    if plan is None:
        await message.answer("План кончился! Выполняем запасную тренировку.")
    if is_rest:
        await message.answer("Сегодня отдых, восстанавливай силы!")
        return

    completed = [ex.get("done", False) for ex in exercises]
    database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises)
    text = compose_workout_text(today, exercises)
    await message.answer(text, reply_markup=exercises_keyboard(exercises, completed))


@router.callback_query(ExerciseCallback.filter())
async def handle_exercise_callback(callback: CallbackQuery, callback_data: ExerciseCallback) -> None:
    user = database.get_user(callback.message.chat.id)
    if not user:
        await callback.answer("Нет профиля")
        return
    today = dt.date.today()
    log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    if log is None:
        await callback.answer("План не найден")
        return

    exercises = log["exercises_done"]
    completed = [item.get("done", False) for item in exercises]

    if callback_data.index == -1:
        if log.get("points"):
            await callback.answer("Тренировка уже завершена")
            return
        text = "День пропущен. Не забывай вернуться завтра!"
        keep_points = log.get("points", 0)
        database.update_daily_log(
            user_id=user["id"],
            date=today.isoformat(),
            exercises_done=exercises,
            difficulty_rate="skipped",
            points=0,
        )
        await callback.message.edit_text(text)
        await callback.answer()
        return

    completed[callback_data.index] = callback_data.completed
    for idx, exercise in enumerate(exercises):
        exercise["done"] = completed[idx]

    all_done = all(completed)
    database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises)

    if all_done:
        await callback.message.edit_text(
            "🎉 Тренировка завершена!\nОценка сложности:", reply_markup=difficulty_keyboard()
        )
        await callback.answer("Отлично!")
        return

    text = compose_workout_text(today, exercises)
    await callback.message.edit_text(text, reply_markup=exercises_keyboard(exercises, completed))
    await callback.answer("Обновлено")


@router.callback_query(DifficultyCallback.filter())
async def handle_difficulty_callback(callback: CallbackQuery, callback_data: DifficultyCallback) -> None:
    user = database.get_user(callback.message.chat.id)
    if not user:
        await callback.answer("Нет профиля")
        return
    today = dt.date.today().isoformat()
    log = database.load_daily_log(user_id=user["id"], date=today)
    if not log:
        await callback.answer("Нет тренировки")
        return

    exercises = log["exercises_done"]
    points = sum(3 if ex.get("name", "").lower().startswith("pull") else 1 for ex in exercises if ex.get("done"))
    database.update_daily_log(
        user_id=user["id"],
        date=today,
        exercises_done=exercises,
        difficulty_rate=callback_data.rate,
        points=points,
    )
    await callback.message.edit_text("Сложность сохранена, очки начислены!")
    await callback.answer("Спасибо за отзыв")


@router.message(StateFilter("*"), F.text == "📈 Статистика")
async def show_stats(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала профиль.")
        return
    total = database.total_points(user["id"])
    streak = calculate_streak(user["id"])
    max_streak = calculate_max_streak(user["id"])
    completed_days = len(database.completion_dates(user["id"]))
    leaders = database.leaderboard()
    leaderboard_text = "\n".join([f"{idx+1}. {item[0]} — {item[1]} очков" for idx, item in enumerate(leaders)]) or "Нет данных"
    await message.answer(
        f"Очки: {total}\nСтрик: {streak} дней (рекорд {max_streak})\n"
        f"Выполнено дней: {completed_days}\nЛидерборд:\n{leaderboard_text}",
        reply_markup=main_menu_keyboard(),
    )


@router.message(Command("generate"))
async def manual_generate(message: Message) -> None:
    await weekly_generation(message.chat.id)
    await message.answer("Сформирован новый недельный план.")


async def weekly_generation(chat_id: int) -> None:
    user = database.get_user(chat_id)
    if not user:
        return
    completion_dates = database.completion_dates(user["id"])
    completion_rate = min(100, len(completion_dates) * 100 // 7) if completion_dates else 0
    last_difficulty = database.load_daily_log(user["id"], dt.date.today().isoformat()) or {}
    perceived = last_difficulty.get("difficulty_rate") or "normal"
    profile = ai.UserProfile(
        weight=user["weight"] or 80,
        height=user["height"] or 180,
        age=user["age"] or 25,
        level=user["level"] or "Новичок",
        injuries=user["injuries"] or "нет",
        completion_rate=completion_rate,
        perceived_difficulty=perceived,
    )
    client = ai.DeepSeekClient()
    raw_plan = client.generate_weekly_plan(profile)
    adjusted = ai.adjust_plan(raw_plan, perceived)
    client.persist_weekly_plan(chat_id, adjusted)


async def scheduled_push(bot: Bot, chat_id: int) -> None:
    user = database.get_user(chat_id)
    if not user:
        return
    today = dt.date.today()
    close_previous_day_if_pending(user["id"], today)
    plan = database.get_plan_for_day(user["id"], weekday_key(today))
    existing_log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    if plan is None:
        await safe_send(bot, chat_id, "План кончился! Жми кнопку генерации или выполняй запасную тренировку.")
        exercises = existing_log["exercises_done"] if existing_log and existing_log.get("exercises_done") else FALLBACK_WORKOUT
    else:
        is_rest, exercises = plan
        if existing_log and existing_log.get("exercises_done"):
            exercises = existing_log["exercises_done"]
        if is_rest:
            await safe_send(bot, chat_id, "Сегодня отдых, восстанавливай силы!")
            return
    if existing_log and existing_log.get("points"):
        return
    completed = [ex.get("done", False) for ex in exercises]
    database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises)
    text = compose_workout_text(today, exercises)
    await safe_send(bot, chat_id, text, reply_markup=exercises_keyboard(exercises, completed))


def _schedule_user_from_row(scheduler: WorkoutScheduler, user_row) -> None:
    if not user_row:
        return
    mode = user_row["notify_mode"] or "fixed"
    if mode == "range" and user_row["notify_range_start_utc"] and user_row["notify_range_end_utc"]:
        if user_row["notify_range_start_utc_iso"] and user_row["notify_range_end_utc_iso"]:
            start_dt = dt.datetime.fromisoformat(user_row["notify_range_start_utc_iso"]).astimezone(dt.timezone.utc)
            end_dt = dt.datetime.fromisoformat(user_row["notify_range_end_utc_iso"]).astimezone(dt.timezone.utc)
            scheduler._range_job(chat_id=user_row["chat_id"], start_utc=start_dt, end_utc=end_dt)
        else:
            scheduler.schedule_range(
                chat_id=user_row["chat_id"],
                start_local=user_row["notify_range_start_utc"],
                end_local=user_row["notify_range_end_utc"],
                timezone="UTC",
            )
    elif user_row["notify_time_utc"]:
        if user_row["notify_time_utc_iso"]:
            parsed = dt.datetime.fromisoformat(user_row["notify_time_utc_iso"]).astimezone(dt.timezone.utc)
            scheduler.schedule_fixed(chat_id=user_row["chat_id"], local_time=parsed.strftime("%H:%M"), timezone="UTC")
        else:
            scheduler.schedule_fixed(chat_id=user_row["chat_id"], local_time=user_row["notify_time_utc"], timezone="UTC")


async def on_startup(bot: Bot, scheduler: WorkoutScheduler) -> None:
    database.init_db()
    for user in database.list_users():
        _schedule_user_from_row(scheduler, user)
    scheduler.start()
    # weekly generation every Sunday 18:00 UTC
    scheduler.scheduler.add_job(
        lambda: asyncio.create_task(generate_all(bot)),
        CronTrigger(day_of_week="sun", hour=18, minute=0, timezone=pytz.UTC),
    )


async def generate_all(bot: Bot) -> None:
    for user in database.list_users():
        await weekly_generation(user["chat_id"])
        await safe_send(bot, user["chat_id"], "Новая недельная программа сгенерирована.")


async def main() -> None:
    database.init_db()
    bot = Bot(
        token=os.getenv("BOT_TOKEN", "DUMMY"),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    scheduler = WorkoutScheduler(lambda chat_id: scheduled_push(bot, chat_id))
    dp.update.middleware(AccessMiddleware())
    dp.include_router(router)
    dp['scheduler'] = scheduler

    await on_startup(bot, scheduler)
    await dp.start_polling(bot, scheduler=scheduler)


if __name__ == "__main__":
    asyncio.run(main())

