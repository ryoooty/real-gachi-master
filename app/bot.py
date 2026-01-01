from __future__ import annotations

import asyncio
import datetime as dt
import io
import os
import random
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message, TelegramObject, ReplyKeyboardMarkup
from aiogram import BaseMiddleware
import pytz
from dotenv import load_dotenv

from app import database
from app.keyboards import (
    ExerciseCallback,
    ProfileCallback,
    SettingsCallback,
    exercises_keyboard,
    main_menu_keyboard,
    profile_keyboard,
    settings_keyboard,
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
    nickname = State()
    weight = State()
    height = State()
    age = State()


class SettingsStates(StatesGroup):
    fixed_time = State()
    range_start = State()
    range_end = State()
    timezone = State()
    additional_count = State()


class PlanStates(StatesGroup):
    file_upload = State()


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
    derived_name = (
        message.from_user.full_name
        or message.from_user.username
        or (str(message.from_user.id) if message.from_user else None)
    )
    if user:
        if user["nickname"] is None and derived_name:
            database.upsert_user(message.chat.id, nickname=derived_name)
            user = database.get_user(message.chat.id)
        return user
    database.upsert_user(message.chat.id, nickname=derived_name)
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


def _log_exercises(log: Dict[str, Any] | None, session: str = "main") -> List[Dict[str, Any]]:
    if not log:
        return []
    exercises = log.get("exercises_done", [])
    if isinstance(exercises, dict):
        return list(exercises.get(session, []))
    if session == "main":
        return list(exercises)
    return []


def _store_log_exercises(
    log: Dict[str, Any] | None, session: str, exercises: List[Dict[str, Any]]
) -> Dict[str, Any]:
    existing = log.get("exercises_done") if log else {}
    if not isinstance(existing, dict):
        existing = {"main": existing or []}
    existing[session] = exercises
    return existing


def _parse_exercise_line(line: str) -> Optional[Dict[str, Any]]:
    parts = [segment.strip() for segment in line.replace("—", "-").split("-")]
    parts = [p for p in parts if p]
    if len(parts) < 2:
        return None
    name = parts[0]
    reps = parts[1]
    points = parse_int(parts[2]) if len(parts) > 2 else None
    exercise: Dict[str, Any] = {"name": name}
    if reps.endswith("м"):
        exercise["meters"] = reps
    elif reps.endswith("с"):
        exercise["seconds"] = parse_int(reps[:-1]) or reps
    else:
        exercise["reps"] = parse_int(reps) or reps
    if points is not None:
        exercise["points"] = points
    return exercise


def parse_plan_content(content: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    plan_days: List[Dict[str, Any]] = []
    additional: List[Dict[str, Any]] = []
    idx = 0
    while idx < len(lines):
        if not lines[idx].isdigit():
            break
        day_number = int(lines[idx])
        idx += 1
        if idx >= len(lines):
            break
        if day_number == 0:
            while idx < len(lines):
                exercise = _parse_exercise_line(lines[idx])
                if exercise:
                    additional.append(exercise)
                idx += 1
            break
        title = lines[idx]
        idx += 1
        exercises: List[Dict[str, Any]] = []
        while idx < len(lines) and not lines[idx].isdigit():
            exercise = _parse_exercise_line(lines[idx])
            if exercise:
                exercises.append(exercise)
            idx += 1
        plan_days.append({"day_index": day_number, "title": title, "exercises": exercises})
    return plan_days, additional


def profile_ready(user: sqlite3.Row) -> bool:
    record = dict(user)
    return all(
        record.get(field) is not None for field in ("nickname", "weight", "height", "age")
    )


def plan_button_label(user_id: Optional[int]) -> str:
    if not user_id:
        return "📅 План на сегодня"
    today = dt.date.today().isoformat()
    log = database.load_daily_log(user_id=user_id, date=today)
    if log and log.get("points"):
        return "💪 Доп тренировка"
    return "📅 План на сегодня"


def format_profile(user: sqlite3.Row) -> str:
    streak = calculate_streak(user["id"])
    total_points_value = database.total_points(user["id"])
    completed_days = len(database.completion_dates(user["id"]))
    nickname = user["nickname"] or f"User {user['chat_id']}"
    return (
        f"{nickname}\n\n"
        f"Вин-стрик: {streak} дней\n"
        f"Всего выполнено дней: {completed_days}\n"
        f"Очки: {total_points_value}"
    )


def menu_for_user(user: Optional[sqlite3.Row]) -> ReplyKeyboardMarkup:
    user_id = user["id"] if user else None
    return main_menu_keyboard(plan_label=plan_button_label(user_id))


def _display_time(iso_value: Optional[str], fallback: Optional[str], timezone: str) -> str:
    if iso_value:
        dt_obj = dt.datetime.fromisoformat(iso_value)
        local = dt_obj.astimezone(pytz.timezone(timezone))
        return local.strftime("%H:%M")
    if fallback:
        return fallback
    return "—"


def settings_overview(user: sqlite3.Row) -> str:
    mode = user["notify_mode"] or "fixed"
    timezone = user["timezone"] or "UTC"
    if mode == "range":
        start_local = _display_time(user["notify_range_start_utc_iso"], user["notify_range_start_utc"], timezone)
        end_local = _display_time(user["notify_range_end_utc_iso"], user["notify_range_end_utc"], timezone)
        timing = f"Диапазон: {start_local} - {end_local}"
    else:
        fixed_local = _display_time(user["notify_time_utc_iso"], user["notify_time_utc"], timezone)
        timing = f"Точное время: {fixed_local}"
    additional_count = user["additional_tasks_count"] or 1
    return f"{timing}\nЧасовой пояс: {timezone}\nДоп. заданий: {additional_count}"


async def send_settings(message: Message | CallbackQuery, user: sqlite3.Row) -> None:
    text = settings_overview(user) + "\n\nВыбери режим:"
    markup = settings_keyboard(user["notify_mode"] or "fixed")
    if isinstance(message, CallbackQuery):
        await message.message.edit_text(text, reply_markup=markup)
    else:
        await message.answer(text, reply_markup=markup)


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


def pluralize_days(value: int) -> str:
    last_two = abs(value) % 100
    last_one = abs(value) % 10
    if 11 <= last_two <= 14:
        suffix = "дней"
    elif last_one == 1:
        suffix = "день"
    elif 2 <= last_one <= 4:
        suffix = "дня"
    else:
        suffix = "дней"
    return f"{value} {suffix}"


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
            nickname=message.from_user.full_name
            or message.from_user.username
            or (str(message.from_user.id) if message.from_user else None),
            notify_time_utc=now_utc.strftime("%H:%M"),
            notify_time_utc_iso=now_utc.isoformat(),
        )
    await state.clear()
    await message.answer(
        "Привет! Я помогу планировать тренировки. Используй меню ниже, чтобы начать.",
        reply_markup=menu_for_user(database.get_user(message.chat.id)),
    )
    user = database.get_user(message.chat.id)
    if user:
        _schedule_user_from_row(scheduler, user)


@router.message(Command("plan"))
async def request_plan_file(message: Message, state: FSMContext) -> None:
    await state.set_state(PlanStates.file_upload)
    await message.answer(
        "Пришли .txt файл с планом тренировок.\n"
        "Формат: номер дня, название, затем упражнения через дефис."
    )


@router.message(PlanStates.file_upload)
async def handle_plan_file(message: Message, state: FSMContext, bot: Bot) -> None:
    if not message.document:
        await message.answer("Нужен .txt файл с планом.")
        return
    buffer = io.BytesIO()
    await message.document.download(destination=buffer)
    try:
        content = buffer.getvalue().decode("utf-8-sig")
    except UnicodeDecodeError:
        await message.answer("Не удалось прочитать файл, нужна UTF-8 кодировка.")
        return

    plan_days, extras = parse_plan_content(content)
    if not plan_days:
        await message.answer("Не найдено ни одного дня плана.")
        return

    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return

    database.replace_plan(user_id=user["id"], plan=plan_days, start_date=dt.date.today())
    database.save_additional_exercises(user_id=user["id"], exercises=extras)
    await state.clear()
    await message.answer(
        f"План из {len(plan_days)} дней сохранен. Доп. упражнений: {len(extras)}.",
        reply_markup=menu_for_user(user),
    )


@router.message(StateFilter("*"), F.text.in_({"👤 Профиль", "👤 Мой Профиль"}))
async def show_profile(message: Message, state: FSMContext) -> None:
    user = ensure_profile(message)
    if not user:
        await message.answer("Не удалось загрузить профиль.")
        return
    if not profile_ready(user):
        await state.set_state(ProfileStates.nickname)
        await state.update_data(mode="all")
        await message.answer("Давай заполним профиль. Как тебя называть?", reply_markup=menu_for_user(user))
        return
    await state.clear()
    await message.answer(format_profile(user), reply_markup=profile_keyboard(user["weight"], user["height"]))


@router.callback_query(ProfileCallback.filter())
async def handle_profile_callback(callback: CallbackQuery, callback_data: ProfileCallback, state: FSMContext) -> None:
    user = database.get_user(callback.message.chat.id)
    if not user:
        await callback.answer("Нет профиля")
        return
    await state.clear()
    if callback_data.action == "all":
        await state.set_state(ProfileStates.nickname)
        await state.update_data(mode="all")
        await callback.message.answer("Обновим профиль. Введи ник:")
    elif callback_data.action == "weight":
        await state.set_state(ProfileStates.weight)
        await state.update_data(mode="single", target="weight")
        await callback.message.answer("Введи вес (кг):")
    elif callback_data.action == "height":
        await state.set_state(ProfileStates.height)
        await state.update_data(mode="single", target="height")
        await callback.message.answer("Введи рост (см):")
    elif callback_data.action == "nickname":
        await state.set_state(ProfileStates.nickname)
        await state.update_data(mode="single", target="nickname")
        await callback.message.answer("Введи ник:")
    await callback.answer()


async def _finish_single_field(message: Message, field: str, value: Any, state: FSMContext) -> None:
    database.upsert_user(message.chat.id, **{field: value})
    await state.clear()
    user = database.get_user(message.chat.id)
    if user:
        await message.answer("Данные обновлены.")
        await message.answer(format_profile(user), reply_markup=profile_keyboard(user["weight"], user["height"]))


@router.message(ProfileStates.nickname)
async def set_nickname(message: Message, state: FSMContext) -> None:
    nickname = message.text.strip()
    if not nickname:
        await message.answer("Нужно ввести ник.")
        return
    data = await state.get_data()
    await state.update_data(nickname=nickname)
    if data.get("mode") == "single":
        await _finish_single_field(message, "nickname", nickname, state)
        return
    await state.set_state(ProfileStates.weight)
    await message.answer("Введи вес (кг):")


@router.message(ProfileStates.weight)
async def set_weight(message: Message, state: FSMContext) -> None:
    weight = parse_int(message.text)
    if weight is None:
        await message.answer("Нужно число. Введи вес (кг):")
        return
    data = await state.get_data()
    await state.update_data(weight=weight)
    if data.get("mode") == "single":
        await _finish_single_field(message, "weight", weight, state)
        return
    await state.set_state(ProfileStates.height)
    await message.answer("Введи рост (см):")


@router.message(ProfileStates.height)
async def set_height(message: Message, state: FSMContext) -> None:
    height = parse_int(message.text)
    if height is None:
        await message.answer("Нужно число. Введи рост (см):")
        return
    data = await state.get_data()
    await state.update_data(height=height)
    if data.get("mode") == "single":
        await _finish_single_field(message, "height", height, state)
        return
    await state.set_state(ProfileStates.age)
    await message.answer("Введи возраст:")


@router.message(ProfileStates.age)
async def set_age(message: Message, state: FSMContext) -> None:
    age = parse_int(message.text)
    if age is None:
        await message.answer("Нужно число. Введи возраст:")
        return
    data = await state.get_data()
    data["age"] = age
    database.upsert_user(
        message.chat.id,
        nickname=data.get("nickname"),
        weight=data.get("weight"),
        height=data.get("height"),
        age=data.get("age"),
    )
    await state.clear()
    user = database.get_user(message.chat.id)
    if user:
        await message.answer("Профиль обновлен!", reply_markup=menu_for_user(user))
        await message.answer(format_profile(user), reply_markup=profile_keyboard(user["weight"], user["height"]))


@router.message(StateFilter("*"), F.text == "⚙️ Настройки")
async def settings_entry(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return
    await send_settings(message, user)


@router.callback_query(SettingsCallback.filter())
async def handle_settings_callback(callback: CallbackQuery, callback_data: SettingsCallback, state: FSMContext) -> None:
    user = database.get_user(callback.message.chat.id)
    if not user:
        await callback.answer("Нет профиля")
        return
    if callback_data.action == "timezone":
        await state.set_state(SettingsStates.timezone)
        await callback.message.answer("Введи таймзону, например Europe/Moscow")
    elif callback_data.action == "fixed":
        await state.set_state(SettingsStates.fixed_time)
        await callback.message.answer("Введи время HH:MM")
    elif callback_data.action == "range":
        await state.set_state(SettingsStates.range_start)
        await callback.message.answer("Введи начало диапазона HH:MM")
    elif callback_data.action == "additional":
        await state.set_state(SettingsStates.additional_count)
        await callback.message.answer("Сколько доп. задач выбирать? Введи число")
    await callback.answer()


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
    user = database.get_user(message.chat.id)
    if user:
        await send_settings(message, user)


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
    user = database.get_user(message.chat.id)
    if user:
        await send_settings(message, user)


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
    user = database.get_user(message.chat.id)
    if user:
        await send_settings(message, user)


@router.message(SettingsStates.additional_count)
async def set_additional_count(message: Message, state: FSMContext) -> None:
    count = parse_int(message.text)
    if count is None or count <= 0:
        await message.answer("Нужно положительное число.")
        return
    database.update_additional_count(message.chat.id, count)
    await state.clear()
    user = database.get_user(message.chat.id)
    if user:
        await send_settings(message, user)


@router.message(StateFilter("*"), F.text.in_({"📅 План на сегодня", "💪 Доп тренировка"}))
async def today_plan(message: Message, state: FSMContext) -> None:
    await state.clear()
    user = ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return

    today = dt.date.today()
    close_previous_day_if_pending(user["id"], today)
    existing_log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    is_additional = message.text == "💪 Доп тренировка"

    if is_additional:
        exercises = _log_exercises(existing_log, session="additional")
        if not exercises:
            extra_pool = database.get_additional_exercises(user["id"])
            if not extra_pool:
                await message.answer("Нет доп. упражнений, загрузите их через /plan.")
                return
            count = user["additional_tasks_count"] or 1
            selected = random.sample(extra_pool, k=min(count, len(extra_pool)))
            for item in selected:
                item["done"] = False
            exercises = selected
            database.update_daily_log(
                user_id=user["id"],
                date=today.isoformat(),
                exercises_done=_store_log_exercises(existing_log, "additional", exercises),
                points=existing_log.get("points") if existing_log else 0,
            )
        completed = [ex.get("done", False) for ex in exercises]
        text = "Дополнительная тренировка\n\n" + compose_workout_text(today, exercises)
        await message.answer(text, reply_markup=exercises_keyboard(exercises, completed, session="additional"))
        return

    plan = database.get_plan_for_date(user_id=user["id"], target_date=today, start_date=user["plan_start_date"])
    exercises = FALLBACK_WORKOUT if plan is None else plan[1]
    is_rest = False if plan is None else plan[0]
    if existing_log:
        stored = _log_exercises(existing_log, session="main")
        if stored:
            exercises = stored
    if existing_log and existing_log.get("points"):
        await message.answer("Тренировка за сегодня уже сохранена.", reply_markup=menu_for_user(user))
        return
    if plan is None:
        await message.answer("План недоступен, выполняем запасную тренировку.")
    if is_rest:
        await message.answer("Сегодня отдых, восстанавливай силы!")
        return

    completed = [ex.get("done", False) for ex in exercises]
    database.update_daily_log(
        user_id=user["id"],
        date=today.isoformat(),
        exercises_done=_store_log_exercises(existing_log, "main", exercises),
    )
    text = compose_workout_text(today, exercises)
    await message.answer(text, reply_markup=exercises_keyboard(exercises, completed, session="main"))


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

    exercises = _log_exercises(log, session=callback_data.session)
    completed = [item.get("done", False) for item in exercises]

    if callback_data.index == -1:
        if callback_data.session == "additional":
            await callback.message.edit_text("Доп. тренировка отменена.")
            await callback.answer()
            return
        if log.get("points"):
            await callback.answer("Тренировка уже завершена")
            return
        text = "День пропущен. Не забывай вернуться завтра!"
        database.update_daily_log(
            user_id=user["id"],
            date=today.isoformat(),
            exercises_done=_store_log_exercises(log, callback_data.session, exercises),
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
    database.update_daily_log(
        user_id=user["id"],
        date=today.isoformat(),
        exercises_done=_store_log_exercises(log, callback_data.session, exercises),
    )

    if all_done:
        points = sum(ex.get("points", 1) for ex in exercises if ex.get("done"))
        if callback_data.session == "additional":
            database.add_points(user["id"], today.isoformat(), points)
            await callback.message.edit_text(
                f"🔥 Доп. тренировка завершена!\nОчки добавлены: {points}"
            )
        else:
            database.update_daily_log(
                user_id=user["id"],
                date=today.isoformat(),
                exercises_done=_store_log_exercises(log, callback_data.session, exercises),
                difficulty_rate="completed",
                points=points,
            )
            await callback.message.edit_text(
                f"🎉 Тренировка завершена!\nОчки начислены: {points}"
            )
        await callback.message.answer("Меню обновлено.", reply_markup=menu_for_user(user))
        await callback.answer("Отлично!")
        return

    text = compose_workout_text(today, exercises)
    await callback.message.edit_text(
        text, reply_markup=exercises_keyboard(exercises, completed, session=callback_data.session)
    )
    await callback.answer("Обновлено")


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
    leaders = []
    for other in database.list_users():
        points = database.total_points(other["id"])
        win_streak = calculate_streak(other["id"])
        name = other["nickname"] or str(other["chat_id"])
        leaders.append((name, points, win_streak))
    leaders.sort(key=lambda item: item[1], reverse=True)
    if leaders:
        best_name, best_points, best_streak = leaders[0]
        worst_name, worst_points, worst_streak = leaders[-1]
        leaderboard_text = (
            "❤️🤙🎉🙏Самый крутой🙏🎉🤙❤️:\n\n"
            f"🥇{best_name} - {best_points}🍭 - {pluralize_days(best_streak)} подряд\n\n"
            "Вообще не крутой👎👎👎👎:\n\n"
            f"🗑{worst_name} - {worst_points}🍭 - {pluralize_days(worst_streak)} подряд"
        )
    else:
        leaderboard_text = "Нет данных"
    await message.answer(
        f"Очки: {total}\nСтрик: {streak} дней (рекорд {max_streak})\n"
        f"Выполнено дней: {completed_days}\nЛидерборд:\n{leaderboard_text}",
        reply_markup=menu_for_user(user),
    )


async def scheduled_push(bot: Bot, chat_id: int) -> None:
    user = database.get_user(chat_id)
    if not user:
        return
    today = dt.date.today()
    close_previous_day_if_pending(user["id"], today)
    plan = database.get_plan_for_date(user_id=user["id"], target_date=today, start_date=user["plan_start_date"])
    existing_log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    if plan is None:
        await safe_send(bot, chat_id, "План недоступен, выполняй запасную тренировку.")
        exercises = _log_exercises(existing_log) if existing_log else FALLBACK_WORKOUT
    else:
        is_rest, exercises = plan
        if existing_log:
            stored = _log_exercises(existing_log)
            if stored:
                exercises = stored
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

