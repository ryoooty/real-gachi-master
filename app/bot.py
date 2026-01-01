from __future__ import annotations

import asyncio
import datetime as dt
from typing import Any, Dict, List

from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
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
from app.time_utils import utc_now_time_str

load_dotenv()

router = Router()


class ProfileStates(StatesGroup):
    weight = State()
    height = State()
    age = State()
    level = State()
    injuries = State()


def human_day_name(date: dt.date) -> str:
    return date.strftime("%d.%m.%Y")


def weekday_key(date: dt.date) -> str:
    return date.strftime("%A").lower()


def compose_workout_text(date: dt.date, exercises: List[Dict[str, Any]]) -> str:
    lines = [f"Тренировка на {human_day_name(date)}", ""]
    for exercise in exercises:
        line = exercise["name"]
        if "reps" in exercise:
            line += f": {exercise['reps']} раз"
        if "seconds" in exercise:
            line += f": {exercise['seconds']} сек"
        if "minutes" in exercise:
            line += f": {exercise['minutes']} мин"
        lines.append(line)
    return "\n".join(lines)


async def ensure_profile(message: Message) -> database.sqlite3.Row | None:
    user = database.get_user(message.chat.id)
    if user:
        return user
    database.upsert_user(message.chat.id)
    return database.get_user(message.chat.id)


@router.message(CommandStart())
async def start(message: Message, state: FSMContext) -> None:
    database.init_db()
    database.upsert_user(message.chat.id, notify_time_utc=utc_now_time_str())
    await state.clear()
    await message.answer(
        "Привет! Я помогу планировать тренировки. Используй меню ниже, чтобы начать.",
        reply_markup=main_menu_keyboard(),
    )


@router.message(F.text == "👤 Мой Профиль")
async def edit_profile(message: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(ProfileStates.weight)
    await message.answer("Введи вес (кг):")


@router.message(ProfileStates.weight)
async def set_weight(message: Message, state: FSMContext) -> None:
    await state.update_data(weight=int(message.text))
    await state.set_state(ProfileStates.height)
    await message.answer("Введи рост (см):")


@router.message(ProfileStates.height)
async def set_height(message: Message, state: FSMContext) -> None:
    await state.update_data(height=int(message.text))
    await state.set_state(ProfileStates.age)
    await message.answer("Введи возраст:")


@router.message(ProfileStates.age)
async def set_age(message: Message, state: FSMContext) -> None:
    await state.update_data(age=int(message.text))
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
        weight=data["weight"],
        height=data["height"],
        age=data["age"],
        level=data["level"],
        injuries=data["injuries"],
    )
    await state.clear()
    await message.answer("Профиль обновлен!", reply_markup=main_menu_keyboard())


@router.message(F.text == "📅 План на сегодня")
async def today_plan(message: Message) -> None:
    user = await ensure_profile(message)
    if not user:
        await message.answer("Сначала создай профиль.")
        return

    today = dt.date.today()
    plan = database.get_plan_for_day(user["id"], weekday_key(today))
    if plan is None:
        await message.answer("План кончился! Жми кнопку генерации.")
        return
    is_rest, exercises = plan
    if is_rest:
        await message.answer("Сегодня отдых, восстанавливай силы!")
        return

    completed = [False for _ in exercises]
    database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises)
    text = compose_workout_text(today, exercises)
    await message.answer(text, reply_markup=exercises_keyboard(exercises, completed))


@router.callback_query(ExerciseCallback.filter())
async def handle_exercise_callback(callback: CallbackQuery, callback_data: ExerciseCallback) -> None:
    user = database.get_user(callback.message.chat.id)
    today = dt.date.today()
    log = database.load_daily_log(user_id=user["id"], date=today.isoformat())
    if log is None:
        await callback.answer("План не найден")
        return

    exercises = log["exercises_done"]
    completed = [item.get("done", False) for item in exercises]

    if callback_data.index == -1:
        text = "День пропущен. Не забывай вернуться завтра!"
        database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises, difficulty_rate="skipped")
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
    today = dt.date.today().isoformat()
    log = database.load_daily_log(user_id=user["id"], date=today)
    if not log:
        await callback.answer("Нет тренировки")
        return

    exercises = log["exercises_done"]
    points = sum(3 if ex.get("name", "").lower().startswith("pull") else 1 for ex in exercises if ex.get("done"))
    database.update_daily_log(user_id=user["id"], date=today, exercises_done=exercises, difficulty_rate=callback_data.rate, points=points)
    await callback.message.edit_text("Сложность сохранена, очки начислены!")
    await callback.answer("Спасибо за отзыв")


async def weekly_generation(chat_id: int) -> None:
    user = database.get_user(chat_id)
    if not user:
        return
    profile = ai.UserProfile(
        weight=user["weight"] or 80,
        height=user["height"] or 180,
        age=user["age"] or 25,
        level=user["level"] or "Новичок",
        injuries=user["injuries"] or "нет",
        completion_rate=90,
        perceived_difficulty="легко",
    )
    client = ai.DeepSeekClient()
    raw_plan = client.generate_weekly_plan(profile)
    adjusted = ai.adjust_plan(raw_plan, "easy")
    client.persist_weekly_plan(chat_id, adjusted)


async def scheduled_push(bot: Bot, chat_id: int) -> None:
    user = database.get_user(chat_id)
    if not user:
        return
    today = dt.date.today()
    plan = database.get_plan_for_day(user["id"], weekday_key(today))
    if plan is None:
        await bot.send_message(chat_id, "План кончился! Жми кнопку генерации.")
        return
    is_rest, exercises = plan
    if is_rest:
        await bot.send_message(chat_id, "Сегодня отдых, восстанавливай силы!")
        return
    completed = [False for _ in exercises]
    database.update_daily_log(user_id=user["id"], date=today.isoformat(), exercises_done=exercises)
    text = compose_workout_text(today, exercises)
    await bot.send_message(chat_id, text, reply_markup=exercises_keyboard(exercises, completed))


async def main() -> None:
    database.init_db()
    bot = Bot(token="DUMMY", parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    dp.include_router(router)

    scheduler = WorkoutScheduler(lambda chat_id: scheduled_push(bot, chat_id))
    scheduler.schedule_user(chat_id=1, local_time="09:00", timezone="Europe/Moscow")
    scheduler.start()

    await weekly_generation(chat_id=1)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
