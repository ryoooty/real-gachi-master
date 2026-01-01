from __future__ import annotations

from aiogram.types import (CallbackQuery, InlineKeyboardMarkup, KeyboardButton,
                           ReplyKeyboardMarkup)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.callback_data import CallbackData


class ExerciseCallback(CallbackData, prefix="ex"):
    index: int
    completed: bool


class DifficultyCallback(CallbackData, prefix="df"):
    rate: str


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📅 План на сегодня"), KeyboardButton(text="📈 Статистика")],
            [KeyboardButton(text="👤 Мой Профиль"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def exercises_keyboard(exercises: list[dict[str, str | int]], completed: list[bool]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for idx, exercise in enumerate(exercises):
        status = "✅" if completed[idx] else "[ ]"
        label = f"{status} {exercise['name']}"
        builder.button(text=label, callback_data=ExerciseCallback(index=idx, completed=not completed[idx]))
    builder.button(text="🚫 Пропустить день", callback_data=ExerciseCallback(index=-1, completed=False))
    builder.adjust(1)
    return builder.as_markup()


def difficulty_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🟢 Легко", callback_data=DifficultyCallback(rate="easy"))
    builder.button(text="🟡 Норм", callback_data=DifficultyCallback(rate="normal"))
    builder.button(text="🔴 Тяжело", callback_data=DifficultyCallback(rate="hard"))
    builder.adjust(3)
    return builder.as_markup()
