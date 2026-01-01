from __future__ import annotations

from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


class ExerciseCallback(CallbackData, prefix="ex"):
    session: str
    index: int
    completed: bool


class SettingsCallback(CallbackData, prefix="st"):
    action: str


class ProfileCallback(CallbackData, prefix="pf"):
    action: str


def main_menu_keyboard(plan_label: str = "📅 План на сегодня") -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=plan_label)],
            [KeyboardButton(text="📈 Статистика")],
            [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
    )


def exercises_keyboard(
    exercises: list[dict[str, str | int]], completed: list[bool], session: str = "main"
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for idx, exercise in enumerate(exercises):
        status = "✅" if completed[idx] else "[ ]"
        label = f"{status} {exercise['name']}"
        builder.button(
            text=label,
            callback_data=ExerciseCallback(session=session, index=idx, completed=not completed[idx]),
        )
    builder.button(
        text="🚫 Пропустить день", callback_data=ExerciseCallback(session=session, index=-1, completed=False)
    )
    builder.adjust(1)
    return builder.as_markup()


def settings_keyboard(mode: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(
        text=("✅ ⏰ Точное время" if mode == "fixed" else "⏰ Точное время"),
        callback_data=SettingsCallback(action="fixed"),
    )
    builder.button(
        text=("✅ 🔁 Диапазон" if mode == "range" else "🔁 Диапазон"),
        callback_data=SettingsCallback(action="range"),
    )
    builder.button(text="🌐 Часовой пояс", callback_data=SettingsCallback(action="timezone"))
    builder.button(text="➕ Доп. задачи", callback_data=SettingsCallback(action="additional"))
    builder.adjust(2, 1, 1)
    return builder.as_markup()


def profile_keyboard(weight: int | None, height: int | None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✏️ Изменить профиль", callback_data=ProfileCallback(action="all"))
    weight_label = f"{weight} кг" if weight is not None else "Вес?"
    height_label = f"{height} см" if height is not None else "Рост?"
    builder.button(text=weight_label, callback_data=ProfileCallback(action="weight"))
    builder.button(text=height_label, callback_data=ProfileCallback(action="height"))
    builder.button(text="Ник", callback_data=ProfileCallback(action="nickname"))
    builder.adjust(1, 2, 1)
    return builder.as_markup()
