from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List

from pydantic import BaseModel, Field

from app import database


class WeeklyPlan(BaseModel):
    days: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_raw(cls, payload: str | Dict[str, Any]) -> "WeeklyPlan":
        data = json.loads(payload) if isinstance(payload, str) else payload
        return cls(days=data)

    def to_db_payload(self) -> Dict[str, Any]:
        return self.days


@dataclass
class UserProfile:
    weight: int
    height: int
    age: int
    level: str
    injuries: str
    completion_rate: int
    perceived_difficulty: str

    def as_prompt(self) -> str:
        return (
            f"Юзер весит {self.weight}кг, рост {self.height}см, возраст {self.age}. "
            f"Уровень: {self.level}. Ограничения: {self.injuries or 'нет'}. "
            f"Прошлую неделю закрыл на {self.completion_rate}%. Было {self.perceived_difficulty}."
        )


class DeepSeekClient:
    def __init__(self, model_name: str = "deepseek-chat"):
        self.model_name = model_name

    def build_prompt(self, profile: UserProfile) -> str:
        preface = profile.as_prompt()
        schema = (
            "Составь план на следующие 7 дней (Пн-Вс). Включи 2 дня отдыха. Формат JSON: "
            '{"monday": [{"name": "Pushups", "reps": 20}], "tuesday": "REST", ...}'
        )
        return f"{preface} {schema}"

    def generate_weekly_plan(self, profile: UserProfile) -> WeeklyPlan:
        """Stub for DeepSeek integration.

        In production this should call the API. Here we return a deterministic
        payload to keep the repository runnable without credentials.
        """
        plan = {
            "monday": [{"name": "Pushups", "reps": 20}, {"name": "Squats", "reps": 30}],
            "tuesday": [{"name": "Plank", "seconds": 60}],
            "wednesday": "REST",
            "thursday": [{"name": "Lunges", "reps": 15}],
            "friday": [{"name": "Pullups", "reps": 5}],
            "saturday": "REST",
            "sunday": [{"name": "Stretching", "minutes": 10}],
        }
        return WeeklyPlan.from_raw(plan)

    def persist_weekly_plan(self, chat_id: int, plan: WeeklyPlan) -> None:
        user = database.get_user(chat_id)
        if not user:
            raise RuntimeError("User must exist before saving plan")
        database.save_weekly_plan(user_id=user["id"], plan=plan.to_db_payload())


def adjust_plan(plan: WeeklyPlan, difficulty_feedback: str) -> WeeklyPlan:
    if difficulty_feedback.lower() != "easy":
        return plan

    boosted: Dict[str, List[Dict[str, Any]]] = {}
    for day, exercises in plan.days.items():
        if isinstance(exercises, str):
            boosted[day] = exercises
            continue
        adjusted: List[Dict[str, Any]] = []
        for exercise in exercises:
            updated = {}
            for key, value in exercise.items():
                if isinstance(value, (int, float)):
                    updated[key] = int(value * 1.05)
                else:
                    updated[key] = value
            adjusted.append(updated)
        boosted[day] = adjusted
    return WeeklyPlan.from_raw(boosted)
