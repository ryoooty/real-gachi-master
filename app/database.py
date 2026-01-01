from __future__ import annotations

import json
import sqlite3
import datetime as dt
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DB_PATH = Path(__file__).resolve().parent / "bot.db"


@contextmanager
def get_conn() -> Iterable[sqlite3.Connection]:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.row_factory = sqlite3.Row
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(%s)" % table)
    columns = {row[1] for row in cursor.fetchall()}
    if column not in columns:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE,
                nickname TEXT,
                notify_time_utc TEXT,
                notify_time_utc_iso TEXT,
                notify_range_start_utc TEXT,
                notify_range_end_utc TEXT,
                notify_range_start_utc_iso TEXT,
                notify_range_end_utc_iso TEXT,
                notify_mode TEXT DEFAULT 'fixed',
                timezone TEXT DEFAULT 'Europe/Moscow',
                weight INTEGER,
                height INTEGER,
                age INTEGER,
                level TEXT,
                injuries TEXT,
                plan_start_date TEXT,
                additional_tasks_count INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                day_index INTEGER NOT NULL,
                title TEXT,
                exercise_list TEXT NOT NULL,
                is_rest_day INTEGER DEFAULT 0,
                UNIQUE(user_id, day_index)
            );
            """,
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                exercises_done TEXT,
                difficulty_rate TEXT,
                points INTEGER DEFAULT 0,
                UNIQUE(user_id, date)
            );
            """,
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS additional_exercises (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                exercise_list TEXT NOT NULL
            );
            """,
        )
        # migrations for existing databases
        _ensure_column(conn, "users", "notify_range_start_utc", "TEXT")
        _ensure_column(conn, "users", "notify_range_end_utc", "TEXT")
        _ensure_column(conn, "users", "notify_mode", "TEXT DEFAULT 'fixed'")
        _ensure_column(conn, "users", "notify_time_utc_iso", "TEXT")
        _ensure_column(conn, "users", "notify_range_start_utc_iso", "TEXT")
        _ensure_column(conn, "users", "notify_range_end_utc_iso", "TEXT")
        _ensure_column(conn, "users", "nickname", "TEXT")
        _ensure_column(conn, "users", "plan_start_date", "TEXT")
        _ensure_column(conn, "users", "additional_tasks_count", "INTEGER DEFAULT 1")
        _ensure_column(conn, "weekly_plan", "title", "TEXT")
        _ensure_column(conn, "weekly_plan", "day_index", "INTEGER")


def upsert_user(chat_id: int, **kwargs: Any) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (chat_id) VALUES (?) ON CONFLICT(chat_id) DO NOTHING",
            (chat_id,),
        )
        if kwargs:
            keys = ", ".join(f"{k} = ?" for k in kwargs)
            values = list(kwargs.values())
            values.append(chat_id)
            cursor.execute(f"UPDATE users SET {keys} WHERE chat_id = ?", values)


def update_additional_count(chat_id: int, count: int) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE users SET additional_tasks_count = ? WHERE chat_id = ?",
            (count, chat_id),
        )


def get_user(chat_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
        return cursor.fetchone()


def list_users() -> List[sqlite3.Row]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM users ORDER BY id ASC")
        return cursor.fetchall()


def get_user_count() -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM users")
        return int(cursor.fetchone()[0])


def save_weekly_plan(user_id: int, plan: Dict[str, Any]) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        for day, exercises in plan.items():
            is_rest = 1 if isinstance(exercises, str) and exercises.upper() == "REST" else 0
            payload = exercises if is_rest else exercises
            cursor.execute(
                """
                INSERT INTO weekly_plan (user_id, day_index, exercise_list, is_rest_day)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, day_index) DO UPDATE SET
                    exercise_list = excluded.exercise_list,
                    is_rest_day = excluded.is_rest_day
                """,
                (user_id, day, json.dumps(payload), is_rest),
            )


def replace_plan(user_id: int, plan: List[Dict[str, Any]], start_date: dt.date) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM weekly_plan WHERE user_id = ?", (user_id,))
        for item in plan:
            cursor.execute(
                """
                INSERT INTO weekly_plan (user_id, day_index, title, exercise_list, is_rest_day)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    item["day_index"],
                    item.get("title"),
                    json.dumps(item.get("exercises", [])),
                    1 if item.get("is_rest") else 0,
                ),
            )
        cursor.execute(
            "UPDATE users SET plan_start_date = ? WHERE id = ?",
            (start_date.isoformat(), user_id),
        )


def save_additional_exercises(user_id: int, exercises: List[Dict[str, Any]]) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO additional_exercises (user_id, exercise_list)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET exercise_list = excluded.exercise_list
            """,
            (user_id, json.dumps(exercises)),
        )


def plan_length(user_id: int) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM weekly_plan WHERE user_id = ?", (user_id,))
        res = cursor.fetchone()[0]
        return int(res or 0)


def get_plan_day(user_id: int, day_index: int) -> Optional[Tuple[bool, List[Dict[str, Any]], Optional[str]]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT exercise_list, is_rest_day, title FROM weekly_plan WHERE user_id = ? AND day_index = ?",
            (user_id, day_index),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        if row[1]:
            return True, [], row[2]
        return False, json.loads(row[0]), row[2]


def get_plan_for_date(user_id: int, target_date: dt.date, start_date: Optional[str]) -> Optional[Tuple[bool, List[Dict[str, Any]], Optional[str]]]:
    total_days = plan_length(user_id)
    if total_days == 0:
        return None
    if start_date:
        start_dt = dt.date.fromisoformat(start_date)
    else:
        start_dt = target_date
    delta = (target_date - start_dt).days
    index = (delta % total_days) + 1
    return get_plan_day(user_id, index)


def get_additional_exercises(user_id: int) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT exercise_list FROM additional_exercises WHERE user_id = ?", (user_id,)
        )
        row = cursor.fetchone()
        if not row:
            return []
        return json.loads(row[0])


def update_daily_log(
    user_id: int,
    date: str,
    exercises_done: List[Dict[str, Any]],
    difficulty_rate: Optional[str] = None,
    points: Optional[int] = None,
) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO daily_logs (user_id, date, exercises_done, difficulty_rate, points)
            VALUES (?, ?, ?, ?, COALESCE(?, 0))
            ON CONFLICT(user_id, date) DO UPDATE SET
                exercises_done = excluded.exercises_done,
                difficulty_rate = COALESCE(excluded.difficulty_rate, daily_logs.difficulty_rate),
                points = COALESCE(excluded.points, daily_logs.points)
            """,
            (user_id, date, json.dumps(exercises_done), difficulty_rate, points),
        )


def load_daily_log(user_id: int, date: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT exercises_done, difficulty_rate, points FROM daily_logs WHERE user_id = ? AND date = ?",
            (user_id, date),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "exercises_done": json.loads(row[0]) if row[0] else [],
            "difficulty_rate": row[1],
            "points": row[2],
        }


def add_points(user_id: int, date: str, points: int) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE daily_logs SET points = COALESCE(points, 0) + ? WHERE user_id = ? AND date = ?",
            (points, user_id, date),
        )


def total_points(user_id: int) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(points) FROM daily_logs WHERE user_id = ?", (user_id,))
        res = cursor.fetchone()[0]
        return int(res or 0)


def completed_days(user_id: int) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM daily_logs WHERE user_id = ? AND COALESCE(points,0) > 0",
            (user_id,),
        )
        res = cursor.fetchone()[0]
        return int(res or 0)


def max_streak(user_id: int) -> int:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT date FROM daily_logs WHERE user_id = ? AND COALESCE(points,0) > 0 ORDER BY date DESC",
            (user_id,),
        )
        rows = [dt for (dt,) in cursor.fetchall()]
    best = 0
    current = 0
    last_date = None
    for iso in rows:
        current_date = dt.datetime.fromisoformat(iso).date()
        if last_date is None or last_date - dt.timedelta(days=1) == current_date:
            current += 1
        elif last_date == current_date:
            continue
        else:
            current = 1
        best = max(best, current)
        last_date = current_date
    return best


def completion_dates(user_id: int) -> List[str]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT date, points FROM daily_logs WHERE user_id = ? AND COALESCE(points,0) > 0 ORDER BY date DESC",
            (user_id,),
        )
        return [row[0] for row in cursor.fetchall()]


def leaderboard() -> List[tuple[int, int, str | None]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT chat_id, COALESCE(SUM(points),0) as pts, users.nickname
            FROM daily_logs
            JOIN users ON users.id = daily_logs.user_id
            GROUP BY chat_id
            ORDER BY pts DESC
            """
        )
        return [(row[0], row[1], row[2]) for row in cursor.fetchall()]

