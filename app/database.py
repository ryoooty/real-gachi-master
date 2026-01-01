from __future__ import annotations

import json
import sqlite3
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
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER UNIQUE,
                notify_time_utc TEXT,
                notify_range_start_utc TEXT,
                notify_range_end_utc TEXT,
                notify_mode TEXT DEFAULT 'fixed',
                timezone TEXT DEFAULT 'Europe/Moscow',
                weight INTEGER,
                height INTEGER,
                age INTEGER,
                level TEXT,
                injuries TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            """,
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weekly_plan (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                day_of_week TEXT NOT NULL,
                exercise_list TEXT NOT NULL,
                is_rest_day INTEGER DEFAULT 0,
                UNIQUE(user_id, day_of_week)
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
        # migrations for existing databases
        _ensure_column(conn, "users", "notify_range_start_utc", "TEXT")
        _ensure_column(conn, "users", "notify_range_end_utc", "TEXT")
        _ensure_column(conn, "users", "notify_mode", "TEXT DEFAULT 'fixed'")


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
                INSERT INTO weekly_plan (user_id, day_of_week, exercise_list, is_rest_day)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, day_of_week) DO UPDATE SET
                    exercise_list = excluded.exercise_list,
                    is_rest_day = excluded.is_rest_day
                """,
                (user_id, day, json.dumps(payload), is_rest),
            )


def get_plan_for_day(user_id: int, day_of_week: str) -> Optional[Tuple[bool, List[Dict[str, Any]]]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT exercise_list, is_rest_day FROM weekly_plan WHERE user_id = ? AND day_of_week = ?",
            (user_id, day_of_week),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        if row[1]:
            return True, []
        return False, json.loads(row[0])


def update_daily_log(
    user_id: int,
    date: str,
    exercises_done: List[Dict[str, Any]],
    difficulty_rate: Optional[str] = None,
    points: int = 0,
) -> None:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO daily_logs (user_id, date, exercises_done, difficulty_rate, points)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id, date) DO UPDATE SET
                exercises_done = excluded.exercises_done,
                difficulty_rate = COALESCE(excluded.difficulty_rate, daily_logs.difficulty_rate),
                points = excluded.points
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


def completion_dates(user_id: int) -> List[str]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT date, points FROM daily_logs WHERE user_id = ? AND COALESCE(points,0) > 0 ORDER BY date DESC",
            (user_id,),
        )
        return [row[0] for row in cursor.fetchall()]


def leaderboard() -> List[tuple[int, int]]:
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT chat_id, COALESCE(SUM(points),0) as pts
            FROM daily_logs
            JOIN users ON users.id = daily_logs.user_id
            GROUP BY chat_id
            ORDER BY pts DESC
            """
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]

