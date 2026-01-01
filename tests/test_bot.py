import datetime as dt
import sys
import tempfile
import types
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))


def _stub_aiogram_modules() -> None:
    aiogram = types.ModuleType("aiogram")
    aiogram.Bot = type("Bot", (), {})

    class _Dispatcher:
        def include_router(self, *args, **kwargs):
            return None

    aiogram.Dispatcher = _Dispatcher

    class _Field:
        def __init__(self, name: str):
            self.name = name

        def in_(self, *args, **kwargs):
            return self

        def __eq__(self, other):
            return self

    aiogram.F = types.SimpleNamespace(text=_Field("text"))

    class _Router:
        def message(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def callback_query(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    aiogram.Router = _Router
    aiogram.BaseMiddleware = type("BaseMiddleware", (), {})

    client_mod = types.ModuleType("aiogram.client")
    default_mod = types.ModuleType("aiogram.client.default")
    default_mod.DefaultBotProperties = type("DefaultBotProperties", (), {})
    client_mod.default = default_mod

    enums_mod = types.ModuleType("aiogram.enums")
    enums_mod.ParseMode = type("ParseMode", (), {"HTML": "HTML"})

    exceptions_mod = types.ModuleType("aiogram.exceptions")
    exceptions_mod.TelegramForbiddenError = type("TelegramForbiddenError", (Exception,), {})

    filters_mod = types.ModuleType("aiogram.filters")
    filters_mod.__path__ = []  # allow submodules
    class _Command:
        def __init__(self, *args, **kwargs):
            pass

    filters_mod.Command = _Command

    class _CommandStart:
        def __init__(self, *args, **kwargs):
            pass

    filters_mod.CommandStart = _CommandStart

    filters_mod.StateFilter = type("StateFilter", (), {"__init__": lambda self, *args, **kwargs: None})
    callback_data_mod = types.ModuleType("aiogram.filters.callback_data")

    class _CallbackData:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__()

        @classmethod
        def filter(cls):
            def wrapper(*args, **kwargs):
                return None

            return wrapper

    callback_data_mod.CallbackData = _CallbackData

    fsm_mod = types.ModuleType("aiogram.fsm")
    context_mod = types.ModuleType("aiogram.fsm.context")
    context_mod.FSMContext = type("FSMContext", (), {})
    state_mod = types.ModuleType("aiogram.fsm.state")
    state_mod.State = type("State", (), {})
    state_mod.StatesGroup = type("StatesGroup", (), {})
    fsm_mod.context = context_mod
    fsm_mod.state = state_mod

    types_mod = types.ModuleType("aiogram.types")
    types_mod.CallbackQuery = type("CallbackQuery", (), {})
    types_mod.Message = type("Message", (), {})
    types_mod.TelegramObject = type("TelegramObject", (), {})
    types_mod.ReplyKeyboardMarkup = type("ReplyKeyboardMarkup", (), {})
    types_mod.KeyboardButton = type("KeyboardButton", (), {})
    types_mod.InlineKeyboardMarkup = type("InlineKeyboardMarkup", (), {})

    sys.modules["aiogram"] = aiogram
    sys.modules["aiogram.client"] = client_mod
    sys.modules["aiogram.client.default"] = default_mod
    sys.modules["aiogram.enums"] = enums_mod
    sys.modules["aiogram.exceptions"] = exceptions_mod
    sys.modules["aiogram.filters"] = filters_mod
    sys.modules["aiogram.filters.callback_data"] = callback_data_mod
    sys.modules["aiogram.fsm"] = fsm_mod
    sys.modules["aiogram.fsm.context"] = context_mod
    sys.modules["aiogram.fsm.state"] = state_mod
    sys.modules["aiogram.types"] = types_mod

    utils_mod = types.ModuleType("aiogram.utils")
    utils_mod.__path__ = []
    keyboard_mod = types.ModuleType("aiogram.utils.keyboard")

    class _InlineKeyboardBuilder:
        def button(self, *args, **kwargs):
            return None

        def adjust(self, *args, **kwargs):
            return None

        def as_markup(self, *args, **kwargs):
            return None

    keyboard_mod.InlineKeyboardBuilder = _InlineKeyboardBuilder
    utils_mod.keyboard = keyboard_mod
    sys.modules["aiogram.utils"] = utils_mod
    sys.modules["aiogram.utils.keyboard"] = keyboard_mod


def _stub_other_modules() -> None:
    pytz_mod = types.ModuleType("pytz")
    pytz_mod.timezone = lambda name=None: dt.timezone.utc
    sys.modules["pytz"] = pytz_mod

    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None
    sys.modules["dotenv"] = dotenv_mod

    apscheduler_mod = types.ModuleType("apscheduler")
    schedulers_mod = types.ModuleType("apscheduler.schedulers")
    schedulers_mod.__path__ = []
    asyncio_mod = types.ModuleType("apscheduler.schedulers.asyncio")

    class _AsyncIOScheduler:
        def __init__(self, *args, **kwargs):
            self.running = False

        def add_job(self, *args, **kwargs):
            return None

        def start(self):
            self.running = True

        def shutdown(self):
            self.running = False

    asyncio_mod.AsyncIOScheduler = _AsyncIOScheduler
    triggers_mod = types.ModuleType("apscheduler.triggers")
    triggers_mod.__path__ = []
    cron_mod = types.ModuleType("apscheduler.triggers.cron")
    cron_mod.CronTrigger = type("CronTrigger", (), {})
    date_mod = types.ModuleType("apscheduler.triggers.date")
    date_mod.DateTrigger = type("DateTrigger", (), {})

    sys.modules["apscheduler"] = apscheduler_mod
    sys.modules["apscheduler.schedulers"] = schedulers_mod
    sys.modules["apscheduler.schedulers.asyncio"] = asyncio_mod
    sys.modules["apscheduler.triggers"] = triggers_mod
    sys.modules["apscheduler.triggers.cron"] = cron_mod
    sys.modules["apscheduler.triggers.date"] = date_mod


_stub_aiogram_modules()
_stub_other_modules()

from app import database
from app.bot import _store_log_exercises, scheduled_push


class ScheduledPushTest(IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        database.DB_PATH = Path(self.tempdir.name) / "bot.db"
        database.init_db()
        database.upsert_user(123, nickname="Tester")

    def tearDown(self):
        self.tempdir.cleanup()

    async def test_scheduled_push_preserves_additional_sessions(self):
        user = database.get_user(123)
        today = dt.date.today().isoformat()

        main_exercises = [{"name": "Push Ups", "done": False}]
        additional_exercises = [{"name": "Plank", "done": False}]

        exercises_done = _store_log_exercises({"exercises_done": {}}, "main", main_exercises)
        exercises_done = _store_log_exercises({"exercises_done": exercises_done}, "additional", additional_exercises)

        database.update_daily_log(
            user_id=user["id"],
            date=today,
            exercises_done=exercises_done,
            points=0,
        )

        bot = AsyncMock()

        await scheduled_push(bot, chat_id=user["chat_id"])

        updated_log = database.load_daily_log(user_id=user["id"], date=today)

        self.assertIn("additional", updated_log["exercises_done"])
        self.assertEqual(updated_log["exercises_done"]["additional"], additional_exercises)

        bot.send_message.assert_awaited()
