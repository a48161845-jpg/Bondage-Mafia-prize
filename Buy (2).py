import asyncio
import os
import asyncpg
import string
import random
from datetime import datetime, timedelta, timezone
import re
import logging
import aiogram
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# Логирование. По умолчанию — INFO (тихо), подробный DEBUG только если явно
# задать переменную окружения BOT_LOG_LEVEL=DEBUG. Шумные сторонние библиотеки
# (aiohttp/asyncio, которые аиограм гоняет на long polling) всегда прижаты к
# WARNING — раньше именно из-за них при DEBUG консоль заливало служебным мусором.
LOG_LEVEL = os.getenv("BOT_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
for noisy_logger in ("aiohttp", "aiohttp.client", "aiohttp.access", "asyncio"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)
logger.info(f"Версия aiogram: {aiogram.__version__}, уровень логов: {LOG_LEVEL}")  # требуется aiogram>=3.15 (pip install -U aiogram)

# Настройки бота — токен и параметры из переменных окружения, а не в коде
API_TOKEN = os.getenv("BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("Не задана переменная окружения BOT_TOKEN с токеном бота.")

_admin_ids_raw = os.getenv("ADMIN_IDS", "")
ADMIN_IDS = [int(x) for x in _admin_ids_raw.split(",") if x.strip()]
if not ADMIN_IDS:
    raise RuntimeError("Не задана переменная окружения ADMIN_IDS (ID суперадминов через запятую).")

_group_chat_id_raw = os.getenv("GROUP_CHAT_ID")
if not _group_chat_id_raw:
    raise RuntimeError("Не задана переменная окружения GROUP_CHAT_ID.")
GROUP_CHAT_ID = int(_group_chat_id_raw)

BROADCAST_TOPIC_NAME = "Рассылка"
PROMOCODES_TOPIC_NAME = "Промокоды"

# Москва — фиксированный UTC+3 без перехода на летнее время, поэтому обычного
# timezone(timedelta) достаточно, без зависимостей на базу часовых поясов.
MSK = timezone(timedelta(hours=3), name="MSK")

bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# =====================================================================
# PostgreSQL: пул соединений + тонкая cursor-совместимая обёртка.
#
# Весь остальной код бота написан в стиле sqlite3/aiosqlite
# (conn = await db_connect(); c = await conn.cursor(); await c.execute(...);
# await c.fetchone()/.fetchall(); await conn.commit(); await conn.close()),
# поэтому вместо переписывания каждого запроса вручную под asyncpg — один
# слой совместимости: он сам переводит плейсхолдеры '?' в '$1, $2, ...' и
# берёт соединение из пула вместо локального файла SQLite.
# =====================================================================
_pg_pool: asyncpg.Pool | None = None

async def init_db_pool():
    """Открывает пул соединений с PostgreSQL. Адрес — из DATABASE_URL
    (стандартная переменная окружения у большинства хостингов баз данных:
    Render, Railway, Supabase, Neon и т.п.), а не локальный файл."""
    global _pg_pool
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Не задана переменная окружения DATABASE_URL с адресом PostgreSQL.")
    _pg_pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=10)
    logger.info("Подключение к PostgreSQL установлено")

async def close_db_pool():
    global _pg_pool
    if _pg_pool:
        await _pg_pool.close()
        logger.info("Пул соединений с PostgreSQL закрыт")

class _PGCursor:
    """Курсор-совместимая обёртка: execute/fetchone/fetchall/lastrowid/rowcount
    ведут себя так же, как у sqlite3/aiosqlite, но работают через asyncpg."""

    def __init__(self, raw_conn):
        self._conn = raw_conn
        self.lastrowid = None
        self.rowcount = 0
        self._rows: list = []
        self._pos = 0

    @staticmethod
    def _to_pg(query: str) -> str:
        out = []
        n = 0
        for ch in query:
            if ch == '?':
                n += 1
                out.append(f"${n}")
            else:
                out.append(ch)
        return "".join(out)

    async def execute(self, query: str, params=()):
        pg_query = self._to_pg(query)
        stripped = query.strip().upper()
        if stripped.startswith(("SELECT", "PRAGMA", "WITH")):
            self._rows = await self._conn.fetch(pg_query, *params)
            self._pos = 0
            self.rowcount = len(self._rows)
        else:
            status = await self._conn.execute(pg_query, *params)
            self._rows = []
            self._pos = 0
            try:
                self.rowcount = int(status.split()[-1])
            except (ValueError, IndexError):
                self.rowcount = 0

    async def fetchone(self):
        if self._pos < len(self._rows):
            row = self._rows[self._pos]
            self._pos += 1
            return tuple(row)
        return None

    async def fetchall(self):
        rows = [tuple(r) for r in self._rows[self._pos:]]
        self._pos = len(self._rows)
        return rows

class _PGConnection:
    """Connection-совместимая обёртка поверх соединения из пула asyncpg."""

    def __init__(self, raw_conn, pool: asyncpg.Pool):
        self._raw = raw_conn
        self._pool = pool

    async def cursor(self) -> _PGCursor:
        return _PGCursor(self._raw)

    async def commit(self):
        pass  # asyncpg сам фиксирует каждый запрос вне явной транзакции

    async def close(self):
        await self._pool.release(self._raw)

    async def fetchval(self, query: str, params=()):
        """Для случаев вроде INSERT ... RETURNING id — заменяет cursor.lastrowid у SQLite."""
        return await self._raw.fetchval(_PGCursor._to_pg(query), *params)

async def db_connect() -> _PGConnection:
    """Совместимая замена aiosqlite.connect('promocodes.db') — берёт соединение
    из пула PostgreSQL вместо открытия локального файла."""
    raw = await _pg_pool.acquire()
    return _PGConnection(raw, _pg_pool)

async def init_db():
    conn = await db_connect()
    c = await conn.cursor()

    # Создание таблицы promocodes
    await c.execute('''CREATE TABLE IF NOT EXISTS promocodes
                 (code TEXT PRIMARY KEY, type INTEGER, active BOOLEAN, activation_deadline TEXT, 
                  reward_duration INTEGER, activations_limit INTEGER, activations_used INTEGER,
                  currency_choice TEXT)''')

    # Создание таблицы user_promocodes (user_id — BIGINT: ID пользователей Telegram
    # давно переросли диапазон обычного 32-битного INTEGER)
    await c.execute('''CREATE TABLE IF NOT EXISTS user_promocodes
                 (user_id BIGINT, username TEXT, code TEXT, activation_date TEXT,
                  PRIMARY KEY (user_id, code))''')

    # Создание таблицы users
    await c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT,
                  language_code TEXT, is_premium BOOLEAN, thread_id INTEGER, last_updated TEXT)''')

    # Создание таблицы заявок на приз (для карточек с кнопками Выдано/Отказано и "Мои заявки")
    await c.execute('''CREATE TABLE IF NOT EXISTS prize_claims
                 (id SERIAL PRIMARY KEY, user_id BIGINT, username TEXT,
                  choice INTEGER, detail TEXT, link TEXT, status TEXT DEFAULT 'pending',
                  created_at TEXT, resolved_at TEXT, resolved_by BIGINT, card_message_id INTEGER)''')

    # Довешивание новых столбцов на уже существующих базах. В PostgreSQL (в отличие
    # от SQLite) ALTER TABLE ... ADD COLUMN IF NOT EXISTS работает сам по себе —
    # не нужно вручную проверять текущий список столбцов через PRAGMA.
    await c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS admin_level INTEGER DEFAULT 0")
    await c.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS topic_msg_id INTEGER")
    await c.execute("ALTER TABLE promocodes ADD COLUMN IF NOT EXISTS currency_choice TEXT")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS promotype INTEGER")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS selection TEXT")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS reward_duration INTEGER")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS resolved_at TEXT")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS resolved_by BIGINT")
    await c.execute("ALTER TABLE user_promocodes ADD COLUMN IF NOT EXISTS card_message_id INTEGER")

    await conn.commit()
    await conn.close()

def escape_markdown_v2(text: str, exclude: str = '') -> str:
    """Экранирует все спецсимволы MarkdownV2."""
    reserved_chars = r"_*[]()~`>#+-=|{}.!"
    if exclude:
        for char in exclude:
            reserved_chars = reserved_chars.replace(char, '')
    pattern = f"[{re.escape(reserved_chars)}]"
    return re.sub(pattern, lambda m: '\\' + m.group(0), text)

# Парсер времени действия
def parse_duration(duration: str, is_reward: bool = False) -> int | str | None:
    if duration.lower() == 'без срока':
        return None if not is_reward else 0
    match = re.match(r'^(\d+)([ymdh])$', duration.lower())
    if not match:
        return False
    value, unit = int(match.group(1)), match.group(2)
    now = datetime.now(MSK)
    if is_reward:
        if unit == 'y':
            return value * 365 * 24  # Годы в часы
        elif unit == 'd':
            return value * 24  # Дни в часы
        elif unit == 'h':
            return value  # Часы
        elif unit == 'm':
            return value // 60  # Минуты в часы
    else:
        if unit == 'y':
            expiry = now + timedelta(days=value * 365)
        elif unit == 'd':
            expiry = now + timedelta(days=value)
        elif unit == 'h':
            expiry = now + timedelta(hours=value)
        elif unit == 'm':
            expiry = now + timedelta(minutes=value)
        return expiry.isoformat()

# Добавьте после определения ADMIN_IDS:
async def check_admin_level(user_id: int, required_level: int) -> bool:
    if user_id in ADMIN_IDS:
        return True  # Уровень 4 (ADMIN_IDS) имеет полный доступ
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT admin_level FROM users WHERE user_id = ?", (user_id,))
    result = await c.fetchone()
    await conn.close()
    return result and result[0] >= required_level

async def safe_callback_answer(callback: CallbackQuery):
    """Отвечает на callback-запрос, не роняя хендлер, если запрос уже устарел."""
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {callback.from_user.id}: {e}")

# Форматирование времени для вывода
def format_datetime(iso_str: str | None) -> str:
    logger.debug(f"Обработка даты: {iso_str}")
    if iso_str is None or iso_str == "Без срока":
        return "Без срока"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        if dt.tzinfo is not None:
            dt = dt.astimezone(MSK)  # на случай старых записей в другом часовом поясе
        return dt.strftime("%Y-%m-%d %H:%M:%S МСК")  # НЕ экранируем!
    except ValueError as e:
        logger.error(f"Неверный формат даты: {iso_str}, ошибка: {e}")
        return "Ошибка даты"

def generate_promocode(promotype: int) -> str:
    characters = string.ascii_uppercase + string.digits
    code = f"{promotype}" + ''.join(random.choice(characters) for _ in range(14))
    return code

def build_main_menu(first_name: str) -> tuple[str, InlineKeyboardMarkup]:
    """Единое приветствие + клавиатура главного меню (используется в /start,
    возврате «Назад» и после ошибок), чтобы не дублировать текст в трёх местах."""
    text = (
        f"Привет, `{escape_markdown_v2(first_name)}`\\! 👋🏻\n"  # Имя моноширинное
        f"Это бот\\-помощник для выдачи наград в чате 𝐁𝐨𝐧𝐝𝐚𝐠𝐞 𝐌𝐚𝐟𝐢𝐚 🖤\n"
        f"__Для дальнейшего взаимодействия выбери удобный пункт ниже:__"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Забрать приз", callback_data="get_prize")],
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promocode")],
        [InlineKeyboardButton(text="📜 Мои заявки", callback_data="my_claims")]
    ])
    return text, keyboard

def build_claim_card_text(user_id: int, username: str | None, reward_type: int, selection: str | None, link: str | None, created_at: str) -> str:
    """Собирает текст карточки заявки: @username(id), Тип (Антитаргет/Префикс/Валюта),
    Выбор (если есть — введённый префикс моно, выбранная валюта обычным текстом), ссылка, дата и время."""
    when = escape_markdown_v2(format_datetime(created_at))
    link_text = escape_markdown_v2(link) if link else escape_markdown_v2("не указана")
    lines = [
        f"@{escape_markdown_v2(username or 'без_юзернейма')}\\({user_id}\\)",
        f"Тип: *{escape_markdown_v2(promotype_label(reward_type))}*",
    ]
    if selection:
        if reward_type == 2:  # Префикс — копируемый пользовательский текст
            lines.append(f"Выбор: `{escape_markdown_v2(selection)}`")
        else:
            lines.append(f"Выбор: {escape_markdown_v2(selection)}")
    lines.append(f"Ссылка: {link_text}")
    lines.append(f"Дата и время: `{when}`")
    return "\n".join(lines)

# Типы промокодов/наград — единый источник подписей с эмодзи для всех экранов
PROMOTYPE_LABELS = {1: "Антитаргет ⚜️", 2: "Префикс 🔰", 3: "Валюта 💵💶🪙"}

def promotype_label(promotype: int) -> str:
    return PROMOTYPE_LABELS.get(promotype, f"Тип {promotype}")

def build_promo_activation_text(user_id: int, username: str | None, promotype: int, selection: str | None, promocode: str, reward_str: str, activation_date: str) -> str:
    """Карточка активации промокода для топика «Промокоды» — в том же стиле,
    что и карточка заявки на приз (build_claim_card_text): @username(id), Тип, Выбор,
    Промокод, Срок действия, Дата и время."""
    lines = [
        f"@{escape_markdown_v2(username or 'без_юзернейма')}\\({user_id}\\)",
        f"Тип: *{escape_markdown_v2(promotype_label(promotype))}*",
    ]
    if selection:
        if promotype == 2:  # Префикс — копируемый пользовательский текст
            lines.append(f"Выбор: `{escape_markdown_v2(selection)}`")
        else:
            lines.append(f"Выбор: {escape_markdown_v2(selection)}")
    lines.append(f"Промокод: `{escape_markdown_v2(promocode)}`")
    lines.append(f"Срок действия: `{escape_markdown_v2(reward_str)}`")
    lines.append(f"Дата и время: `{escape_markdown_v2(format_datetime(activation_date))}`")
    return "\n".join(lines)

async def finalize_immediate_promocode(message: Message, state: FSMContext, promocode: str, promotype: int, reward_duration, selection: str | None = None) -> None:
    """Общая активация для типов промокода без доп. ввода (Антитаргет, Валюта):
    пишет активацию в БД, публикует карточку в топик «Промокоды» и подтверждает пользователю."""
    user_id = message.from_user.id
    activation_date = datetime.now(MSK).isoformat()
    reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"

    conn = await db_connect()
    c = await conn.cursor()
    try:
        await c.execute(
            "INSERT INTO user_promocodes (user_id, username, code, activation_date, promotype, selection, reward_duration, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
            (user_id, message.from_user.username, promocode, activation_date, promotype, selection, reward_duration))
        await c.execute("UPDATE promocodes SET activations_used = activations_used + 1 WHERE code = ?", (promocode,))
        await conn.commit()
    except asyncpg.UniqueViolationError:
        await conn.close()
        text = escape_markdown_v2("Вы уже активировали этот промокод.")
        await send_error_and_return_to_main(user_id, text, state)
        return
    except asyncpg.PostgresError as e:
        await conn.close()
        logger.error(f"Ошибка базы данных при активации промокода {promocode}: {e}")
        text = escape_markdown_v2("Ошибка базы данных. Попробуйте позже.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    promocodes_topic_id = await ensure_promocodes_topic()
    if promocodes_topic_id:
        promo_text = build_promo_activation_text(
            user_id, message.from_user.username,
            promotype, selection, promocode, reward_str, activation_date
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Выдано", callback_data=f"promo_ok_{user_id}_{promocode}"),
             InlineKeyboardButton(text="❌ Отказано", callback_data=f"promo_no_{user_id}_{promocode}")]
        ])
        try:
            sent = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=promocodes_topic_id, text=promo_text, reply_markup=keyboard)
            await c.execute("UPDATE user_promocodes SET card_message_id = ? WHERE user_id = ? AND code = ?", (sent.message_id, user_id, promocode))
            await conn.commit()
            logger.debug(f"Информация об активации промокода {promocode} отправлена в топик Промокоды для {user_id}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка отправки в топик Промокоды для {user_id}: {e}")
            logger.debug(f"Проблемный текст: {promo_text}")
    await conn.close()

    label = promotype_label(promotype)
    text = escape_markdown_v2(f"Промокод на {label} активирован. Ожидайте ответа администратора!")
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=(await state.get_data()).get('current_message_id'))
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Успешная активация промокода типа {promotype} для {user_id}, welcome_message_id={message_id}")
    else:
        await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке подтверждения. Промокод активирован, попробуйте еще раз."), state)

async def ensure_promocodes_topic():
    logger.debug("Проверка топика Промокоды")
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT thread_id FROM users WHERE user_id = ?", (-1,))
    result = await c.fetchone()

    if result and result[0]:
        thread_id = result[0]
        try:
            test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=escape_markdown_v2("Проверка топика"))
            await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
            logger.debug(f"Топик Промокоды активен: thread_id={thread_id}")
            await conn.close()
            return thread_id
        except TelegramBadRequest as e:
            logger.error(f"Топик Промокоды неактивен (thread_id={thread_id}): {e}")
            await c.execute("DELETE FROM users WHERE user_id = ?", (-1,))
            await conn.commit()
            await conn.close()

    try:
        test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, text=escape_markdown_v2("Проверка топика Промокоды"))
        if test_msg.message_thread_id:
            thread_id = test_msg.message_thread_id
            topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{thread_id}")
            if topic.title and topic.title.strip().lower() == PROMOCODES_TOPIC_NAME.lower():
                conn = await db_connect()
                c = await conn.cursor()
                await c.execute("INSERT INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, thread_id = EXCLUDED.thread_id, last_updated = EXCLUDED.last_updated",
                          (-1, PROMOCODES_TOPIC_NAME, thread_id, datetime.now(MSK).isoformat()))
                await conn.commit()
                await conn.close()
                await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
                logger.debug(f"Найден существующий топик Промокоды: thread_id={thread_id}")
                return thread_id
        await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка проверки существующего топика Промокоды: {e}")

    try:
        topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=PROMOCODES_TOPIC_NAME)
        thread_id = topic.message_thread_id
        conn = await db_connect()
        c = await conn.cursor()
        await c.execute("INSERT INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, thread_id = EXCLUDED.thread_id, last_updated = EXCLUDED.last_updated",
                  (-1, PROMOCODES_TOPIC_NAME, thread_id, datetime.now(MSK).isoformat()))
        await conn.commit()
        await conn.close()
        logger.debug(f"Создан новый топик Промокоды: thread_id={thread_id}")
        return thread_id
    except TelegramBadRequest as e:
        logger.error(f"Ошибка создания топика Промокоды: {e}")
        return None

# Проверка/создание топика для рассылки
async def ensure_broadcast_topic():
    logger.debug("Проверка топика рассылки")
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT thread_id FROM users WHERE user_id = ?", (0,))
    result = await c.fetchone()

    if result and result[0]:
        thread_id = result[0]
        try:
            test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=escape_markdown_v2("Проверка топика"))
            await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
            logger.debug(f"Топик рассылки активен: thread_id={thread_id}")
            await conn.close()
            return True
        except TelegramBadRequest as e:
            logger.error(f"Топик рассылки неактивен (thread_id={thread_id}): {e}")
            await c.execute("DELETE FROM users WHERE user_id = ?", (0,))
            await conn.commit()
            await conn.close()

    try:
        test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, text=escape_markdown_v2("Проверка топика рассылки"))
        if test_msg.message_thread_id:
            thread_id = test_msg.message_thread_id
            topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{thread_id}")
            if topic.title and topic.title.strip().lower() == BROADCAST_TOPIC_NAME.lower():
                conn = await db_connect()
                c = await conn.cursor()
                await c.execute("INSERT INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, thread_id = EXCLUDED.thread_id, last_updated = EXCLUDED.last_updated",
                          (0, BROADCAST_TOPIC_NAME, thread_id, datetime.now(MSK).isoformat()))
                await conn.commit()
                await conn.close()
                await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
                logger.debug(f"Найден существующий топик рассылки: thread_id={thread_id}")
                return True
        await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка проверки существующего топика: {e}")

    try:
        topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=BROADCAST_TOPIC_NAME)
        thread_id = topic.message_thread_id
        conn = await db_connect()
        c = await conn.cursor()
        await c.execute("INSERT INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username, thread_id = EXCLUDED.thread_id, last_updated = EXCLUDED.last_updated",
                  (0, BROADCAST_TOPIC_NAME, thread_id, datetime.now(MSK).isoformat()))
        await conn.commit()
        await conn.close()
        logger.debug(f"Создан новый топик рассылки: thread_id={thread_id}")
        return True
    except TelegramBadRequest as e:
        logger.error(f"Ошибка создания топика рассылки: {e}")
        return False

# Обновление данных пользователя
async def update_user_data(message: Message, thread_id: int = None):
    user_id = message.from_user.id
    username = message.from_user.username or None
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name or None
    language_code = message.from_user.language_code or None
    is_premium = message.from_user.is_premium or False
    last_updated = datetime.now(MSK).isoformat()
    new_name = f"{first_name} ({user_id})"
    admin_level = 4 if user_id in ADMIN_IDS else None
    logger.debug(f"Обновление данных пользователя {user_id}: {new_name}")

    conn = await db_connect()
    c = await conn.cursor()
    try:
        await c.execute("SELECT first_name, thread_id, admin_level FROM users WHERE user_id = ?", (user_id,))
        existing = await c.fetchone()
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка при запросе пользователя {user_id}: {e}")
        await conn.close()
        return None

    current_thread_id = thread_id or (existing[1] if existing else None)
    old_first_name = existing[0] if existing else None
    old_name = f"{old_first_name} ({user_id})" if existing else None
    admin_level = admin_level if admin_level is not None else (existing[2] if existing else 0)

    try:
        await c.execute('''INSERT INTO users
                     (user_id, username, first_name, last_name, language_code, is_premium, thread_id, last_updated, admin_level)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                     ON CONFLICT (user_id) DO UPDATE SET
                       username = EXCLUDED.username, first_name = EXCLUDED.first_name,
                       last_name = EXCLUDED.last_name, language_code = EXCLUDED.language_code,
                       is_premium = EXCLUDED.is_premium, thread_id = EXCLUDED.thread_id,
                       last_updated = EXCLUDED.last_updated, admin_level = EXCLUDED.admin_level''',
                  (user_id, username, first_name, last_name, language_code, is_premium, current_thread_id, last_updated, admin_level))
        await conn.commit()
        logger.debug(f"Пользователь {user_id} успешно добавлен/обновлён в базе данных")
    except asyncpg.PostgresError as e:
        logger.error(f"Ошибка при сохранении пользователя {user_id}: {e}")
        await conn.close()
        return None

    await conn.close()

    if current_thread_id and old_name != new_name:
        try:
            await bot.edit_forum_topic(chat_id=GROUP_CHAT_ID, message_thread_id=current_thread_id, name=new_name)
            logger.debug(f"Топик {current_thread_id} обновлен: {new_name}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка обновления топика {current_thread_id}: {e}")
            try:
                topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=new_name)
                new_thread_id = topic.message_thread_id
                conn = await db_connect()
                c = await conn.cursor()
                await c.execute("UPDATE users SET thread_id = ? WHERE user_id = ?", (new_thread_id, user_id))
                await conn.commit()
                await conn.close()
                logger.debug(f"Создан новый топик {new_thread_id} для {user_id}")
                return new_thread_id
            except TelegramBadRequest as e:
                logger.error(f"Ошибка создания топика для {user_id}: {e}")
    logger.debug(f"Данные пользователя {user_id} обновлены, thread_id={current_thread_id}")
    return current_thread_id

# Проверка прав бота
async def check_bot_permissions():
    logger.debug("Проверка прав бота")
    try:
        chat = await bot.get_chat(GROUP_CHAT_ID)
        member = await bot.get_chat_member(GROUP_CHAT_ID, bot.id)
        permissions = {
            'can_post_messages': getattr(member, 'can_post_messages', None),
            'can_manage_topics': getattr(member, 'can_manage_topics', None),
            'can_edit_messages': getattr(member, 'can_edit_messages', None),
            'can_delete_messages': getattr(member, 'can_delete_messages', None),
            'is_admin': member.status == 'administrator',
            'is_forum': chat.is_forum
        }
        logger.debug(f"Текущие права бота: {permissions}")

        test_success = False
        edit_text = None  # чтобы лог в except не падал с NameError, если упадёт ещё send_message
        try:
            test_text = escape_markdown_v2("Тест сообщения")
            test_msg = await bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=test_text
            )
            edit_text = escape_markdown_v2("Тест сообщения (отредактировано)")
            await bot.edit_message_text(
                chat_id=GROUP_CHAT_ID,
                message_id=test_msg.message_id,
                text=edit_text
            )
            await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
            logger.debug("Тестовое сообщение отправлено, отредактировано и удалено")
            test_success = True
        except TelegramBadRequest as e:
            logger.error(f"Ошибка тестовой отправки/редактирования/удаления: {e}")
            logger.debug(f"Проблемный текст: {edit_text}")

        if not permissions['is_forum'] or not permissions['is_admin'] or not permissions['can_manage_topics'] or not test_success:
            logger.error("Бот не имеет необходимых прав")
            return False
        logger.debug("Проверка прав завершена, бот может работать")
        return True
    except TelegramBadRequest as e:
        logger.error(f"Ошибка проверки прав бота: {e}")
        return False

# Состояния FSM
class UserStates(StatesGroup):
    waiting_for_promocode = State()
    waiting_for_prefix = State()
    waiting_for_prize_data = State()

class AdminStates(StatesGroup):
    waiting_for_promocode_type = State()
    waiting_for_promo_currency = State()
    waiting_for_validity = State()
    waiting_for_reward_duration = State()
    waiting_for_activations = State()
    waiting_for_remove_code = State()
    waiting_for_admin_id = State()  # Для назначения админа
    waiting_for_admin_level = State()  # Для выбора уровня админа
    waiting_for_remove_admin_id = State()  # Для снятия админа

# Функция для отправки сообщений пользователю в ЛС
async def send_to_user_and_topic(user_id: int, text: str, state: FSMContext, reply_markup=None, prev_message_id: int = None):
    """Отправляет сообщение пользователю в ЛС, предварительно удаляя предыдущее (если есть).
    В топик группы не дублируется — там остаются только структурированные заявки
    (send_prize_claim_summary) и промокодные форварды/отчёты."""
    logger.debug(f"Отправка сообщения пользователю {user_id}, prev_message_id={prev_message_id}, text: {text[:50]}...")

    if prev_message_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=prev_message_id)
            logger.debug(f"Успешно удалено сообщение {prev_message_id} в ЛС {user_id}")
        except TelegramBadRequest as e:
            logger.warning(f"Ошибка удаления сообщения {prev_message_id} в ЛС {user_id}: {e}")

    try:
        msg = await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup)
        logger.debug(f"Сообщение отправлено в ЛС {user_id}, message_id={msg.message_id}")
        return True, msg.message_id
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки в ЛС {user_id}: {e}")
        return False, None

async def get_user_thread_id(user_id: int, state: FSMContext) -> int | None:
    """Извлекает thread_id из состояния или базы данных."""
    data = await state.get_data()
    thread_id = data.get('thread_id')
    if thread_id:
        logger.debug(f"thread_id={thread_id} извлечён из состояния для {user_id}")
        return thread_id

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT thread_id FROM users WHERE user_id = ?", (user_id,))
    result = await c.fetchone()
    await conn.close()
    if result and result[0]:
        thread_id = result[0]
        await state.update_data(thread_id=thread_id)
        logger.debug(f"thread_id={thread_id} извлечён из базы для {user_id}")
        return thread_id

    # Если топика нет, создаём новый
    try:
        first_name = (await bot.get_chat(user_id)).first_name
        topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=f"{first_name} ({user_id})")
        thread_id = topic.message_thread_id
        conn = await db_connect()
        c = await conn.cursor()
        await c.execute("UPDATE users SET thread_id = ? WHERE user_id = ?", (thread_id, user_id))
        await conn.commit()
        await conn.close()
        await state.update_data(thread_id=thread_id)
        logger.debug(f"Создан новый топик {thread_id} для {user_id}")
        return thread_id
    except TelegramBadRequest as e:
        logger.error(f"Ошибка создания топика для {user_id}: {e}")
        return None

# Функция для пересылки сообщений пользователя
async def forward_to_topic(message: Message, state: FSMContext):
    user_id = message.from_user.id
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT thread_id FROM users WHERE user_id = ?", (user_id,))
    result = await c.fetchone()
    await conn.close()
    thread_id = result[0] if result else None

    if not thread_id:
        first_name = message.from_user.first_name
        try:
            topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=f"{first_name} ({user_id})")
            thread_id = topic.message_thread_id
            thread_id = await update_user_data(message, thread_id)
            await state.update_data(thread_id=thread_id)
            logger.debug(f"Создан топик {thread_id} для {user_id}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка создания топика для {user_id}: {e}")
            await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при создании топика."))
            return False

    try:
        await bot.forward_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, from_chat_id=message.chat.id, message_id=message.message_id)
        logger.debug(f"Сообщение от {user_id} переслано в топик {thread_id}")
        return True
    except TelegramBadRequest as e:
        logger.error(f"Ошибка пересылки сообщения от {user_id} в топик {thread_id}: {e}")
        try:
            await bot.forward_message(chat_id=GROUP_CHAT_ID, from_chat_id=message.chat.id, message_id=message.message_id)
            logger.debug(f"Сообщение от {user_id} переслано в общий чат")
            return True
        except TelegramBadRequest as e:
            logger.error(f"Ошибка пересылки в общий чат для {user_id}: {e}")
            await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при пересылке сообщения."))
            return False

async def send_prize_claim_summary(user_id: int, thread_id: int | None, reward_type: int, selection: str | None, link: str | None, from_user) -> None:
    """Публикует в личном топике пользователя ОДНУ итоговую карточку заявки на приз
    (username(id), тип, выбор, ссылка, дата и время) с кнопками Выдано/Отказано —
    вместо пересылки сырых сообщений. Заявка также сохраняется в БД для «Мои заявки»."""
    if not thread_id:
        logger.error(f"Не удалось определить топик для карточки приза пользователя {user_id}")
        return
    created_at = datetime.now(MSK).isoformat()
    summary = build_claim_card_text(user_id, from_user.username, reward_type, selection, link, created_at)

    conn = await db_connect()
    c = await conn.cursor()
    try:
        claim_id = await conn.fetchval(
            "INSERT INTO prize_claims (user_id, username, choice, detail, link, status, created_at) VALUES (?, ?, ?, ?, ?, 'pending', ?) RETURNING id",
            (user_id, from_user.username, reward_type, selection, link, created_at)
        )
        await conn.commit()

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Выдано", callback_data=f"claim_ok_{claim_id}"),
             InlineKeyboardButton(text="❌ Отказано", callback_data=f"claim_no_{claim_id}")]
        ])
        try:
            sent = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=summary, reply_markup=keyboard)
            await c.execute("UPDATE prize_claims SET card_message_id = ? WHERE id = ?", (sent.message_id, claim_id))
            await conn.commit()
            logger.debug(f"Карточка заявки #{claim_id} отправлена в топик {thread_id} для {user_id}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка отправки карточки заявки для {user_id}: {e}")
    finally:
        await conn.close()

@dp.callback_query(F.data.startswith("claim_ok_") | F.data.startswith("claim_no_"))
async def handle_claim_decision(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает нажатие админом «Выдано»/«Отказано» на карточке заявки:
    фиксирует решение в БД, убирает кнопки и уведомляет пользователя."""
    admin_id = callback.from_user.id
    if not await check_admin_level(admin_id, 1):
        await callback.answer("У вас нет прав для этого действия.", show_alert=True)
        return

    approve = callback.data.startswith("claim_ok_")
    try:
        claim_id = int(callback.data.rsplit("_", 1)[-1])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT user_id, username, choice, detail, link, status, created_at FROM prize_claims WHERE id = ?", (claim_id,))
    row = await c.fetchone()
    if not row:
        await conn.close()
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    claim_user_id, username, choice, detail, link, status, created_at = row

    new_status = 'approved' if approve else 'rejected'
    resolved_at = datetime.now(MSK).isoformat()
    await c.execute(
        "UPDATE prize_claims SET status = ?, resolved_at = ?, resolved_by = ? WHERE id = ? AND status = 'pending'",
        (new_status, resolved_at, admin_id, claim_id)
    )
    await conn.commit()
    updated = c.rowcount  # 0, если заявку уже успел обработать другой админ (защита от гонки)
    await conn.close()

    if updated == 0:
        await callback.answer("Эта заявка уже обработана.", show_alert=True)
        return

    await safe_callback_answer(callback)

    admin_label = callback.from_user.username or callback.from_user.first_name or str(admin_id)
    status_label = "✅ Выдано" if approve else "❌ Отказано"
    when = escape_markdown_v2(format_datetime(resolved_at))
    card_text = build_claim_card_text(claim_user_id, username, choice, detail, link, created_at)
    card_text += f"\n\nСтатус: *{status_label}*\nОбработал: @{escape_markdown_v2(admin_label)} \\({when}\\)"

    try:
        await callback.message.edit_text(card_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    except TelegramBadRequest as e:
        logger.error(f"Ошибка редактирования карточки заявки #{claim_id}: {e}")

    user_status_text = (
        f"Ваша заявка на *{escape_markdown_v2(promotype_label(choice))}* обработана\\!\n"
        f"Статус: *{status_label}*"
    )
    try:
        await bot.send_message(chat_id=claim_user_id, text=user_status_text)
        logger.debug(f"Пользователь {claim_user_id} уведомлён по заявке #{claim_id}")
    except TelegramBadRequest as e:
        logger.error(f"Не удалось уведомить пользователя {claim_user_id} по заявке #{claim_id}: {e}")

@dp.callback_query(F.data.startswith("promo_ok_") | F.data.startswith("promo_no_"))
async def handle_promo_claim_decision(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает нажатие админом «Выдано»/«Отказано» на карточке активации промокода —
    то же самое, что уже есть для заявок на приз, но для промокодной ветки."""
    admin_id = callback.from_user.id
    if not await check_admin_level(admin_id, 1):
        await callback.answer("У вас нет прав для этого действия.", show_alert=True)
        return

    approve = callback.data.startswith("promo_ok_")
    try:
        _, _, target_user_id_str, promocode = callback.data.split("_", 3)
        target_user_id = int(target_user_id_str)
    except (ValueError, IndexError):
        await callback.answer("Некорректные данные.", show_alert=True)
        return

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute(
        "SELECT username, promotype, selection, reward_duration, activation_date, status FROM user_promocodes WHERE user_id = ? AND code = ?",
        (target_user_id, promocode)
    )
    row = await c.fetchone()
    if not row:
        await conn.close()
        await callback.answer("Активация не найдена.", show_alert=True)
        return
    username, promotype, selection, reward_duration, activation_date, status = row

    new_status = 'approved' if approve else 'rejected'
    resolved_at = datetime.now(MSK).isoformat()
    await c.execute(
        "UPDATE user_promocodes SET status = ?, resolved_at = ?, resolved_by = ? WHERE user_id = ? AND code = ? AND status = 'pending'",
        (new_status, resolved_at, admin_id, target_user_id, promocode)
    )
    await conn.commit()
    updated = c.rowcount  # 0, если заявку уже успел обработать другой админ (защита от гонки)
    await conn.close()

    if updated == 0:
        await callback.answer("Эта активация уже обработана.", show_alert=True)
        return

    await safe_callback_answer(callback)

    admin_label = callback.from_user.username or callback.from_user.first_name or str(admin_id)
    status_label = "✅ Выдано" if approve else "❌ Отказано"
    when = escape_markdown_v2(format_datetime(resolved_at))
    reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
    card_text = build_promo_activation_text(target_user_id, username, promotype, selection, promocode, reward_str, activation_date)
    card_text += f"\n\nСтатус: *{status_label}*\nОбработал: @{escape_markdown_v2(admin_label)} \\({when}\\)"

    try:
        await callback.message.edit_text(card_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[]))
    except TelegramBadRequest as e:
        logger.error(f"Ошибка редактирования карточки промокода {promocode}: {e}")

    user_status_text = (
        f"Ваш промокод на *{escape_markdown_v2(promotype_label(promotype))}* обработан\\!\n"
        f"Статус: *{status_label}*"
    )
    try:
        await bot.send_message(chat_id=target_user_id, text=user_status_text)
        logger.debug(f"Пользователь {target_user_id} уведомлён по промокоду {promocode}")
    except TelegramBadRequest as e:
        logger.error(f"Не удалось уведомить пользователя {target_user_id} по промокоду {promocode}: {e}")

@dp.callback_query(F.data == "my_claims")
async def show_my_claims(callback: CallbackQuery, state: FSMContext):
    """Показывает пользователю его последние заявки на призы и их статус."""
    user_id = callback.from_user.id
    await safe_callback_answer(callback)

    data = await state.get_data()
    prev_message_id = data.get('current_message_id') or data.get('welcome_message_id')

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute(
        "SELECT choice, detail, status, created_at FROM prize_claims WHERE user_id = ? ORDER BY id DESC LIMIT 10",
        (user_id,)
    )
    claims = await c.fetchall()
    await conn.close()

    status_labels = {'pending': '⏳ На рассмотрении', 'approved': '✅ Выдано', 'rejected': '❌ Отказано'}

    if not claims:
        text = escape_markdown_v2("У вас пока нет заявок на призы.")
    else:
        lines = ["📜 *Ваши последние заявки*:\n"]
        for choice, detail, status, created_at in claims:
            reward_label = promotype_label(choice)
            label = reward_label if not detail else f"{reward_label}: {detail}"
            status_text = status_labels.get(status, status)
            lines.append(
                f"🔹 {escape_markdown_v2(label)} — {escape_markdown_v2(status_text)}\n"
                f"   `{escape_markdown_v2(format_datetime(created_at))}`"
            )
        text = "\n\n".join(lines)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
    else:
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при загрузке заявок."))

@dp.message(F.text == "Назначить админа")
async def appoint_admin_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для назначения админов."))
        return
    text = escape_markdown_v2("Введите ID пользователя, которого хотите назначить админом:")
    await message.reply(text)
    await state.set_state(AdminStates.waiting_for_admin_id)

@dp.message(AdminStates.waiting_for_admin_id)
async def process_admin_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для назначения админов."))
        await state.clear()
        return
    try:
        new_admin_id = int(message.text.strip())
    except ValueError:
        await message.reply(escape_markdown_v2("Введите корректный ID пользователя (число)."))
        return
    if new_admin_id in ADMIN_IDS:
        await message.reply(escape_markdown_v2("Этот пользователь имеет максимальный уровень доступа и не может быть изменён."))
        await state.clear()
        return
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT user_id FROM users WHERE user_id = ?", (new_admin_id,))
    if not await c.fetchone():
        await message.reply(escape_markdown_v2("Пользователь не найден в базе. Он должен сначала взаимодействовать с ботом."))
        await conn.close()
        return
    await conn.close()
    await state.update_data(new_admin_id=new_admin_id)
    max_level = 4 if user_id in ADMIN_IDS else 3
    text = escape_markdown_v2(f"Введите уровень админа (1-{max_level}):")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"admin_level_{i}") for i in range(1, max_level + 1)]
    ])
    await message.reply(text, reply_markup=keyboard)
    await state.set_state(AdminStates.waiting_for_admin_level)

@dp.callback_query(F.data.startswith("admin_level_"))
async def process_admin_level(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await check_admin_level(user_id, 3):
        await callback.message.reply(escape_markdown_v2("У вас нет прав для назначения админов."))
        await state.clear()
        return
    await safe_callback_answer(callback)
    admin_level = int(callback.data.split("_")[2])
    
    # Запрет назначения уровня 4
    if admin_level == 4:
        await callback.message.reply(
            escape_markdown_v2("Уровень 4 доступен только для предустановленных администраторов.")
        )
        await state.clear()
        return
        
    # Проверка прав для назначения уровня 3
    is_superadmin = user_id in ADMIN_IDS
    if admin_level == 3 and not is_superadmin:
        await callback.message.reply(
            escape_markdown_v2("Только суперадмины могут назначать уровень 3.")
        )
        await state.clear()
        return
        
    data = await state.get_data()
    new_admin_id = data.get('new_admin_id')
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("UPDATE users SET admin_level = ? WHERE user_id = ?", (admin_level, new_admin_id))
    await conn.commit()
    await conn.close()
    await callback.message.reply(
        escape_markdown_v2(f"Пользователь {new_admin_id} назначен админом уровня {admin_level}.")
    )
    await state.clear()

@dp.message(F.text == "Снять админа")
async def remove_admin_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для снятия админов."))
        return
    text = escape_markdown_v2("Введите ID пользователя, с которого хотите снять админ-права:")
    await message.reply(text)
    await state.set_state(AdminStates.waiting_for_remove_admin_id)

@dp.message(AdminStates.waiting_for_remove_admin_id)
async def process_remove_admin_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для снятия админов."))
        await state.clear()
        return
    try:
        remove_admin_id = int(message.text.strip())
    except ValueError:
        await message.reply(escape_markdown_v2("Введите корректный ID пользователя (число)."))
        return
    if remove_admin_id in ADMIN_IDS:
        await message.reply(escape_markdown_v2("Нельзя снять права с суперадмина."))
        await state.clear()
        return
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT admin_level FROM users WHERE user_id = ?", (remove_admin_id,))
    result = await c.fetchone()
    if not result or result[0] == 0:
        await message.reply(escape_markdown_v2("Пользователь не является админом."))
        await conn.close()
        await state.clear()
        return
    await c.execute("UPDATE users SET admin_level = 0 WHERE user_id = ?", (remove_admin_id,))
    await conn.commit()
    await conn.close()
    await message.reply(
        escape_markdown_v2(f"Админ-права сняты с пользователя {remove_admin_id}.")
    )
    await state.clear()

@dp.message(F.text == "Просмотреть всех админов")
async def view_all_admins(message: Message):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 4):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра админов."))
        return
    
    conn = await db_connect()
    c = await conn.cursor()
    
    # Получаем обычных админов (исключая суперадминов)
    if ADMIN_IDS:
        placeholders = ','.join('?' * len(ADMIN_IDS))
        await c.execute(f"""
            SELECT user_id, first_name, admin_level 
            FROM users 
            WHERE admin_level > 0 
            AND user_id NOT IN ({placeholders})
        """, ADMIN_IDS)
    else:
        await c.execute("SELECT user_id, first_name, admin_level FROM users WHERE admin_level > 0")
    admins = await c.fetchall()
    
    # Получаем суперадминов
    super_admins = []
    for admin_id in ADMIN_IDS:
        await c.execute("SELECT first_name FROM users WHERE user_id = ?", (admin_id,))
        result = await c.fetchone()
        first_name = result[0] if result else "Неизвестно"
        super_admins.append((admin_id, first_name))
    
    await conn.close()

    response = "🛠 *Список администраторов*:\n\n"
    
    # Вывод суперадминов
    for admin_id, first_name in super_admins:
        response += (
            f"👑 *Суперадмин*:\n"
            f"ID: `{admin_id}`\n"
            f"Имя: `{escape_markdown_v2(first_name)}`\n"
            f"Уровень: 4\n\n"
        )
    
    # Вывод обычных админов - исправлено экранирование скобок
    for admin_id, first_name, level in admins:
        response += (
            f"👤 *Админ* \\(уровень {level}\\):\n"  # Экранирование скобок
            f"ID: `{admin_id}`\n"
            f"Имя: `{escape_markdown_v2(first_name)}`\n\n"
        )
    
    if not admins and not super_admins:
        response = "Нет назначенных администраторов."
        
    try:
        await message.reply(response)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки списка админов: {e}")
        # Попробуем отправить без форматирования
        plain_response = re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', response)
        await message.reply(plain_response)

# Обработчик команды /start
@dp.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    logger.debug(f"Получена команда /start от {user_id}: {message.from_user.first_name}")
    thread_id = await update_user_data(message)
    welcome_message, keyboard = build_main_menu(message.from_user.first_name)
    success, message_id = await send_to_user_and_topic(user_id, welcome_message, state, reply_markup=keyboard, prev_message_id=None)
    if success:
        await state.update_data(welcome_message_id=message_id, thread_id=thread_id)
        logger.debug(f"Приветствие отправлено для {user_id}, welcome_message_id={message_id}, thread_id={thread_id}")
    else:
        logger.error(f"Ошибка отправки приветствия для {user_id}")

# Обработчик выбора "Забрать приз"
async def send_prize_link_prompt(user_id: int, state: FSMContext, prev_message_id: int | None) -> None:
    """Экран 1 сценария «Забрать приз»: просьба прислать ссылку на победителей."""
    text = escape_markdown_v2("Отправьте текст/ссылку на сообщение с победителями режима/ивента:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
        await state.set_state(UserStates.waiting_for_prize_data)
        logger.debug(f"Установлено состояние waiting_for_prize_data, current_message_id={message_id} для {user_id}")
    else:
        logger.error(f"Ошибка отправки сообщения для {user_id}")
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."))

async def send_prize_choice_screen(user_id: int, state: FSMContext, prev_message_id: int | None, prize_link: str | None) -> None:
    """Экран 2 сценария «Забрать приз»: выбор конкретной награды."""
    text = escape_markdown_v2("Теперь укажите желаемый приз:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Канал с призами", url="https://t.me/+5NkH3lETrxNkYjhi")],
        [InlineKeyboardButton(text="Антитаргет ⚜️", callback_data="select_prize_antitarget"),
         InlineKeyboardButton(text="Префикс 🔰", callback_data="select_prize_prefix")],
        [InlineKeyboardButton(text="Валюта 💵💶🪙", callback_data="select_prize_currency")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_prize_link")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
    if success:
        await state.update_data(current_message_id=message_id, prize_link=prize_link)
        await state.set_state(None)
        logger.debug(f"Отправлен запрос выбора приза, current_message_id={message_id} для {user_id}")
    else:
        logger.error(f"Ошибка отправки запроса выбора приза для {user_id}")
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."))

@dp.callback_query(F.data == "get_prize")
async def get_prize(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} выбрал 'Забрать приз'")
    await safe_callback_answer(callback)

    data = await state.get_data()
    prev_message_id = data.get('welcome_message_id') or data.get('current_message_id')
    logger.debug(f"Извлечён prev_message_id={prev_message_id} для {user_id}")
    await send_prize_link_prompt(user_id, state, prev_message_id)

@dp.callback_query(F.data == "back_to_prize_link")
async def back_to_prize_link(callback: CallbackQuery, state: FSMContext):
    """«Назад» с экрана выбора приза — возвращает на шаг ввода ссылки, а не в главное меню."""
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} вернулся к вводу ссылки на приз")
    await safe_callback_answer(callback)
    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    await send_prize_link_prompt(user_id, state, prev_message_id)

@dp.callback_query(F.data == "back_to_prize_choice")
async def back_to_prize_choice(callback: CallbackQuery, state: FSMContext):
    """«Назад» с экрана выбора валюты/ввода префикса — возвращает на выбор награды."""
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} вернулся к выбору приза")
    await safe_callback_answer(callback)
    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    prize_link = data.get('prize_link')
    await send_prize_choice_screen(user_id, state, prev_message_id, prize_link)

# Обработчик ввода победителей
@dp.message(UserStates.waiting_for_prize_data)
async def process_prize_data(message: Message, state: FSMContext):
    user_id = message.from_user.id
    prize_link = (message.text or message.caption or "").strip()
    logger.debug(f"Получена ссылка на победителей от {user_id}: {prize_link}")
    if not prize_link:
        text = escape_markdown_v2("Пришлите текст или ссылку на сообщение с победителями.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    logger.debug(f"Извлечён current_message_id={prev_message_id} для {user_id}")
    await send_prize_choice_screen(user_id, state, prev_message_id, prize_link)

@dp.callback_query(F.data.in_(["select_prize_antitarget", "select_prize_prefix", "select_prize_currency"]))
async def handle_prize_selection(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} выбрал приз: {callback.data}")
    await safe_callback_answer(callback)

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    prize_link = data.get('prize_link')
    logger.debug(f"Извлечён prev_message_id={prev_message_id} для {user_id}")

    # Извлекаем thread_id
    thread_id = await get_user_thread_id(user_id, state)
    if not thread_id:
        text = escape_markdown_v2("Ошибка: не удалось определить топик. Начните заново.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    if callback.data == "select_prize_antitarget":
        # Одна итоговая карточка заявки вместо отдельного служебного сообщения
        await send_prize_claim_summary(user_id, thread_id, 1, None, prize_link, callback.from_user)

        text = r"Заявка на Антитаргет отправлена администраторам\. Ожидайте выдачу приза ✨" "\n\nДля повторного взаимодействия с ботом отправьте команду /start"
        success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=prev_message_id)
        if success:
            await state.clear()
            await state.update_data(welcome_message_id=message_id)
            logger.debug(f"Заявка на Антитаргет отправлена для {user_id}, welcome_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке заявки. Заявка отправлена, попробуйте снова."), state)

    elif callback.data == "select_prize_currency":
        text = escape_markdown_v2("Выберите валюту:")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Евро 💶", callback_data="currency_eur"),
             InlineKeyboardButton(text="Доллары волка 💵", callback_data="currency_usd")],
            [InlineKeyboardButton(text="Монета 🪙", callback_data="currency_coin")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_prize_choice")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
        if success:
            await state.update_data(current_message_id=message_id, thread_id=thread_id, prize_link=prize_link)
            logger.debug(f"Запрос выбора валюты отправлен для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке выбора валюты."), state)

    elif callback.data == "select_prize_prefix":
        text = (
            f"Напишите желаемый префикс\\.\n"
            f"__Ограничение__: до 16 символов\\!\n"
            f"__Не допускаются__: оскорбления, эмодзи, должности админов\\."
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_prize_choice")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
        if success:
            await state.clear()
            await state.update_data(current_message_id=message_id, promocode=None, thread_id=thread_id, prize_link=prize_link)
            await state.set_state(UserStates.waiting_for_prefix)
            logger.debug(f"Запрос префикса отправлен для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке запроса префикса."), state)

@dp.callback_query(F.data.startswith("currency_"))
async def handle_currency_selection(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} выбрал валюту: {callback.data}")
    await safe_callback_answer(callback)

    currency_labels = {
        "currency_eur": "Евро 💶",
        "currency_usd": "Доллары волка 💵",
        "currency_coin": "Монета 🪙",
    }
    choice = currency_labels.get(callback.data)
    if not choice:
        return

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    prize_link = data.get('prize_link')
    thread_id = data.get('thread_id') or await get_user_thread_id(user_id, state)
    if not thread_id:
        text = escape_markdown_v2("Ошибка: не удалось определить топик. Начните заново.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    # Одна итоговая карточка заявки вместо отдельного служебного сообщения
    await send_prize_claim_summary(user_id, thread_id, 3, choice, prize_link, callback.from_user)

    text = r"Заявка на Валюту отправлена администраторам\. Ожидайте выдачу приза ✨" "\n\nДля повторного взаимодействия с ботом отправьте команду /start"
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=prev_message_id)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Заявка на валюту отправлена для {user_id}, welcome_message_id={message_id}")
    else:
        await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке заявки. Заявка отправлена, попробуйте снова."), state)

# Обработчик выбора "Ввести промокод"
@dp.callback_query(F.data == "enter_promocode")
async def enter_promocode(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await safe_callback_answer(callback)

    data = await state.get_data()
    welcome_message_id = data.get('welcome_message_id')

    text = escape_markdown_v2("Введите промокод:")
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=welcome_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
        await state.set_state(UserStates.waiting_for_promocode)
    else:
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."))

@dp.message(UserStates.waiting_for_promocode)
async def process_promocode(message: Message, state: FSMContext):
    user_id = message.from_user.id
    promocode = message.text.strip()
    logger.debug(f"Получен промокод от {user_id}: {promocode}")

    admin_commands = [
        "Просмотреть активные промокоды", "Просмотреть все промокоды", "Создать промокод", 
        "Удалить промокод", "Назначить админа", "Снять админа", "Просмотреть всех админов"
    ]
    if promocode in admin_commands:
        required_level = {
            "Просмотреть активные промокоды": 1,
            "Просмотреть все промокоды": 1,
            "Создать промокод": 1,
            "Удалить промокод": 2,
            "Назначить админа": 3,
            "Снять админа": 3,
            "Просмотреть всех админов": 4
        }
        if not await check_admin_level(user_id, required_level[promocode]):
            text = escape_markdown_v2("У вас нет доступа к этой команде.")
            await send_error_and_return_to_main(user_id, text, state)
            return
        await state.clear()
        if promocode == "Просмотреть активные промокоды":
            await view_activated_promocodes(message)
        elif promocode == "Просмотреть все промокоды":
            await view_all_promocodes(message)
        elif promocode == "Создать промокод":
            await create_promocode_start(message, state)
        elif promocode == "Удалить промокод":
            await remove_promocode_start(message, state)
        elif promocode == "Назначить админа":
            await appoint_admin_start(message, state)
        elif promocode == "Снять админа":
            await remove_admin_start(message, state)
        elif promocode == "Просмотреть всех админов":
            await view_all_admins(message)
        return

    if not re.match(r'^[123][A-Z0-9]{14}$', promocode):
        text = escape_markdown_v2("Неверный формат промокода. Должно быть 15 символов, начиная с 1, 2 или 3.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT type, active, activation_deadline, reward_duration, activations_limit, activations_used, currency_choice FROM promocodes WHERE code = ?", (promocode,))
    result = await c.fetchone()
    if not result:
        text = escape_markdown_v2("Промокод не найден.")
        await send_error_and_return_to_main(user_id, text, state)
        await conn.close()
        return
    promotype, active, activation_deadline, reward_duration, activations_limit, activations_used, currency_choice = result

    if not active:
        text = escape_markdown_v2("Промокод неактивирован.")
        await send_error_and_return_to_main(user_id, text, state)
        await conn.close()
        return
    if activation_deadline and datetime.fromisoformat(activation_deadline.replace('Z', '+00:00')) < datetime.now(MSK):
        text = escape_markdown_v2("Срок активации промокода истёк.")
        await send_error_and_return_to_main(user_id, text, state)
        await conn.close()
        return
    if activations_used >= activations_limit:
        text = escape_markdown_v2("Лимит активаций исчерпан.")
        await send_error_and_return_to_main(user_id, text, state)
        await conn.close()
        return

    await c.execute("SELECT user_id FROM user_promocodes WHERE user_id = ? AND code = ?", (user_id, promocode))
    if await c.fetchone():
        text = escape_markdown_v2("Вы уже активировали этот промокод.")
        await send_error_and_return_to_main(user_id, text, state)
        await conn.close()
        return

    await conn.close()

    if promotype in (1, 3):
        await finalize_immediate_promocode(message, state, promocode, promotype, reward_duration, selection=currency_choice)
    elif promotype == 2:
        text = r"Промокод на Префикс активирован\."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Получить префикс", callback_data="request_prefix")],
            [InlineKeyboardButton(text="Что такое префикс?", callback_data="what_is_prefix")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=(await state.get_data()).get('current_message_id'))
        if success:
            await state.clear()
            await state.update_data(current_message_id=message_id, promocode=promocode)
            logger.debug(f"Успешная активация Префикса для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке подтверждения. Промокод активирован, попробуйте еще."), state)

@dp.callback_query(F.data.in_(["request_prefix", "what_is_prefix", "back_to_prefix_menu"]))
async def handle_prefix_buttons(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await safe_callback_answer(callback)

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')  # Используем current_message_id
    logger.debug(f"Извлечён prev_message_id={prev_message_id} для {user_id}")

    # Извлекаем thread_id
    thread_id = await get_user_thread_id(user_id, state)
    if not thread_id:
        text = escape_markdown_v2("Ошибка: не удалось определить топик. Начните заново.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    if callback.data == "request_prefix":
        text = (
            f"Напишите желаемый префикс\\.\n"
            f"__Ограничение__: до 16 символов\\!\n"
            f"__Не допускаются__: оскорбления, эмодзи, должности админов\\."
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Отмена", callback_data="back_to_main")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
        if success:
            await state.update_data(current_message_id=message_id, thread_id=thread_id)
            await state.set_state(UserStates.waiting_for_prefix)
            logger.debug(f"Запрос префикса отправлен для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке запроса префикса."), state)
    elif callback.data == "what_is_prefix":
        text = (
            f"*Префикс* — надпись рядом с ником в чате мафии, "
            f"с его помощью ты можешь писать с знаком \\! когда в игре ночь, "
            f"после убийства, находясь не в игре\\."
        )
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Назад", callback_data="back_to_prefix_menu")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
        if success:
            await state.update_data(current_message_id=message_id, thread_id=thread_id)  # Сохраняем как current_message_id
            logger.debug(f"Информация о префиксе отправлена для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке информации о префиксе."), state)
    elif callback.data == "back_to_prefix_menu":
        promocode = data.get('promocode')
        if not promocode:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка: промокод не найден. Введите промокод заново."), state)
            return
        text = r"Промокод на Префикс активирован\."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Получить префикс", callback_data="request_prefix")],
            [InlineKeyboardButton(text="Что такое префикс?", callback_data="what_is_prefix")]
        ])
        success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
        if success:
            await state.update_data(current_message_id=message_id, thread_id=thread_id)  # Обновляем current_message_id
            logger.debug(f"Возвращено меню префикса для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при возврате к меню префикса."), state)

async def send_error_and_return_to_main(user_id: int, error_text: str, state: FSMContext):
    """Показывает предупреждение вместе с главным меню ОДНИМ сообщением. Раньше это были
    два сообщения подряд (ошибка, затем сразу поверх неё — главное меню), из-за чего
    пользователь не успевал прочитать предупреждение — оно тут же исчезало."""
    prev_message_id = (await state.get_data()).get('current_message_id')
    first_name = (await bot.get_chat(user_id)).first_name
    welcome_message, keyboard = build_main_menu(first_name)
    combined_text = f"⚠️ {error_text}\n\n{welcome_message}"
    success, message_id = await send_to_user_and_topic(user_id, combined_text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Показано предупреждение и главное меню для {user_id}, welcome_message_id={message_id}")
    else:
        logger.error(f"Ошибка отправки сообщения об ошибке пользователю {user_id}")

async def send_warning_and_retry(user_id: int, warning_text: str, retry_text: str, state: FSMContext, reply_markup=None) -> None:
    """Показывает предупреждение и повторно просит ввод — БЕЗ возврата в главное меню и без
    сброса состояния: пользователь остаётся на этом же шаге и может ввести значение заново."""
    prev_message_id = (await state.get_data()).get('current_message_id')
    combined_text = f"⚠️ {warning_text}\n\n{retry_text}"
    success, message_id = await send_to_user_and_topic(user_id, combined_text, state, reply_markup=reply_markup, prev_message_id=prev_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
    else:
        logger.error(f"Ошибка отправки предупреждения пользователю {user_id}")

# Обработчик кнопки "Назад" к главному меню
@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} нажал 'Назад' к главному меню")
    await safe_callback_answer(callback)

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    welcome_message_id = data.get('welcome_message_id')
    logger.debug(f"Извлечены prev_message_id={prev_message_id}, welcome_message_id={welcome_message_id} для {user_id}")

    if prev_message_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=prev_message_id)
            logger.debug(f"Удалено сообщение message_id={prev_message_id} для {user_id}")
        except TelegramBadRequest as e:
            logger.warning(f"Не удалось удалить сообщение {prev_message_id} для {user_id}: {e}")

    first_name = (await bot.get_chat(user_id)).first_name
    welcome_message, keyboard = build_main_menu(first_name)

    if welcome_message_id:
        try:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=welcome_message_id,
                text=welcome_message,
                reply_markup=keyboard
            )
            logger.debug(f"Отредактировано приветствие message_id={welcome_message_id} для {user_id}")
            await state.clear()
            await state.update_data(welcome_message_id=welcome_message_id)
            return
        except TelegramBadRequest as e:
            logger.warning(f"Не удалось отредактировать приветствие {welcome_message_id} для {user_id}: {e}")

    success, message_id = await send_to_user_and_topic(user_id, welcome_message, state, reply_markup=keyboard)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Отправлено новое приветствие message_id={message_id} для {user_id}, состояние сброшено")
    else:
        logger.error(f"Ошибка отправки приветствия для {user_id}")

# Обработчик ввода префикса
@dp.message(UserStates.waiting_for_prefix)
async def process_prefix(message: Message, state: FSMContext):
    user_id = message.from_user.id
    prefix = message.text.strip()
    logger.debug(f"Получен префикс от {user_id}: {prefix}")

    admin_commands = [
        "Просмотреть активные промокоды", "Просмотреть все промокоды", "Создать промокод", 
        "Удалить промокод", "Назначить админа", "Снять админа", "Просмотреть всех админов"
    ]
    if prefix in admin_commands:
        required_level = {
            "Просмотреть активные промокоды": 1,
            "Просмотреть все промокоды": 1,
            "Создать промокод": 1,
            "Удалить промокод": 2,
            "Назначить админа": 3,
            "Снять админа": 3,
            "Просмотреть всех админов": 4
        }
        if not await check_admin_level(user_id, required_level[prefix]):
            text = escape_markdown_v2("У вас нет доступа к этой команде.")
            await send_error_and_return_to_main(user_id, text, state)
            return
        await state.clear()
        if prefix == "Просмотреть активные промокоды":
            await view_activated_promocodes(message)
        elif prefix == "Просмотреть все промокоды":
            await view_all_promocodes(message)
        elif prefix == "Создать промокод":
            await create_promocode_start(message, state)
        elif prefix == "Удалить промокод":
            await remove_promocode_start(message, state)
        elif prefix == "Назначить админа":
            await appoint_admin_start(message, state)
        elif prefix == "Снять админа":
            await remove_admin_start(message, state)
        elif prefix == "Просмотреть всех админов":
            await view_all_admins(message)
        return

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    promocode = data.get('promocode')
    prize_link = data.get('prize_link')
    thread_id = data.get('thread_id')
    logger.debug(f"Извлечён current_message_id={prev_message_id}, promocode={promocode} для {user_id}")

    if promocode:
        # Ветка активации промокода-Префикса — оставлена как было: сырое сообщение
        # с префиксом форвардится в личный топик пользователя.
        if not await forward_to_topic(message, state):
            text = escape_markdown_v2("Ошибка при пересылке сообщения.")
            await send_error_and_return_to_main(user_id, text, state)
            return

    retry_prompt = escape_markdown_v2("Напишите желаемый префикс ещё раз:")
    retry_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_prize_choice" if not promocode else "back_to_main")]
    ])
    if len(prefix) > 16:
        text = escape_markdown_v2("Префикс слишком длинный! Ограничение: до 16 символов.")
        await send_warning_and_retry(user_id, text, retry_prompt, state, reply_markup=retry_keyboard)
        return
    if re.search(r'[\U0001F000-\U0001FFFF]', prefix):
        text = escape_markdown_v2("Эмодзи не допускаются!")
        await send_warning_and_retry(user_id, text, retry_prompt, state, reply_markup=retry_keyboard)
        logger.error(f"Эмодзи в префиксе от {user_id}: {prefix}")
        return
    if re.search(r'(админ|модер|владелка|owner|moder|admin)', prefix, re.IGNORECASE):
        text = escape_markdown_v2("Должности админов не допускаются!")
        await send_warning_and_retry(user_id, text, retry_prompt, state, reply_markup=retry_keyboard)
        logger.warning(f"Попытка использовать админский префикс {prefix} пользователем {user_id}")
        return
    if re.search(r'[^\w\s]', prefix):
        text = escape_markdown_v2("Недопустимые символы. Используйте только буквы, цифры и пробелы.")
        await send_warning_and_retry(user_id, text, retry_prompt, state, reply_markup=retry_keyboard)
        logger.warning(f"Недопустимые символы в префиксе {prefix} от {user_id}")
        return

    if promocode:
        activation_date = datetime.now(MSK).isoformat()
        conn = await db_connect()
        c = await conn.cursor()
        try:
            await c.execute("SELECT type, reward_duration FROM promocodes WHERE code = ?", (promocode,))
            result = await c.fetchone()
            if not result:
                text = escape_markdown_v2("Промокод не найден.")
                await send_error_and_return_to_main(user_id, text, state)
                await conn.close()
                return
            promotype, reward_duration = result
            await c.execute(
                "INSERT INTO user_promocodes (user_id, username, code, activation_date, promotype, selection, reward_duration, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
                (user_id, message.from_user.username, promocode, activation_date, promotype, prefix, reward_duration))
            await c.execute("UPDATE promocodes SET activations_used = activations_used + 1 WHERE code = ?", (promocode,))
            await conn.commit()
        except asyncpg.UniqueViolationError:
            text = escape_markdown_v2("Вы уже активировали этот промокод.")
            await send_error_and_return_to_main(user_id, text, state)
            await conn.close()
            return
        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка базы данных при получении промокода {promocode}: {e}")
            text = escape_markdown_v2("Ошибка базы данных. Попробуйте позже.")
            await send_error_and_return_to_main(user_id, text, state)
            await conn.close()
            return

        reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
        promocodes_topic_id = await ensure_promocodes_topic()
        if promocodes_topic_id:
            promo_text = build_promo_activation_text(
                user_id, message.from_user.username,
                promotype, prefix, promocode, reward_str, activation_date
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Выдано", callback_data=f"promo_ok_{user_id}_{promocode}"),
                 InlineKeyboardButton(text="❌ Отказано", callback_data=f"promo_no_{user_id}_{promocode}")]
            ])
            try:
                sent = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=promocodes_topic_id, text=promo_text, reply_markup=keyboard)
                await c.execute("UPDATE user_promocodes SET card_message_id = ? WHERE user_id = ? AND code = ?", (sent.message_id, user_id, promocode))
                await conn.commit()
                logger.debug(f"Информация об активации промокода {promocode} отправлена в топик Промокоды для {user_id}")
            except TelegramBadRequest as e:
                logger.error(f"Ошибка отправки в топик Промокоды для {user_id}: {e}")
                logger.debug(f"Проблемный текст: {promo_text}")
        await conn.close()
    else:
        # Ветка "Забрать приз" — одна итоговая карточка вместо форварда сырого сообщения
        if not thread_id:
            thread_id = await get_user_thread_id(user_id, state)
        await send_prize_claim_summary(user_id, thread_id, 2, prefix, prize_link, message.from_user)

    text = r"Ваша заявка отправлена администратору\. Ожидайте выдачу префикса ✨" "\n\nДля повторного взаимодействия с ботом отправьте команду /start"
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=prev_message_id)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Заявка на префикс отправлена для {user_id}, welcome_message_id={message_id}")
    else:
        await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке подтверждения префикса. Заявка отправлена, попробуйте снова."), state)

# Обработчик сообщений в топиках
@dp.message(F.chat.id == GROUP_CHAT_ID, F.message_thread_id)
async def handle_topic_message(message: Message, state: FSMContext):
    logger.debug(f"Получено сообщение в топике: chat_id={message.chat.id}, thread_id={message.message_thread_id}, from_user={message.from_user.id}")
    if message.from_user.id not in ADMIN_IDS:
        logger.debug(f"Не админ: {message.from_user.id}")
        return

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT thread_id FROM users WHERE user_id = 0")
    broadcast_thread = await c.fetchone()

    if broadcast_thread and message.message_thread_id == broadcast_thread[0]:
        if message.text.startswith('/msg'):
            await message.reply(escape_markdown_v2("Команда /msg не работает в топике рассылки."))
            await conn.close()
            return

        await c.execute("SELECT user_id, thread_id FROM users WHERE user_id != 0")
        users = await c.fetchall()
        await conn.close()

        if not users:
            await message.reply(escape_markdown_v2("Нет пользователей для рассылки."))
            return
        sent_count = 0
        error_count = 0
        broadcast_text = escape_markdown_v2(message.text)
        for user_id, thread_id in users:
            try:
                success, _ = await send_to_user_and_topic(user_id, broadcast_text, state)
                if success:
                    sent_count += 1
                else:
                    error_count += 1
                await asyncio.sleep(0.05)
            except TelegramBadRequest as e:
                error_count += 1
                await message.reply(escape_markdown_v2(f"Ошибка отправки пользователю {user_id}: {e}"))
        await message.reply(escape_markdown_v2(f"Рассылка завершена. Отправлено: {sent_count}, ошибок: {error_count}."))
        return

    try:
        topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{message.message_thread_id}")
        topic_name = topic.title.strip() if topic.title else None
    except TelegramBadRequest:
        await conn.close()
        return

    match = re.search(r'(.+)\((\d+)\)', topic_name or "")
    user_id = None
    if match:
        user_id = int(match.group(2))
    else:
        await c.execute("SELECT user_id FROM users WHERE thread_id = ?", (message.message_thread_id,))
        result = await c.fetchone()
        if result:
            user_id = result[0]
        await conn.close()

    if not user_id:
        await message.reply(escape_markdown_v2("Топик не связан с пользователем."))
        return

    if message.text.startswith('/msg'):
        msg_text = message.text[4:].strip()
        if not msg_text:
            await message.reply(escape_markdown_v2("Укажите текст после /msg, например: /msg Привет!"))
            return
        admin_message = escape_markdown_v2(f"Сообщение от администратора: {msg_text}")
        success, _ = await send_to_user_and_topic(user_id, admin_message, state)
        if success:
            await message.reply(escape_markdown_v2(f"Сообщение отправлено пользователю {user_id}."))
        else:
            await message.reply(escape_markdown_v2(f"Ошибка отправки пользователю {user_id}. Проверьте блокировку."))

@dp.message(Command('admin'))
async def admin_panel(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет доступа к админ-панели."))
        return

    admin_level = 4 if user_id in ADMIN_IDS else None
    if admin_level is None:
        conn = await db_connect()
        c = await conn.cursor()
        await c.execute("SELECT admin_level FROM users WHERE user_id = ?", (user_id,))
        result = await c.fetchone()
        await conn.close()
        admin_level = result[0] if result else 0

    level_display = escape_markdown_v2(f"{admin_level} (Суперадмин)" if admin_level == 4 else str(admin_level))
    text = (
        f"🛠 *Админ\\-панель* 🛠\n"
        f"👤 *Ваш уровень доступа*: {level_display}\n"
        f"{escape_markdown_v2('━━━━━━━━━━━━━━━━━━━━━')}\n"
        f"📋 *Доступные действия*:\n"
        f"🔎 Выберите действие из меню ниже:\n\n"
        # УДАЛЕН БЛОК С АКТИВНЫМИ ПРОМОКОДАМИ
    )

    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True, keyboard=[])
    keyboard.keyboard.append([
        KeyboardButton(text="Создать промокод"),
        KeyboardButton(text="Просмотреть активные промокоды")
    ])
    if admin_level >= 2:
        keyboard.keyboard.append([KeyboardButton(text="Удалить промокод")])
    if admin_level >= 3:
        keyboard.keyboard.append([
            KeyboardButton(text="Назначить админа"),
            KeyboardButton(text="Снять админа")
        ])
    if admin_level == 4:
        keyboard.keyboard.append([KeyboardButton(text="Просмотреть всех админов")])

    try:
        await message.reply(text, reply_markup=keyboard)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки админ-панели для {user_id}: {e}")
        logger.debug(f"Проблемный текст: {text}")
        await message.reply(escape_markdown_v2("Ошибка при отображении админ-панели. Попробуйте позже."))

@dp.message(F.text == "Создать промокод")
async def create_promocode_start(message: Message, state: FSMContext):
    if not await check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    text = escape_markdown_v2("Выберите тип промокода:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Антитаргет ⚜️", callback_data="promotype_1")],
        [InlineKeyboardButton(text="Префикс 🔰", callback_data="promotype_2")],
        [InlineKeyboardButton(text="Валюта 💵💶🪙", callback_data="promotype_3")]
    ])
    await message.reply(text, reply_markup=keyboard)
    await state.set_state(AdminStates.waiting_for_promocode_type)

@dp.callback_query(F.data.startswith("promotype_"))
async def select_promocode_type(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await check_admin_level(user_id, 1):
        await callback.message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    await safe_callback_answer(callback)
    promotype = int(callback.data.split("_")[1])
    await state.update_data(promotype=promotype)

    if promotype == 3:
        text = escape_markdown_v2("Выберите валюту для этого промокода:")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Евро 💶", callback_data="promocurrency_eur"),
             InlineKeyboardButton(text="Доллары волка 💵", callback_data="promocurrency_usd")],
            [InlineKeyboardButton(text="Монета 🪙", callback_data="promocurrency_coin")]
        ])
        await callback.message.edit_text(text, reply_markup=keyboard)
        await state.set_state(AdminStates.waiting_for_promo_currency)
        return

    text = escape_markdown_v2("Введите время работы промокода (например, 1y, 7d, 24h, 30m или 'без срока'):")
    await callback.message.edit_text(text)
    await state.set_state(AdminStates.waiting_for_validity)

@dp.callback_query(F.data.startswith("promocurrency_"))
async def select_promocode_currency(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not await check_admin_level(user_id, 1):
        await callback.message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    await safe_callback_answer(callback)
    currency_labels = {
        "promocurrency_eur": "Евро 💶",
        "promocurrency_usd": "Доллары волка 💵",
        "promocurrency_coin": "Монета 🪙",
    }
    currency = currency_labels.get(callback.data)
    if not currency:
        return
    await state.update_data(promo_currency=currency)
    text = escape_markdown_v2("Введите время работы промокода (например, 1y, 7d, 24h, 30m или 'без срока'):")
    await callback.message.edit_text(text)
    await state.set_state(AdminStates.waiting_for_validity)

@dp.message(AdminStates.waiting_for_validity)
async def add_promocode_validity(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    validity_input = message.text.strip().lower()
    activation_deadline = parse_duration(validity_input)
    if activation_deadline is False:
        await message.reply(
            escape_markdown_v2("Неверный формат времени. Используйте, например, 1y, 7d, 24h, 30m или 'без срока':")
        )
        return
    await state.update_data(activation_deadline=activation_deadline)
    await message.reply(
        escape_markdown_v2("Введите количество дней действия награды (например, 1y, 7d, 24h, 30m или 'без срока'):")
    )
    await state.set_state(AdminStates.waiting_for_reward_duration)

@dp.message(AdminStates.waiting_for_reward_duration)
async def add_promocode_reward_duration(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    reward_input = message.text.strip().lower()
    reward_duration = parse_duration(reward_input, is_reward=True)
    if reward_duration is False:
        await message.reply(
            escape_markdown_v2("Неверный формат дней. Используйте, например, 1y, 7d, 24h, 30m или 'без срока'):")
        )
        return
    await state.update_data(reward_duration=reward_duration)
    await message.reply(
        escape_markdown_v2("Введите количество активаций (целое число, например, 3):")
    )
    await state.set_state(AdminStates.waiting_for_activations)

@dp.message(AdminStates.waiting_for_activations)
async def add_promocode_activations(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."))
        return
    try:
        activations_limit = int(message.text.strip())
        if activations_limit <= 0:
            raise ValueError
    except ValueError:
        await message.reply(
            escape_markdown_v2("Введите положительное целое число для количества активаций:")
        )
        return

    data = await state.get_data()
    promotype = data.get('promotype')
    activation_deadline = data.get('activation_deadline')
    reward_duration = data.get('reward_duration')
    promo_currency = data.get('promo_currency')
    promocode = generate_promocode(promotype)

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute('INSERT INTO promocodes (code, type, active, activation_deadline, reward_duration, activations_limit, activations_used, currency_choice) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
              (promocode, promotype, True, activation_deadline, reward_duration, activations_limit, 0, promo_currency))
    await conn.commit()
    await conn.close()

    promotype_str = promotype_label(promotype)
    deadline_str = format_datetime(activation_deadline)
    reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
    text_lines = [
        "✅ *Промокод создан*",
        f"Промокод: `{escape_markdown_v2(promocode)}`",
        f"Тип: *{escape_markdown_v2(promotype_str)}*",
    ]
    if promo_currency:
        text_lines.append(f"Выбор: {escape_markdown_v2(promo_currency)}")
    text_lines.append(f"Активен до: `{escape_markdown_v2(deadline_str)}`")
    text_lines.append(f"Срок награды: `{escape_markdown_v2(reward_str)}`")
    text_lines.append(f"Доступно активаций: `{activations_limit}`")
    text = "\n".join(text_lines)
    await message.reply(text)
    await state.clear()

@dp.message(F.text == "Удалить промокод")
async def remove_promocode_start(message: Message, state: FSMContext):
    if not await check_admin_level(message.from_user.id, 2):
        await message.reply(escape_markdown_v2("У вас нет прав для удаления промокодов."))
        return
    await message.reply(escape_markdown_v2("Введите промокод для удаления:"))
    await state.set_state(AdminStates.waiting_for_remove_code)

@dp.message(AdminStates.waiting_for_remove_code)
async def remove_promocode(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not await check_admin_level(user_id, 2):
        await message.reply(escape_markdown_v2("У вас нет прав для удаления промокодов."))
        return
    promocode = message.text.strip()

    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT code FROM promocodes WHERE code = ?", (promocode,))
    exists = await c.fetchone()
    if not exists:
        await message.reply(escape_markdown_v2(f"Промокод {promocode} не существует."))
        await state.clear()
        await conn.close()
        return

    await c.execute("DELETE FROM promocodes WHERE code = ?", (promocode,))
    await conn.commit()
    await conn.close()
    await message.reply(escape_markdown_v2(f"Промокод {promocode} удалён."))
    await state.clear()

@dp.message(F.text == "Просмотреть активные промокоды")
async def view_activated_promocodes(message: Message):
    if not await check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра промокодов."))
        return
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT code, type, activation_deadline, reward_duration, activations_limit, activations_used, currency_choice FROM promocodes WHERE active = true")
    promocodes = await c.fetchall()
    await conn.close()

    if not promocodes:
        await message.reply(escape_markdown_v2("Нет активных промокодов."))
        return

    response = "📋 *Активные промокоды*:\n\n"
    for code, promotype, deadline, reward, limit, used, currency_choice in promocodes:
        promotype_str = promotype_label(promotype)
        deadline_text = format_datetime(deadline)
        reward_text = "Без срока" if reward == 0 else f"{reward} часов"
        response += (
            f"🔹 *Код*: `{escape_markdown_v2(code)}`\n"
            f"   Тип: `{escape_markdown_v2(promotype_str)}`\n"
        )
        if currency_choice:
            response += f"   Выбор: `{escape_markdown_v2(currency_choice)}`\n"
        response += (
            f"   Доступен до: `{escape_markdown_v2(deadline_text)}`\n"
            f"   Срок награды: `{escape_markdown_v2(reward_text)}`\n"
            f"   Активаций: `{used}/{limit}`\n\n"
        )

    try:
        await message.reply(response)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки списка промокодов: {e}")
        logger.debug(f"Проблемный текст: {response}")
        await message.reply(escape_markdown_v2("Ошибка при отображении промокодов. Попробуйте позже."))

@dp.message(F.text == "Просмотреть все промокоды")
async def view_all_promocodes(message: Message):
    if not await check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра промокодов."))
        return
    conn = await db_connect()
    c = await conn.cursor()
    await c.execute("SELECT code, type, active, activation_deadline, reward_duration, activations_limit, activations_used, currency_choice FROM promocodes")
    results = await c.fetchall()
    await conn.close()

    if not results:
        await message.reply(escape_markdown_v2("Нет созданных промокодов."))
        return

    response = "📋 *Все промокоды*:\n\n"
    for code, promotype, active, deadline, reward, limit, used, currency_choice in results:
        is_expired = False
        if deadline and deadline.lower() not in ['без срока', 'без срока']:
            try:
                is_expired = datetime.fromisoformat(deadline.replace('Z', '+00:00')) < datetime.now(MSK)
            except ValueError as e:
                is_expired = True
        status = "Активен" if active and not is_expired else "Неактивен"
        promotype_str = promotype_label(promotype)
        deadline_text = format_datetime(deadline)
        reward_text = "Без срока" if reward == 0 else f"{reward} часов"
        response += (
            f"🔹 *Код*: `{escape_markdown_v2(code)}`\n"
            f"   Тип: `{escape_markdown_v2(promotype_str)}`\n"
        )
        if currency_choice:
            response += f"   Выбор: `{escape_markdown_v2(currency_choice)}`\n"
        response += (
            f"   Статус: `{escape_markdown_v2(status)}`\n"
            f"   Доступен до: `{escape_markdown_v2(deadline_text)}`\n"
            f"   Срок награды: `{escape_markdown_v2(reward_text)}`\n"
            f"   Активаций: `{used}/{limit}`\n\n"
        )
    await message.reply(response)

# Fallback-обработчик для сообщений в ЛС
@dp.message(F.chat.type == "private")
async def handle_private_message(message: Message, state: FSMContext):
    user_id = message.from_user.id
    content = message.text or "не текст (например, фото/стикер)"
    logger.debug(f"Получено сообщение в ЛС от {user_id}: {content}")

    if message.text and message.text.startswith('/'):
        return

    # Больше не форвардим в топик всё подряд вне сценариев — только ссылка на
    # победителей и выбор приза попадают туда, и то одной итоговой карточкой.
    if await state.get_state() is None:
        logger.debug(f"Сообщение вне сценария от {user_id} проигнорировано (не форвардится): {content}")

# Запуск бота
async def main():
    try:
        await init_db_pool()  # Подключение к PostgreSQL
        await init_db()  # Инициализация схемы базы данных
        if not await check_bot_permissions():
            logger.warning("Бот не имеет необходимых прав, но продолжает работу по вашему указанию")
            # Не прерываем выполнение
        if not await ensure_broadcast_topic():
            logger.error("Не удалось создать топик рассылки")
            return
        if not await ensure_promocodes_topic():
            logger.error("Не удалось создать топик Промокоды")
            return
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()
        await close_db_pool()

if __name__ == "__main__":
    import sys
    if sys.version_info < (3, 7):
        print("Python 3.7+ требуется")
        sys.exit(1)
    asyncio.run(main())