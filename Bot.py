import asyncio
import sqlite3
import string
import random
from datetime import datetime, timedelta
import re
import logging
import aiogram
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest

# Логирование
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info(f"Версия aiogram: {aiogram.__version__}")

# Настройки бота
API_TOKEN = 'BOT_TOKEN'
ADMIN_IDS = [7233257134]
GROUP_CHAT_ID = -1003742575858
BROADCAST_TOPIC_NAME = "Рассылка"
PROMOCODES_TOPIC_NAME = "Промокоды"

bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN_V2))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

def init_db():
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    
    # Создание таблицы promocodes
    c.execute('''CREATE TABLE IF NOT EXISTS promocodes
                 (code TEXT PRIMARY KEY, type INTEGER, active BOOLEAN, activation_deadline TEXT, 
                  reward_duration INTEGER, activations_limit INTEGER, activations_used INTEGER)''')
    
    # Создание таблицы user_promocodes
    c.execute('''CREATE TABLE IF NOT EXISTS user_promocodes
                 (user_id INTEGER, username TEXT, code TEXT, activation_date TEXT,
                  PRIMARY KEY (user_id, code))''')
    
    # Создание таблицы users
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT,
                  language_code TEXT, is_premium BOOLEAN, thread_id INTEGER, last_updated TEXT)''')
    
    # Проверка и добавление столбца admin_level, если он отсутствует
    c.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in c.fetchall()]
    if 'admin_level' not in columns:
        c.execute("ALTER TABLE users ADD COLUMN admin_level INTEGER DEFAULT 0")
        logger.info("Добавлен столбец admin_level в таблицу users")
    
    conn.commit()
    conn.close()

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
    now = datetime.now()
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
def check_admin_level(user_id: int, required_level: int) -> bool:
    if user_id in ADMIN_IDS:
        return True  # Уровень 4 (ADMIN_IDS) имеет полный доступ
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT admin_level FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result and result[0] >= required_level

# Форматирование времени для вывода
def format_datetime(iso_str: str | None) -> str:
    logger.debug(f"Обработка даты: {iso_str}")
    if iso_str is None or iso_str == "Без срока":
        return "Без срока"
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")  # НЕ экранируем!
    except ValueError as e:
        logger.error(f"Неверный формат даты: {iso_str}, ошибка: {e}")
        return "Ошибка даты"

def generate_promocode(promotype: int) -> str:
    characters = string.ascii_uppercase + string.digits
    code = f"{promotype}" + ''.join(random.choice(characters) for _ in range(14))
    return code

async def ensure_promocodes_topic():
    logger.debug("Проверка топика Промокоды")
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = ?", (-1,))
    result = c.fetchone()

    if result and result[0]:
        thread_id = result[0]
        try:
            test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=escape_markdown_v2("Проверка топика"))
            await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
            logger.debug(f"Топик Промокоды активен: thread_id={thread_id}")
            conn.close()
            return thread_id
        except TelegramBadRequest as e:
            logger.error(f"Топик Промокоды неактивен (thread_id={thread_id}): {e}")
            c.execute("DELETE FROM users WHERE user_id = ?", (-1,))
            conn.commit()
            conn.close()

    try:
        test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, text=escape_markdown_v2("Проверка топика Промокоды"))
        if test_msg.message_thread_id:
            thread_id = test_msg.message_thread_id
            topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{thread_id}")
            if topic.title and topic.title.strip().lower() == PROMOCODES_TOPIC_NAME.lower():
                conn = sqlite3.connect('promocodes.db')
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?)",
                          (-1, PROMOCODES_TOPIC_NAME, thread_id, datetime.now().isoformat()))
                conn.commit()
                conn.close()
                await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
                logger.debug(f"Найден существующий топик Промокоды: thread_id={thread_id}")
                return thread_id
        await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка проверки существующего топика Промокоды: {e}")

    try:
        topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=PROMOCODES_TOPIC_NAME)
        thread_id = topic.message_thread_id
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?)",
                  (-1, PROMOCODES_TOPIC_NAME, thread_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        logger.debug(f"Создан новый топик Промокоды: thread_id={thread_id}")
        return thread_id
    except TelegramBadRequest as e:
        logger.error(f"Ошибка создания топика Промокоды: {e}")
        return None

# Проверка/создание топика для рассылки
async def ensure_broadcast_topic():
    logger.debug("Проверка топика рассылки")
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = ?", (0,))
    result = c.fetchone()

    if result and result[0]:
        thread_id = result[0]
        try:
            test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=escape_markdown_v2("Проверка топика"))
            await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
            logger.debug(f"Топик рассылки активен: thread_id={thread_id}")
            conn.close()
            return True
        except TelegramBadRequest as e:
            logger.error(f"Топик рассылки неактивен (thread_id={thread_id}): {e}")
            c.execute("DELETE FROM users WHERE user_id = ?", (0,))
            conn.commit()
            conn.close()

    try:
        test_msg = await bot.send_message(chat_id=GROUP_CHAT_ID, text=escape_markdown_v2("Проверка топика рассылки"))
        if test_msg.message_thread_id:
            thread_id = test_msg.message_thread_id
            topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{thread_id}")
            if topic.title and topic.title.strip().lower() == BROADCAST_TOPIC_NAME.lower():
                conn = sqlite3.connect('promocodes.db')
                c = conn.cursor()
                c.execute("INSERT OR REPLACE INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?)",
                          (0, BROADCAST_TOPIC_NAME, thread_id, datetime.now().isoformat()))
                conn.commit()
                conn.close()
                await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
                logger.debug(f"Найден существующий топик рассылки: thread_id={thread_id}")
                return True
        await bot.delete_message(chat_id=GROUP_CHAT_ID, message_id=test_msg.message_id)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка проверки существующего топика: {e}")

    try:
        topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=BROADCAST_TOPIC_NAME)
        thread_id = topic.message_thread_id
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO users (user_id, username, thread_id, last_updated) VALUES (?, ?, ?, ?)",
                  (0, BROADCAST_TOPIC_NAME, thread_id, datetime.now().isoformat()))
        conn.commit()
        conn.close()
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
    last_updated = datetime.now().isoformat()
    new_name = f"{first_name} ({user_id})"
    admin_level = 4 if user_id in ADMIN_IDS else None
    logger.debug(f"Обновление данных пользователя {user_id}: {new_name}")

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    try:
        c.execute("SELECT first_name, thread_id, admin_level FROM users WHERE user_id = ?", (user_id,))
        existing = c.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при запросе пользователя {user_id}: {e}")
        conn.close()
        return None

    current_thread_id = thread_id or (existing[1] if existing else None)
    old_first_name = existing[0] if existing else None
    old_name = f"{old_first_name} ({user_id})" if existing else None
    admin_level = admin_level if admin_level is not None else (existing[2] if existing else 0)

    try:
        c.execute('''INSERT OR REPLACE INTO users
                     (user_id, username, first_name, last_name, language_code, is_premium, thread_id, last_updated, admin_level)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (user_id, username, first_name, last_name, language_code, is_premium, current_thread_id, last_updated, admin_level))
        conn.commit()
        logger.debug(f"Пользователь {user_id} успешно добавлен/обновлён в базе данных")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при сохранении пользователя {user_id}: {e}")
        conn.close()
        return None

    conn.close()

    if current_thread_id and old_name != new_name:
        try:
            await bot.edit_forum_topic(chat_id=GROUP_CHAT_ID, message_thread_id=current_thread_id, name=new_name)
            logger.debug(f"Топик {current_thread_id} обновлен: {new_name}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка обновления топика {current_thread_id}: {e}")
            try:
                topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=new_name)
                new_thread_id = topic.message_thread_id
                conn = sqlite3.connect('promocodes.db')
                c = conn.cursor()
                c.execute("UPDATE users SET thread_id = ? WHERE user_id = ?", (new_thread_id, user_id))
                conn.commit()
                conn.close()
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
        try:
            test_text = escape_markdown_v2("Тест сообщения")
            test_msg = await bot.send_message(
                chat_id=GROUP_CHAT_ID,
                text=test_text,
                parse_mode=ParseMode.MARKDOWN_V2
            )
            edit_text = escape_markdown_v2("Тест сообщения (отредактировано)")
            await bot.edit_message_text(
                chat_id=GROUP_CHAT_ID,
                message_id=test_msg.message_id,
                text=edit_text,
                parse_mode=ParseMode.MARKDOWN_V2
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
    waiting_for_validity = State()
    waiting_for_reward_duration = State()
    waiting_for_activations = State()
    waiting_for_remove_code = State()
    waiting_for_admin_id = State()  # Для назначения админа
    waiting_for_admin_level = State()  # Для выбора уровня админа
    waiting_for_remove_admin_id = State()  # Для снятия админа

# Функция для отправки сообщений в ЛС и топик
async def send_to_user_and_topic(user_id: int, text: str, state: FSMContext, reply_markup=None, is_admin_msg: bool = False, prev_message_id: int = None):
    logger.debug(f"Отправка сообщения пользователю {user_id}, prev_message_id={prev_message_id}, text: {text[:50]}...")
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    if not result:
        logger.error(f"Пользователь {user_id} не найден в базе")
        return False, None
    thread_id = result[0]

    if prev_message_id:
        try:
            await bot.delete_message(chat_id=user_id, message_id=prev_message_id)
            logger.debug(f"Успешно удалено сообщение {prev_message_id} в ЛС {user_id}")
        except TelegramBadRequest as e:
            logger.warning(f"Ошибка удаления сообщения {prev_message_id} в ЛС {user_id}: {e}")

    ls_success = False
    ls_message_id = None
    try:
        msg = await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        ls_message_id = msg.message_id
        logger.debug(f"Сообщение отправлено в ЛС {user_id}, message_id={ls_message_id}")
        ls_success = True
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки в ЛС {user_id}: {e}")
        return False, None

    topic_success = False
    if not thread_id:
        try:
            first_name = (await bot.get_chat(user_id)).first_name
            topic = await bot.create_forum_topic(chat_id=GROUP_CHAT_ID, name=f"{first_name} ({user_id})")
            thread_id = topic.message_thread_id
            conn = sqlite3.connect('promocodes.db')
            c = conn.cursor()
            c.execute("UPDATE users SET thread_id = ? WHERE user_id = ?", (thread_id, user_id))
            conn.commit()
            conn.close()
            await state.update_data(thread_id=thread_id)
            logger.debug(f"Создан топик {thread_id} для {user_id}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка создания топика для {user_id}: {e}")
            return False, None

    try:
        topic_text = f"[Бот] {text}" if not is_admin_msg else text
        await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=topic_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
        logger.debug(f"Сообщение отправлено в топик {thread_id}")
        topic_success = True
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки в топик {thread_id}: {e}")
        try:
            await bot.send_message(chat_id=GROUP_CHAT_ID, text=f"[Бот для {user_id}] {text}", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN_V2)
            logger.debug(f"Сообщение отправлено в общий чат для {user_id}")
            topic_success = True
        except TelegramBadRequest as e:
            logger.error(f"Ошибка отправки в общий чат для {user_id}: {e}")

    return ls_success or topic_success, ls_message_id

async def get_user_thread_id(user_id: int, state: FSMContext) -> int | None:
    """Извлекает thread_id из состояния или базы данных."""
    data = await state.get_data()
    thread_id = data.get('thread_id')
    if thread_id:
        logger.debug(f"thread_id={thread_id} извлечён из состояния для {user_id}")
        return thread_id

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
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
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        c.execute("UPDATE users SET thread_id = ? WHERE user_id = ?", (thread_id, user_id))
        conn.commit()
        conn.close()
        await state.update_data(thread_id=thread_id)
        logger.debug(f"Создан новый топик {thread_id} для {user_id}")
        return thread_id
    except TelegramBadRequest as e:
        logger.error(f"Ошибка создания топика для {user_id}: {e}")
        return None

# Функция для пересылки сообщений пользователя
async def forward_to_topic(message: Message, state: FSMContext):
    user_id = message.from_user.id
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
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
            await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при создании топика."), parse_mode=ParseMode.MARKDOWN_V2)
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
            await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при пересылке сообщения."), parse_mode=ParseMode.MARKDOWN_V2)
            return False

@dp.message(lambda message: message.text == "Назначить админа")
async def appoint_admin_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для назначения админов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    text = escape_markdown_v2("Введите ID пользователя, которого хотите назначить админом:")
    await message.reply(text, parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_admin_id)

@dp.message(AdminStates.waiting_for_admin_id)
async def process_admin_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для назначения админов."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        return
    try:
        new_admin_id = int(message.text.strip())
    except ValueError:
        await message.reply(escape_markdown_v2("Введите корректный ID пользователя (число)."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    if new_admin_id in ADMIN_IDS:
        await message.reply(escape_markdown_v2("Этот пользователь имеет максимальный уровень доступа и не может быть изменён."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        return
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE user_id = ?", (new_admin_id,))
    if not c.fetchone():
        await message.reply(escape_markdown_v2("Пользователь не найден в базе. Он должен сначала взаимодействовать с ботом."), parse_mode=ParseMode.MARKDOWN_V2)
        conn.close()
        return
    conn.close()
    await state.update_data(new_admin_id=new_admin_id)
    max_level = 4 if user_id in ADMIN_IDS else 3
    text = escape_markdown_v2(f"Введите уровень админа (1-{max_level}):")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=str(i), callback_data=f"admin_level_{i}") for i in range(1, max_level + 1)]
    ])
    await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_admin_level)

@dp.callback_query(lambda c: c.data.startswith("admin_level_"))
async def process_admin_level(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not check_admin_level(user_id, 3):
        await callback.message.reply(escape_markdown_v2("У вас нет прав для назначения админов."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        return
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")
    admin_level = int(callback.data.split("_")[2])
    
    # Запрет назначения уровня 4
    if admin_level == 4:
        await callback.message.reply(
            escape_markdown_v2("Уровень 4 доступен только для предустановленных администраторов."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await state.clear()
        return
        
    # Проверка прав для назначения уровня 3
    is_superadmin = user_id in ADMIN_IDS
    if admin_level == 3 and not is_superadmin:
        await callback.message.reply(
            escape_markdown_v2("Только суперадмины могут назначать уровень 3."),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        await state.clear()
        return
        
    data = await state.get_data()
    new_admin_id = data.get('new_admin_id')
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("UPDATE users SET admin_level = ? WHERE user_id = ?", (admin_level, new_admin_id))
    conn.commit()
    conn.close()
    await callback.message.reply(
        escape_markdown_v2(f"Пользователь {new_admin_id} назначен админом уровня {admin_level}."),
        parse_mode=ParseMode.MARKDOWN_V2
    )
    await state.clear()

@dp.message(lambda message: message.text == "Снять админа")
async def remove_admin_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для снятия админов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    text = escape_markdown_v2("Введите ID пользователя, с которого хотите снять админ-права:")
    await message.reply(text, parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_remove_admin_id)

@dp.message(AdminStates.waiting_for_remove_admin_id)
async def process_remove_admin_id(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 3):
        await message.reply(escape_markdown_v2("У вас нет прав для снятия админов."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        return
    try:
        remove_admin_id = int(message.text.strip())
    except ValueError:
        await message.reply(escape_markdown_v2("Введите корректный ID пользователя (число)."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    if remove_admin_id in ADMIN_IDS:
        await message.reply(escape_markdown_v2("Нельзя снять права с суперадмина."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        return
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT admin_level FROM users WHERE user_id = ?", (remove_admin_id,))
    result = c.fetchone()
    if not result or result[0] == 0:
        await message.reply(escape_markdown_v2("Пользователь не является админом."), parse_mode=ParseMode.MARKDOWN_V2)
        conn.close()
        await state.clear()
        return
    c.execute("UPDATE users SET admin_level = 0 WHERE user_id = ?", (remove_admin_id,))
    conn.commit()
    conn.close()
    await message.reply(
        escape_markdown_v2(f"Админ-права сняты с пользователя {remove_admin_id}."),
        parse_mode=ParseMode.MARKDOWN_V2
    )
    await state.clear()

@dp.message(lambda message: message.text == "Просмотреть всех админов")
async def view_all_admins(message: Message):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 4):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра админов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    
    # Получаем обычных админов (исключая суперадминов)
    if ADMIN_IDS:
        placeholders = ','.join('?' * len(ADMIN_IDS))
        c.execute(f"""
            SELECT user_id, first_name, admin_level 
            FROM users 
            WHERE admin_level > 0 
            AND user_id NOT IN ({placeholders})
        """, ADMIN_IDS)
    else:
        c.execute("SELECT user_id, first_name, admin_level FROM users WHERE admin_level > 0")
    admins = c.fetchall()
    
    # Получаем суперадминов
    super_admins = []
    for admin_id in ADMIN_IDS:
        c.execute("SELECT first_name FROM users WHERE user_id = ?", (admin_id,))
        result = c.fetchone()
        first_name = result[0] if result else "Неизвестно"
        super_admins.append((admin_id, first_name))
    
    conn.close()

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
        await message.reply(response, parse_mode=ParseMode.MARKDOWN_V2)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки списка админов: {e}")
        # Попробуем отправить без форматирования
        plain_response = re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', response)
        await message.reply(plain_response, parse_mode=ParseMode.MARKDOWN_V2)

# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message, state: FSMContext):
    user_id = message.from_user.id
    first_name = escape_markdown_v2(message.from_user.first_name)
    logger.debug(f"Получена команда /start от {user_id}: {first_name}")
    thread_id = await update_user_data(message)
    welcome_message = (
        f"Привет, `{first_name}`\\! 👋🏻\n"  # Имя теперь моноширинное
        f"Это бот\\-помощник для выдачи наград в чате 𝐁𝐨𝐧𝐝𝐚𝐠𝐞 𝐌𝐚𝐟𝐢𝐚 🖤\n"
        f"__Для дальнейшего взаимодействия выбери удобный пункт ниже:__"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Забрать приз", callback_data="get_prize")],
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promocode")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, welcome_message, state, reply_markup=keyboard, prev_message_id=None)
    if success:
        await state.update_data(welcome_message_id=message_id, thread_id=thread_id)
        logger.debug(f"Приветствие отправлено для {user_id}, welcome_message_id={message_id}, thread_id={thread_id}")
        if thread_id:
            topic_text = escape_markdown_v2(f"[Бот] Пользователь начал взаимодействие.")
            await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=thread_id, text=topic_text, parse_mode=ParseMode.MARKDOWN_V2)
    else:
        logger.error(f"Ошибка отправки приветствия для {user_id}")

# Обработчик выбора "Забрать приз"
@dp.callback_query(lambda c: c.data == "get_prize")
async def get_prize(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} выбрал 'Забрать приз'")
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")

    data = await state.get_data()
    welcome_message_id = data.get('welcome_message_id')
    logger.debug(f"Извлечён welcome_message_id={welcome_message_id} для {user_id}")

    text = escape_markdown_v2("Отправьте текст/ссылку на сообщение с победителями режима/ивента:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=welcome_message_id)
    if success:
        await state.clear()
        await state.update_data(current_message_id=message_id)
        await state.set_state(UserStates.waiting_for_prize_data)
        logger.debug(f"Установлено состояние waiting_for_prize_data, current_message_id={message_id} для {user_id}")
    else:
        logger.error(f"Ошибка отправки сообщения для {user_id}")
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."), parse_mode=ParseMode.MARKDOWN_V2)

# Обработчик ввода победителей
@dp.message(UserStates.waiting_for_prize_data)
async def process_prize_data(message: Message, state: FSMContext):
    user_id = message.from_user.id
    logger.debug(f"Получены данные о победителях от {user_id}: {message.text}")
    if not await forward_to_topic(message, state):
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при пересылке."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    logger.debug(f"Извлечён current_message_id={prev_message_id} для {user_id}")

    text = escape_markdown_v2("Теперь укажите желаемый приз:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Антитаргет", callback_data="select_prize_antitarget")],
        [InlineKeyboardButton(text="Префикс", callback_data="select_prize_prefix")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_main")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, text, state, reply_markup=keyboard, prev_message_id=prev_message_id)
    if success:
        await state.clear()
        await state.update_data(current_message_id=message_id)
        logger.debug(f"Отправлен запрос выбора приза, current_message_id={message_id} для {user_id}")
    else:
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."), parse_mode=ParseMode.MARKDOWN_V2)

@dp.callback_query(lambda c: c.data in ["select_prize_antitarget", "select_prize_prefix"])
async def handle_prize_selection(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} выбрал приз: {callback.data}")
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    logger.debug(f"Извлечён prev_message_id={prev_message_id} для {user_id}")

    # Извлекаем thread_id
    thread_id = await get_user_thread_id(user_id, state)
    if not thread_id:
        text = escape_markdown_v2("Ошибка: не удалось определить топик. Начните заново.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    if callback.data == "select_prize_antitarget":
        # Пересылаем выбор Антитаргета в топик
        try:
            await bot.send_message(
                chat_id=GROUP_CHAT_ID,
                message_thread_id=thread_id,
                text=escape_markdown_v2(f"Пользователь выбрал приз: Антитаргет"),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            logger.debug(f"Выбор Антитаргета переслано в топик {thread_id} для {user_id}")
        except TelegramBadRequest as e:
            logger.error(f"Ошибка пересылки выбора Антитаргета в топик {thread_id}: {e}")

        text = r"Заявка на Антитаргет отправлена администраторам\. Ожидайте выдачу приза ✨"
        success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=prev_message_id)
        if success:
            await state.clear()
            await state.update_data(welcome_message_id=message_id)
            logger.debug(f"Заявка на Антитаргет отправлена для {user_id}, welcome_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке заявки. Заявка отправлена, попробуйте снова."), state)

    elif callback.data == "select_prize_prefix":
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
            await state.clear()
            await state.update_data(current_message_id=message_id, promocode=None, thread_id=thread_id)
            await state.set_state(UserStates.waiting_for_prefix)
            logger.debug(f"Запрос префикса отправлен для {user_id}, current_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке запроса префикса."), state)

# Обработчик выбора "Ввести промокод"
@dp.callback_query(lambda c: c.data == "enter_promocode")
async def enter_promocode(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        pass

    data = await state.get_data()
    welcome_message_id = data.get('welcome_message_id')

    text = escape_markdown_v2("Введите промокод:")
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=welcome_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
        await state.set_state(UserStates.waiting_for_promocode)
    else:
        await bot.send_message(chat_id=user_id, text=escape_markdown_v2("Ошибка при запросе."), parse_mode=ParseMode.MARKDOWN_V2)

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
        if not check_admin_level(user_id, required_level[promocode]):
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

    if not re.match(r'^[12][A-Z0-9]{14}$', promocode):
        text = escape_markdown_v2("Неверный формат промокода. Должно быть 15 символов, начиная с 1 или 2.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT type, active, activation_deadline, reward_duration, activations_limit, activations_used FROM promocodes WHERE code = ?", (promocode,))
    result = c.fetchone()
    if not result:
        text = escape_markdown_v2("Промокод не найден.")
        await send_error_and_return_to_main(user_id, text, state)
        conn.close()
        return
    promotype, active, activation_deadline, reward_duration, activations_limit, activations_used = result

    if not active:
        text = escape_markdown_v2("Промокод неактивирован.")
        await send_error_and_return_to_main(user_id, text, state)
        conn.close()
        return
    if activation_deadline and datetime.fromisoformat(activation_deadline.replace('Z', '+00:00')) < datetime.now():
        text = escape_markdown_v2("Срок активации промокода истёк.")
        await send_error_and_return_to_main(user_id, text, state)
        conn.close()
        return
    if activations_used >= activations_limit:
        text = escape_markdown_v2("Лимит активаций исчерпан.")
        await send_error_and_return_to_main(user_id, text, state)
        conn.close()
        return

    c.execute("SELECT user_id FROM user_promocodes WHERE user_id = ? AND code = ?", (user_id, promocode))
    if c.fetchone():
        text = escape_markdown_v2("Вы уже активировали этот промокод.")
        await send_error_and_return_to_main(user_id, text, state)
        conn.close()
        return

    conn.close()

    promotype_str = "Антитаргет" if promotype == 1 else "Префикс"
    reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
    first_name = message.from_user.first_name or "Нет имени"
    username = message.from_user.username or "Нет юзернейма"
    activation_date = datetime.now().isoformat()
    activation_formatted = format_datetime(activation_date)

    if promotype == 1:
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        try:
            c.execute("INSERT INTO user_promocodes (user_id, username, code, activation_date) VALUES (?, ?, ?, ?)",
                      (user_id, message.from_user.username, promocode, activation_date))
            c.execute("UPDATE promocodes SET activations_used = activations_used + 1 WHERE code = ?", (promocode,))
            conn.commit()
        except sqlite3.IntegrityError:
            text = escape_markdown_v2("Вы уже активировали этот промокод.")
            await send_error_and_return_to_main(user_id, text, state)
            conn.close()
            return
        except sqlite3.Error as e:
            logger.error(f"Ошибка базы данных при активации промокода {promocode}: {e}")
            text = escape_markdown_v2("Ошибка базы данных. Попробуйте позже.")
            await send_error_and_return_to_main(user_id, text, state)
            conn.close()
            return
        conn.close()

        promocodes_topic_id = await ensure_promocodes_topic()
        if promocodes_topic_id:
            promo_text = (
                f"@{escape_markdown_v2(username)}\n"
                f"\\(Name: `{escape_markdown_v2(first_name)}` / ID: `{user_id}`\\)\n"
                f"**Промокод**: `{escape_markdown_v2(promocode)}`\n"
                f"**Тип**: {escape_markdown_v2(promotype_str)}, Срок действия: {escape_markdown_v2(reward_str)}\n"
                f"**Активирован**: {escape_markdown_v2(activation_formatted)}"
            )
            try:
                await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=promocodes_topic_id, text=promo_text, parse_mode=ParseMode.MARKDOWN_V2)
                logger.debug(f"Информация об активации промокода {promocode} отправлена в топик Промокоды для {user_id}")
            except TelegramBadRequest as e:
                logger.error(f"Ошибка отправки в топик Промокоды для {user_id}: {e}")
                logger.debug(f"Проблемный текст: {promo_text}")

        text = r"Промокод на Антитаргет активирован\. Ожидайте ответа администратора\!"
        success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=(await state.get_data()).get('current_message_id'))
        if success:
            await state.clear()
            await state.update_data(welcome_message_id=message_id)
            logger.debug(f"Успешная активация Антитаргета для {user_id}, welcome_message_id={message_id}")
        else:
            await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке подтверждения. Промокод активирован, попробуйте еще раз."), state)
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

@dp.callback_query(lambda c: c.data in ["request_prefix", "what_is_prefix", "back_to_prefix_menu"])
async def handle_prefix_buttons(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")

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
            f"**Префикс** — надпись рядом с ником в чате мафии, "
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
    """Отправляет сообщение об ошибке и возвращает в главное меню."""
    prev_message_id = (await state.get_data()).get('current_message_id')
    success, message_id = await send_to_user_and_topic(user_id, error_text, state, prev_message_id=prev_message_id)
    if success:
        await state.update_data(current_message_id=message_id)
    else:
        logger.error(f"Ошибка отправки сообщения об ошибке пользователю {user_id}")

    # Возврат в главное меню
    first_name = escape_markdown_v2((await bot.get_chat(user_id)).first_name)
    welcome_message = (
        f"Привет, `{first_name}`\\! 👋🏻\n"  # Имя теперь моноширинное
        f"Это бот\\-помощник для выдачи наград в чате 𝐁𝐨𝐧𝐝𝐚𝐠𝐞 𝐌𝐚𝐟𝐢𝐚 🖤\n"
        f"__Для дальнейшего взаимодействия выбери удобный пункт ниже:__"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Забрать приз", callback_data="get_prize")],
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promocode")]
    ])
    success, message_id = await send_to_user_and_topic(user_id, welcome_message, state, reply_markup=keyboard, prev_message_id=message_id)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Возвращено главное меню для {user_id}, welcome_message_id={message_id}")
    else:
        logger.error(f"Ошибка возврата в главное меню для {user_id}")

# Обработчик кнопки "Назад" к главному меню
@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    logger.debug(f"Пользователь {user_id} нажал 'Назад' к главному меню")
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")

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

    first_name = escape_markdown_v2((await bot.get_chat(user_id)).first_name)
    welcome_message = (
        f"Привет, `{first_name}`\\! 👋🏻\n"  # Имя теперь моноширинное
        f"Это бот\\-помощник для выдачи наград в чате 𝐁𝐨𝐧𝐝𝐚𝐠𝐞 𝐌𝐚𝐟𝐢𝐚 🖤\n"
        f"__Для дальнейшего взаимодействия выбери удобный пункт ниже:__"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Забрать приз", callback_data="get_prize")],
        [InlineKeyboardButton(text="Ввести промокод", callback_data="enter_promocode")]
    ])

    if welcome_message_id:
        try:
            await bot.edit_message_text(
                chat_id=user_id,
                message_id=welcome_message_id,
                text=welcome_message,
                reply_markup=keyboard,
                parse_mode=ParseMode.MARKDOWN_V2
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
        if not check_admin_level(user_id, required_level[prefix]):
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

    if not await forward_to_topic(message, state):
        text = escape_markdown_v2("Ошибка при пересылке сообщения.")
        await send_error_and_return_to_main(user_id, text, state)
        return

    data = await state.get_data()
    prev_message_id = data.get('current_message_id')
    promocode = data.get('promocode')
    logger.debug(f"Извлечён current_message_id={prev_message_id}, promocode={promocode} для {user_id}")

    if len(prefix) > 16:
        text = escape_markdown_v2("Префикс слишком длинный! Ограничение: до 16 символов.")
        await send_error_and_return_to_main(user_id, text, state)
        return
    if re.search(r'[\U0001F000-\U0001FFFF]', prefix):
        text = escape_markdown_v2("Эмодзи не допускаются!")
        await send_error_and_return_to_main(user_id, text, state)
        logger.error(f"Эмодзи в префиксе от {user_id}: {prefix}")
        return
    if re.search(r'(админ|модер|владелка|owner|moder|admin)', prefix, re.IGNORECASE):
        text = escape_markdown_v2("Должности админов не допускаются!")
        await send_error_and_return_to_main(user_id, text, state)
        logger.warning(f"Попытка использовать админский префикс {prefix} пользователем {user_id}")
        return
    if re.search(r'[^\w\s]', prefix):
        text = escape_markdown_v2("Недопустимые символы. Используйте только буквы, цифры и пробелы.")
        await send_error_and_return_to_main(user_id, text, state)
        logger.warning(f"Недопустимые символы в префиксе {prefix} от {user_id}")
        return

    if promocode:
        activation_date = datetime.now().isoformat()
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        try:
            c.execute("SELECT type, reward_duration FROM promocodes WHERE code = ?", (promocode,))
            result = c.fetchone()
            if not result:
                text = escape_markdown_v2("Промокод не найден.")
                await send_error_and_return_to_main(user_id, text, state)
                conn.close()
                return
            promotype, reward_duration = result
            promotype_str = "Антитаргет" if promotype == 1 else "Префикс"
            c.execute("INSERT INTO user_promocodes (user_id, username, code, activation_date) VALUES (?, ?, ?, ?)",
                      (user_id, message.from_user.username, promocode, activation_date))
            c.execute("UPDATE promocodes SET activations_used = activations_used + 1 WHERE code = ?", (promocode,))
            conn.commit()
        except sqlite3.IntegrityError:
            text = escape_markdown_v2("Вы уже активировали этот промокод.")
            await send_error_and_return_to_main(user_id, text, state)
            conn.close()
            return
        except sqlite3.Error as e:
            logger.error(f"Ошибка базы данных при получении промокода {promocode}: {e}")
            text = escape_markdown_v2("Ошибка базы данных. Попробуйте позже.")
            await send_error_and_return_to_main(user_id, text, state)
            conn.close()
            return
        conn.close()

        reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
        first_name = message.from_user.first_name or "Нет имени"
        username = message.from_user.username or "Нет юзернейма"
        activation_formatted = format_datetime(activation_date)
        promocodes_topic_id = await ensure_promocodes_topic()
        if promocodes_topic_id:
            promo_text = (
                f"@{escape_markdown_v2(username)}\n"
                f"\\(Name: `{escape_markdown_v2(first_name)}` / ID: `{user_id}`\\)\n"
                f"**Промокод**: `{escape_markdown_v2(promocode)}`\n"
                f"**Тип**: {escape_markdown_v2(promotype_str)}, Срок действия: {escape_markdown_v2(reward_str)}\n"
                f"**Префикс**: `{escape_markdown_v2(prefix)}`\n"
                f"**Активирован**: {escape_markdown_v2(activation_formatted)}"
            )
            try:
                await bot.send_message(chat_id=GROUP_CHAT_ID, message_thread_id=promocodes_topic_id, text=promo_text, parse_mode=ParseMode.MARKDOWN_V2)
                logger.debug(f"Информация об активации промокода {promocode} отправлена в топик Промокоды для {user_id}")
            except TelegramBadRequest as e:
                logger.error(f"Ошибка отправки в топик Промокоды для {user_id}: {e}")
                logger.debug(f"Проблемный текст: {promo_text}")

    text = r"Ваша заявка отправлена администратору\. Ожидайте выдачу префикса ✨"
    success, message_id = await send_to_user_and_topic(user_id, text, state, prev_message_id=prev_message_id)
    if success:
        await state.clear()
        await state.update_data(welcome_message_id=message_id)
        logger.debug(f"Заявка на префикс отправлена для {user_id}, welcome_message_id={message_id}")
    else:
        await send_error_and_return_to_main(user_id, escape_markdown_v2("Ошибка при отправке подтверждения префикса. Заявка отправлена, попробуйте снова."), state)

# Обработчик сообщений в топиках
@dp.message(lambda message: message.chat.id == GROUP_CHAT_ID and message.message_thread_id)
async def handle_topic_message(message: Message, state: FSMContext):
    logger.debug(f"Получено сообщение в топике: chat_id={message.chat.id}, thread_id={message.message_thread_id}, from_user={message.from_user.id}")
    if message.from_user.id not in ADMIN_IDS:
        logger.debug(f"Не админ: {message.from_user.id}")
        return

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT thread_id FROM users WHERE user_id = 0")
    broadcast_thread = c.fetchone()

    if broadcast_thread and message.message_thread_id == broadcast_thread[0]:
        if message.text.startswith('/msg'):
            await message.reply(escape_markdown_v2("Команда /msg не работает в топике рассылки."), parse_mode=ParseMode.MARKDOWN_V2)
            conn.close()
            return

        c.execute("SELECT user_id, thread_id FROM users WHERE user_id != 0")
        users = c.fetchall()
        conn.close()

        if not users:
            await message.reply(escape_markdown_v2("Нет пользователей для рассылки."), parse_mode=ParseMode.MARKDOWN_V2)
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
                await message.reply(escape_markdown_v2(f"Ошибка отправки пользователю {user_id}: {e}"), parse_mode=ParseMode.MARKDOWN_V2)
        await message.reply(escape_markdown_v2(f"Рассылка завершена. Отправлено: {sent_count}, ошибок: {error_count}."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    try:
        topic = await bot.get_chat(chat_id=f"{GROUP_CHAT_ID}#{message.message_thread_id}")
        topic_name = topic.title.strip() if topic.title else None
    except TelegramBadRequest:
        conn.close()
        return

    match = re.search(r'(.+)\((\d+)\)', topic_name or "")
    user_id = None
    if match:
        user_id = int(match.group(2))
    else:
        c.execute("SELECT user_id FROM users WHERE thread_id = ?", (message.message_thread_id,))
        result = c.fetchone()
        if result:
            user_id = result[0]
        conn.close()

    if not user_id:
        await message.reply(escape_markdown_v2("Топик не связан с пользователем."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    if message.text.startswith('/msg'):
        msg_text = message.text[4:].strip()
        if not msg_text:
            await message.reply(escape_markdown_v2("Укажите текст после /msg, например: /msg Привет!"), parse_mode=ParseMode.MARKDOWN_V2)
            return
        admin_message = escape_markdown_v2(f"Сообщение от администратора: {msg_text}")
        success, _ = await send_to_user_and_topic(user_id, admin_message, state, is_admin_msg=True)
        if success:
            await message.reply(escape_markdown_v2(f"Сообщение отправлено пользователю {user_id}."), parse_mode=ParseMode.MARKDOWN_V2)
        else:
            await message.reply(escape_markdown_v2(f"Ошибка отправки пользователю {user_id}. Проверьте блокировку."), parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(Command('admin'))
async def admin_panel(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет доступа к админ-панели."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    admin_level = 4 if user_id in ADMIN_IDS else None
    if admin_level is None:
        conn = sqlite3.connect('promocodes.db')
        c = conn.cursor()
        c.execute("SELECT admin_level FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        conn.close()
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
        await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки админ-панели для {user_id}: {e}")
        logger.debug(f"Проблемный текст: {text}")
        await message.reply(escape_markdown_v2("Ошибка при отображении админ-панели. Попробуйте позже."), parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(lambda message: message.text == "Создать промокод")
async def create_promocode_start(message: Message, state: FSMContext):
    if not check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    text = escape_markdown_v2("Выберите тип промокода:")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Анти-таргет", callback_data="promotype_1")],
        [InlineKeyboardButton(text="Префикс", callback_data="promotype_2")]
    ])
    await message.reply(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_promocode_type)

@dp.callback_query(lambda c: c.data.startswith("promotype_"))
async def select_promocode_type(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not check_admin_level(user_id, 1):
        await callback.message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        await callback.answer()
    except TelegramBadRequest as e:
        logger.error(f"Ошибка ответа на callback для {user_id}: {e}")
    promotype = int(callback.data.split("_")[1])
    await state.update_data(promotype=promotype)
    text = escape_markdown_v2("Введите время работы промокода (например, 1y, 7d, 24h, 30m или 'без срока'):")
    await callback.message.edit_text(text, parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_validity)

@dp.message(AdminStates.waiting_for_validity)
async def add_promocode_validity(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    validity_input = message.text.strip().lower()
    activation_deadline = parse_duration(validity_input)
    if activation_deadline is False:
        await message.reply(
            escape_markdown_v2("Неверный формат времени. Используйте, например, 1y, 7d, 24h, 30m или 'без срока':"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    await state.update_data(activation_deadline=activation_deadline)
    await message.reply(
        escape_markdown_v2("Введите количество дней действия награды (например, 1y, 7d, 24h, 30m или 'без срока'):"),
        parse_mode=ParseMode.MARKDOWN_V2
    )
    await state.set_state(AdminStates.waiting_for_reward_duration)

@dp.message(AdminStates.waiting_for_reward_duration)
async def add_promocode_reward_duration(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    reward_input = message.text.strip().lower()
    reward_duration = parse_duration(reward_input, is_reward=True)
    if reward_duration is False:
        await message.reply(
            escape_markdown_v2("Неверный формат дней. Используйте, например, 1y, 7d, 24h, 30m или 'без срока'):"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return
    await state.update_data(reward_duration=reward_duration)
    await message.reply(
        escape_markdown_v2("Введите количество активаций (целое число, например, 3):"),
        parse_mode=ParseMode.MARKDOWN_V2
    )
    await state.set_state(AdminStates.waiting_for_activations)

@dp.message(AdminStates.waiting_for_activations)
async def add_promocode_activations(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для создания промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    try:
        activations_limit = int(message.text.strip())
        if activations_limit <= 0:
            raise ValueError
    except ValueError:
        await message.reply(
            escape_markdown_v2("Введите положительное целое число для количества активаций:"),
            parse_mode=ParseMode.MARKDOWN_V2
        )
        return

    data = await state.get_data()
    promotype = data.get('promotype')
    activation_deadline = data.get('activation_deadline')
    reward_duration = data.get('reward_duration')
    promocode = generate_promocode(promotype)

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute('INSERT INTO promocodes (code, type, active, activation_deadline, reward_duration, activations_limit, activations_used) VALUES (?, ?, ?, ?, ?, ?, ?)',
              (promocode, promotype, 1, activation_deadline, reward_duration, activations_limit, 0))
    conn.commit()
    conn.close()

    promotype_str = "Анти-таргет" if promotype == 1 else "Префикс"
    deadline_str = format_datetime(activation_deadline)
    reward_str = "Без срока" if reward_duration == 0 else f"{reward_duration} часов"
    text = (
        f"Промокод успешно создан\n"
        f"Промокод: *{escape_markdown_v2(promocode)}*\n"
        f"Тип: {escape_markdown_v2(promotype_str)}\n"
        f"Активен до: {escape_markdown_v2(deadline_str)}\n"
        f"Срок награды: {escape_markdown_v2(reward_str)}\n"
        f"Доступно активаций: {activations_limit}"
    )
    await message.reply(text, parse_mode=ParseMode.MARKDOWN_V2)
    await state.clear()

@dp.message(lambda message: message.text == "Удалить промокод")
async def remove_promocode_start(message: Message, state: FSMContext):
    if not check_admin_level(message.from_user.id, 2):
        await message.reply(escape_markdown_v2("У вас нет прав для удаления промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    await message.reply(escape_markdown_v2("Введите промокод для удаления:"), parse_mode=ParseMode.MARKDOWN_V2)
    await state.set_state(AdminStates.waiting_for_remove_code)

@dp.message(AdminStates.waiting_for_remove_code)
async def remove_promocode(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if not check_admin_level(user_id, 2):
        await message.reply(escape_markdown_v2("У вас нет прав для удаления промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    promocode = message.text.strip()

    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT code FROM promocodes WHERE code = ?", (promocode,))
    exists = c.fetchone()
    if not exists:
        await message.reply(escape_markdown_v2(f"Промокод {promocode} не существует."), parse_mode=ParseMode.MARKDOWN_V2)
        await state.clear()
        conn.close()
        return

    c.execute("DELETE FROM promocodes WHERE code = ?", (promocode,))
    conn.commit()
    conn.close()
    await message.reply(escape_markdown_v2(f"Промокод {promocode} удалён."), parse_mode=ParseMode.MARKDOWN_V2)
    await state.clear()

@dp.message(lambda message: message.text == "Просмотреть активные промокоды")
async def view_activated_promocodes(message: Message):
    if not check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT code, type, activation_deadline, reward_duration, activations_limit, activations_used FROM promocodes WHERE active = 1")
    promocodes = c.fetchall()
    conn.close()

    if not promocodes:
        await message.reply(escape_markdown_v2("Нет активных промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    response = "📋 *Активные промокоды*:\n\n"
    for code, promotype, deadline, reward, limit, used in promocodes:
        promotype_str = "Анти-таргет" if promotype == 1 else "Префикс"
        deadline_text = format_datetime(deadline)
        reward_text = "Без срока" if reward == 0 else f"{reward} часов"
        response += (
            f"🔹 *Код*: `{escape_markdown_v2(code)}`\n"
            f"   Тип: `{escape_markdown_v2(promotype_str)}`\n"
            f"   Доступен до: `{escape_markdown_v2(deadline_text)}`\n"
            f"   Срок награды: `{escape_markdown_v2(reward_text)}`\n"
            f"   Активаций: `{used}/{limit}`\n\n"
        )

    try:
        await message.reply(response, parse_mode=ParseMode.MARKDOWN_V2)
    except TelegramBadRequest as e:
        logger.error(f"Ошибка отправки списка промокодов: {e}")
        logger.debug(f"Проблемный текст: {response}")
        await message.reply(escape_markdown_v2("Ошибка при отображении промокодов. Попробуйте позже."), parse_mode=ParseMode.MARKDOWN_V2)

@dp.message(lambda message: message.text == "Просмотреть все промокоды")
async def view_all_promocodes(message: Message):
    if not check_admin_level(message.from_user.id, 1):
        await message.reply(escape_markdown_v2("У вас нет прав для просмотра промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return
    conn = sqlite3.connect('promocodes.db')
    c = conn.cursor()
    c.execute("SELECT code, type, active, activation_deadline, reward_duration, activations_limit, activations_used FROM promocodes")
    results = c.fetchall()
    conn.close()

    if not results:
        await message.reply(escape_markdown_v2("Нет созданных промокодов."), parse_mode=ParseMode.MARKDOWN_V2)
        return

    response = "📋 *Все промокоды*:\n\n"
    for code, promotype, active, deadline, reward, limit, used in results:
        is_expired = False
        if deadline and deadline.lower() not in ['без срока', 'без срока']:
            try:
                is_expired = datetime.fromisoformat(deadline.replace('Z', '+00:00')) < datetime.now()
            except ValueError as e:
                is_expired = True
        status = "Активен" if active and not is_expired else "Неактивен"
        promotype_str = "Анти-таргет" if promotype == 1 else "Префикс"
        deadline_text = format_datetime(deadline)
        reward_text = "Без срока" if reward == 0 else f"{reward} часов"
        response += (
            f"🔹 *Код*: `{code}`\n"
            f"   Тип: `{promotype_str}`\n"
            f"   Статус: `{status}`\n"
            f"   Доступен до: `{deadline_text}`\n"
            f"   Срок награды: `{reward_text}`\n"
            f"   Активаций: `{used}/{limit}`\n\n"
        )
    await message.reply(escape_markdown_v2(response), parse_mode=ParseMode.MARKDOWN_V2)

# Fallback-обработчик для сообщений в ЛС
@dp.message(lambda message: message.chat.type == "private")
async def handle_private_message(message: Message, state: FSMContext):
    user_id = message.from_user.id
    content = message.text or "не текст (например, фото/стикер)"
    logger.debug(f"Получено сообщение в ЛС от {user_id}: {content}")

    if message.text and message.text.startswith('/'):
        return

    if await state.get_state() is None:
        await forward_to_topic(message, state)

# Запуск бота
async def main():
    try:
        init_db()  # Инициализация базы данных
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

if __name__ == "__main__":
    import sys
    if sys.version_info < (3, 7):
        print("Python 3.7+ требуется")
        sys.exit(1)
    asyncio.run(main())
