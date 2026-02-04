import asyncio
import logging
import random
import re
import json
import sqlite3
import os
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, 
    InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode

# ==================== КОНФИГУРАЦИЯ ====================
BOT_TOKEN = "8435441500:AAHd0oGyPLkHx2lIBDMSCEb1hxRgINtFiYY"
ADMIN_ID = 1736344274
DB_PATH = '/data/bot_database.db' if os.path.exists('/data') else 'bot_database.db'

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

active_tasks = {}

# ==================== БАЗА ДАННЫХ ====================
def get_db():
    """Получить подключение к БД"""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def init_db():
    """Инициализация базы данных"""
    logger.info(f"Initializing database at {DB_PATH}")
    
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS films (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            image_id TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            button_text TEXT NOT NULL,
            link TEXT NOT NULL,
            channel_id TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS texts (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_data TEXT NOT NULL,
            buttons TEXT,
            scheduled_date TEXT NOT NULL,
            scheduled_time TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Дефолтные тексты
    default_texts = {
        'welcome': '''👋 Добро пожаловать, любители аниме, фильмов и сериалов! 🎥

📚 Здесь ты найдешь крупнейшую библиотеку с лучшими аниме, фильмами и сериалами, включая самые свежие новинки!

🔍 Ищешь аниме по коду из Telegram, TikTok, YouTube или Instagram? Этот бот быстро поможет тебе найти нужное!

⚙️ Как пользоваться ботом? <a href="{instruction_link}">Инструкция по поиску здесь</a>.

🍿 Приятного просмотра! Не забудь воспользоваться кнопками ниже для быстрого поиска! ❤️''',
        'subscribe_required': '📝 Для использования бота, Вы должны нажать на все кнопки и подписаться на все каналы\n\nВ канал подавайте только 1 заявку, если отправите несколько заявок - бан',
        'film_not_found': '❌ Фильм с таким кодом не найден.',
        'ad_text': '📢 Для приобретения рекламы напишите админу: @admin',
        'search_prompt': '🔍 Введите код фильма/аниме для поиска:',
        'random_empty': '😔 В базе пока нет фильмов'
    }
    
    for key, value in default_texts.items():
        cursor.execute('INSERT OR IGNORE INTO texts (key, value) VALUES (?, ?)', (key, value))
    
    # Дефолтные настройки
    cursor.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', 
                   ('instruction_link', 'https://t.me/+fsafas34'))
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

# ==================== ФУНКЦИИ БД ====================
def db_execute(query, params=(), fetch=False, fetchone=False):
    """Универсальная функция для работы с БД"""
    conn = None
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(query, params)
        
        if fetchone:
            result = cursor.fetchone()
        elif fetch:
            result = cursor.fetchall()
        else:
            conn.commit()
            result = cursor.lastrowid
        
        return result
    except Exception as e:
        logger.error(f"Database error: {e}")
        return None if fetch or fetchone else 0
    finally:
        if conn:
            conn.close()

def add_user(user_id, username, first_name):
    existing = db_execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,), fetchone=True)
    if existing is None:
        db_execute('INSERT INTO users (user_id, username, first_name, joined_at) VALUES (?, ?, ?, datetime("now"))',
                   (user_id, username, first_name))
    else:
        db_execute('UPDATE users SET username = ?, first_name = ? WHERE user_id = ?',
                   (username, first_name, user_id))

def get_all_users():
    result = db_execute('SELECT user_id FROM users', fetch=True)
    return [row[0] for row in result] if result else []

def get_users_stats():
    stats = {'total': 0, 'today': 0, 'week': 0, 'month': 0}
    
    result = db_execute('SELECT COUNT(*) FROM users', fetchone=True)
    stats['total'] = result[0] if result else 0
    
    result = db_execute('SELECT COUNT(*) FROM users WHERE date(joined_at, "+3 hours") = date("now", "+3 hours")', fetchone=True)
    stats['today'] = result[0] if result else 0
    
    result = db_execute('SELECT COUNT(*) FROM users WHERE datetime(joined_at, "+3 hours") >= datetime("now", "+3 hours", "-7 days")', fetchone=True)
    stats['week'] = result[0] if result else 0
    
    result = db_execute('SELECT COUNT(*) FROM users WHERE datetime(joined_at, "+3 hours") >= datetime("now", "+3 hours", "-30 days")', fetchone=True)
    stats['month'] = result[0] if result else 0
    
    return stats

def add_film(code, name, image_id=None):
    return db_execute('INSERT INTO films (code, name, image_id) VALUES (?, ?, ?)', (code, name, image_id))

def delete_film_by_code(code):
    db_execute('DELETE FROM films WHERE code = ?', (code,))
    return True

def get_film_by_code(code):
    return db_execute('SELECT name, image_id FROM films WHERE code = ?', (code,), fetchone=True)

def get_all_films():
    result = db_execute('SELECT code, name, image_id FROM films', fetch=True)
    return result if result else []

def get_random_film():
    return db_execute('SELECT code, name, image_id FROM films ORDER BY RANDOM() LIMIT 1', fetchone=True)

def get_films_count():
    result = db_execute('SELECT COUNT(*) FROM films', fetchone=True)
    return result[0] if result else 0

def generate_unique_code():
    existing = db_execute('SELECT code FROM films', fetch=True)
    existing_codes = [row[0] for row in existing] if existing else []
    
    for _ in range(100):
        code = str(random.randint(1000, 9999))
        if code not in existing_codes:
            return code
    return str(random.randint(10000, 99999))

def add_channel(button_text, link, channel_id=None):
    db_execute('INSERT INTO channels (button_text, link, channel_id) VALUES (?, ?, ?)', 
               (button_text, link, channel_id))

def clear_all_channels():
    db_execute('DELETE FROM channels')

def delete_channel_by_id(id):
    db_execute('DELETE FROM channels WHERE id = ?', (id,))

def get_all_channels():
    result = db_execute('SELECT id, button_text, link, channel_id FROM channels', fetch=True)
    return result if result else []

def get_text(key):
    result = db_execute('SELECT value FROM texts WHERE key = ?', (key,), fetchone=True)
    return result[0] if result else ''

def update_text(key, value):
    db_execute('INSERT OR REPLACE INTO texts (key, value) VALUES (?, ?)', (key, value))

def get_setting(key):
    result = db_execute('SELECT value FROM settings WHERE key = ?', (key,), fetchone=True)
    return result[0] if result else None

def update_setting(key, value):
    db_execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))

# Рассылки
def save_scheduled_broadcast(message_data, buttons, scheduled_date, scheduled_time):
    return db_execute(
        'INSERT INTO scheduled_broadcasts (message_data, buttons, scheduled_date, scheduled_time, status) VALUES (?, ?, ?, ?, "pending")',
        (json.dumps(message_data), json.dumps(buttons) if buttons else None, scheduled_date, scheduled_time)
    )

def get_pending_broadcasts():
    result = db_execute(
        'SELECT id, message_data, buttons, scheduled_date, scheduled_time, created_at FROM scheduled_broadcasts WHERE status = "pending" ORDER BY scheduled_date, scheduled_time',
        fetch=True
    )
    return result if result else []

def get_broadcast_by_id(broadcast_id):
    return db_execute('SELECT id, message_data, buttons, scheduled_date, scheduled_time, status FROM scheduled_broadcasts WHERE id = ?', 
                      (broadcast_id,), fetchone=True)

def update_broadcast(broadcast_id, message_data=None, buttons=None, scheduled_date=None, scheduled_time=None):
    if message_data is not None:
        db_execute('UPDATE scheduled_broadcasts SET message_data = ? WHERE id = ?', (json.dumps(message_data), broadcast_id))
    if buttons is not None:
        db_execute('UPDATE scheduled_broadcasts SET buttons = ? WHERE id = ?', (json.dumps(buttons) if buttons else None, broadcast_id))
    if scheduled_date is not None:
        db_execute('UPDATE scheduled_broadcasts SET scheduled_date = ? WHERE id = ?', (scheduled_date, broadcast_id))
    if scheduled_time is not None:
        db_execute('UPDATE scheduled_broadcasts SET scheduled_time = ? WHERE id = ?', (scheduled_time, broadcast_id))

def delete_broadcast(broadcast_id):
    db_execute('DELETE FROM scheduled_broadcasts WHERE id = ?', (broadcast_id,))

def mark_broadcast_completed(broadcast_id):
    db_execute('UPDATE scheduled_broadcasts SET status = "completed" WHERE id = ?', (broadcast_id,))

# ==================== FSM СОСТОЯНИЯ ====================
class AdminStates(StatesGroup):
    broadcast_message = State()
    broadcast_buttons = State()
    broadcast_date = State()
    broadcast_time = State()
    broadcast_confirm = State()
    
    edit_broadcast_message = State()
    edit_broadcast_buttons = State()
    edit_broadcast_date = State()
    edit_broadcast_time = State()
    
    add_film_name = State()
    add_film_code = State()
    add_film_image = State()
    add_channels = State()
    edit_text_value = State()
    
    edit_welcome_text = State()
    edit_welcome_image = State()
    edit_instruction_link = State()

class UserStates(StatesGroup):
    waiting_code = State()

# ==================== ПРОВЕРКА ПОДПИСКИ ====================
async def check_subscription(user_id):
    channels = get_all_channels()
    if not channels:
        return True
    
    for channel in channels:
        channel_id = channel[3] if len(channel) > 3 else None
        if not channel_id:
            continue
        
        try:
            member = await bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
            if member.status in ['left', 'kicked']:
                logger.info(f"User {user_id} not subscribed to {channel_id}")
                return False
        except Exception as e:
            logger.error(f"Error checking subscription for {channel_id}: {e}")
            continue
    
    return True

def get_subscribe_keyboard():
    channels = get_all_channels()
    buttons = []
    for channel in channels:
        button_text = channel[1]
        link = channel[2]
        buttons.append([InlineKeyboardButton(text=button_text, url=link)])
    buttons.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== КЛАВИАТУРЫ ====================
def get_user_reply_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Поиск по коду"), KeyboardButton(text="🎲 Случайный код")],
            [KeyboardButton(text="🔥 Купить рекламу в этом боте")]
        ],
        resize_keyboard=True
    )

def get_welcome_inline_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по коду", callback_data="search_code")],
        [InlineKeyboardButton(text="📖 Открыть меню", callback_data="open_menu")]
    ])

def get_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Сделать рассылку", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="📋 Отложенные рассылки", callback_data="admin_scheduled")],
        [InlineKeyboardButton(text="📋 Списки фильмов", callback_data="admin_films_list")],
        [InlineKeyboardButton(text="➕ Добавить фильм", callback_data="admin_add_film"),
         InlineKeyboardButton(text="➖ Удалить фильм", callback_data="admin_delete_film")],
        [InlineKeyboardButton(text="📢 Добавить каналы", callback_data="admin_add_channels")],
        [InlineKeyboardButton(text="🗑 Удалить каналы", callback_data="admin_delete_channels")],
        [InlineKeyboardButton(text="👁 Посмотреть каналы", callback_data="admin_view_channels")],
        [InlineKeyboardButton(text="👋 Приветствие", callback_data="admin_welcome_settings")],
        [InlineKeyboardButton(text="📝 Тексты", callback_data="admin_texts")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")]
    ])

def get_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
    ])

def get_texts_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Требование подписки", callback_data="edit_text_subscribe_required")],
        [InlineKeyboardButton(text="❌ Фильм не найден", callback_data="edit_text_film_not_found")],
        [InlineKeyboardButton(text="📢 Текст рекламы", callback_data="edit_text_ad_text")],
        [InlineKeyboardButton(text="🔍 Текст поиска", callback_data="edit_text_search_prompt")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
    ])

def get_welcome_settings_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Изменить текст", callback_data="edit_welcome_text")],
        [InlineKeyboardButton(text="🖼 Изменить картинку", callback_data="edit_welcome_image")],
        [InlineKeyboardButton(text="🔗 Изменить ссылку", callback_data="edit_instruction_link")],
        [InlineKeyboardButton(text="👁 Предпросмотр", callback_data="preview_welcome")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")]
    ])

# ==================== ПАРСИНГ ====================
def parse_channels_text(text):
    channels = []
    lines = text.strip().split('\n')
    pattern = r'^[\d.]*\s*(.+?)\s*\((https?://[^\)]+)\)\s*(-?\d+)?$'
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        match = re.match(pattern, line)
        if match:
            channels.append({
                'button_text': match.group(1).strip(),
                'link': match.group(2).strip(),
                'channel_id': match.group(3).strip() if match.group(3) else None
            })
    return channels

def parse_buttons_text(text):
    buttons = []
    lines = text.strip().split('\n')
    for line in lines:
        line = line.strip()
        if '|' in line:
            parts = line.split('|', 1)
            if len(parts) == 2:
                btn_text = parts[0].strip()
                btn_url = parts[1].strip()
                if btn_text and btn_url:
                    buttons.append({'text': btn_text, 'url': btn_url})
    return buttons

def create_inline_keyboard_from_buttons(buttons):
    if not buttons:
        return None
    keyboard = [[InlineKeyboardButton(text=btn['text'], url=btn['url'])] for btn in buttons]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def parse_date(date_str):
    date_str = date_str.strip()
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', date_str)
    if match:
        try:
            return datetime(int(match.group(3)), int(match.group(2)), int(match.group(1))).strftime('%Y-%m-%d')
        except:
            return None
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})$', date_str)
    if match:
        year = datetime.now().year
        try:
            target = datetime(year, int(match.group(2)), int(match.group(1)))
            if target.date() < datetime.now().date():
                target = datetime(year + 1, int(match.group(2)), int(match.group(1)))
            return target.strftime('%Y-%m-%d')
        except:
            return None
    
    return None

def format_date_display(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%d.%m.%Y')
    except:
        return date_str

# ==================== РАССЫЛКА ====================
async def do_broadcast(message_data, buttons=None):
    users = get_all_users()
    success = 0
    failed = 0
    keyboard = create_inline_keyboard_from_buttons(buttons) if buttons else None
    
    for user_id in users:
        try:
            if message_data.get('photo'):
                await bot.send_photo(user_id, message_data['photo'], caption=message_data.get('caption'),
                                    reply_markup=keyboard, parse_mode=ParseMode.HTML)
            elif message_data.get('video'):
                await bot.send_video(user_id, message_data['video'], caption=message_data.get('caption'),
                                    reply_markup=keyboard, parse_mode=ParseMode.HTML)
            else:
                await bot.send_message(user_id, message_data['text'], reply_markup=keyboard,
                                      parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            success += 1
        except Exception as e:
            logger.error(f"Broadcast error for {user_id}: {e}")
            failed += 1
        await asyncio.sleep(0.05)
    
    return success, failed

async def send_preview(chat_id, message_data, buttons=None):
    keyboard = create_inline_keyboard_from_buttons(buttons) if buttons else None
    try:
        if message_data.get('photo'):
            await bot.send_photo(chat_id, message_data['photo'],
                               caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('caption', '')}",
                               reply_markup=keyboard, parse_mode=ParseMode.HTML)
        elif message_data.get('video'):
            await bot.send_video(chat_id, message_data['video'],
                               caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('caption', '')}",
                               reply_markup=keyboard, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(chat_id, f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('text', '')}",
                                  reply_markup=keyboard, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка: {e}")

async def scheduled_broadcast_task(broadcast_id):
    broadcast = get_broadcast_by_id(broadcast_id)
    if not broadcast or broadcast['status'] != 'pending':
        return
    
    now_msk = datetime.utcnow() + timedelta(hours=3)
    target = datetime.strptime(f"{broadcast['scheduled_date']} {broadcast['scheduled_time']}", "%Y-%m-%d %H:%M")
    delay = max(0, (target - now_msk).total_seconds())
    
    if delay > 0:
        await asyncio.sleep(delay)
    
    broadcast = get_broadcast_by_id(broadcast_id)
    if not broadcast or broadcast['status'] != 'pending':
        return
    
    message_data = json.loads(broadcast['message_data'])
    buttons = json.loads(broadcast['buttons']) if broadcast['buttons'] else None
    
    success, failed = await do_broadcast(message_data, buttons)
    mark_broadcast_completed(broadcast_id)
    
    if broadcast_id in active_tasks:
        del active_tasks[broadcast_id]
    
    try:
        await bot.send_message(ADMIN_ID, f"✅ Рассылка #{broadcast_id} выполнена!\n📨 Успешно: {success}\n❌ Ошибок: {failed}")
    except:
        pass

def start_broadcast_task(broadcast_id):
    if broadcast_id in active_tasks:
        active_tasks[broadcast_id].cancel()
    active_tasks[broadcast_id] = asyncio.create_task(scheduled_broadcast_task(broadcast_id))

async def restart_pending_broadcasts():
    for broadcast in get_pending_broadcasts():
        start_broadcast_task(broadcast['id'])

# ==================== ПРИВЕТСТВИЕ ====================
def get_welcome_text():
    text = get_text('welcome')
    link = get_setting('instruction_link') or 'https://t.me/+fsafas34'
    return text.replace('{instruction_link}', link)

async def send_welcome_message(user_id):
    try:
        welcome_text = get_welcome_text()
        welcome_image = get_setting('welcome_image')
        
        if welcome_image:
            await bot.send_photo(user_id, welcome_image, caption=welcome_text,
                               reply_markup=get_welcome_inline_keyboard(), parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(user_id, welcome_text, reply_markup=get_welcome_inline_keyboard(),
                                  parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        
        await bot.send_message(user_id, "👇 Меню открыто! Используй кнопки ниже:",
                              reply_markup=get_user_reply_keyboard())
    except Exception as e:
        logger.error(f"Error sending welcome to {user_id}: {e}")

# ==================== ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ ====================
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    add_user(user_id, message.from_user.username, message.from_user.first_name)
    
    if user_id == ADMIN_ID:
        await message.answer("👑 Добро пожаловать, Админ!", reply_markup=get_admin_keyboard())
        return
    
    if not await check_subscription(user_id):
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    await send_welcome_message(user_id)

@router.callback_query(F.data == "check_sub")
async def check_sub_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    
    if await check_subscription(user_id):
        try:
            await callback.message.delete()
        except:
            pass
        await send_welcome_message(user_id)
    else:
        await callback.answer("❌ Вы не подписаны на все каналы!", show_alert=True)

@router.callback_query(F.data == "search_code")
async def search_code_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await callback.message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        await callback.answer()
        return
    
    await state.set_state(UserStates.waiting_code)
    await callback.message.answer(get_text('search_prompt'))
    await callback.answer()

@router.callback_query(F.data == "open_menu")
async def open_menu_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await callback.message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        await callback.answer()
        return
    
    await callback.message.answer("👇 Меню открыто! Используй кнопки ниже:", reply_markup=get_user_reply_keyboard())
    await callback.answer()

@router.message(F.text == "🔍 Поиск по коду")
async def search_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    await state.set_state(UserStates.waiting_code)
    await message.answer(get_text('search_prompt'))

@router.message(F.text == "🎲 Случайный код")
async def random_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    count = get_films_count()
    logger.info(f"Random film request. Films count: {count}")
    
    if count == 0:
        await message.answer(get_text('random_empty'))
        return
    
    film = get_random_film()
    if not film:
        await message.answer(get_text('random_empty'))
        return
    
    code, name, image_id = film[0], film[1], film[2] if len(film) > 2 else None
    response = f"🎲 <b>Случайный фильм:</b>\n\n🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
    
    if image_id:
        try:
            await message.answer_photo(photo=image_id, caption=response, parse_mode=ParseMode.HTML)
        except:
            await message.answer(response, parse_mode=ParseMode.HTML)
    else:
        await message.answer(response, parse_mode=ParseMode.HTML)

@router.message(F.text == "🔥 Купить рекламу в этом боте")
async def ad_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if user_id == ADMIN_ID:
        await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())
        return
    
    if not await check_subscription(user_id):
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    await message.answer(get_text('ad_text'))

@router.message(UserStates.waiting_code)
async def process_search_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await state.clear()
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    code = message.text.strip()
    film = get_film_by_code(code)
    
    await state.clear()
    
    if film:
        name, image_id = film[0], film[1] if len(film) > 1 else None
        response = f"🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
        
        if image_id:
            try:
                await message.answer_photo(photo=image_id, caption=response, parse_mode=ParseMode.HTML)
            except:
                await message.answer(response, parse_mode=ParseMode.HTML)
        else:
            await message.answer(response, parse_mode=ParseMode.HTML)
    else:
        await message.answer(get_text('film_not_found'))

# ==================== АДМИН ====================
@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.message.edit_text("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ПРИВЕТСТВИЕ НАСТРОЙКИ ====================
@router.callback_query(F.data == "admin_welcome_settings")
async def admin_welcome_settings(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    
    has_image = "✅ Есть" if get_setting('welcome_image') else "❌ Нет"
    link = get_setting('instruction_link') or 'не задана'
    
    await callback.message.edit_text(
        f"👋 <b>Настройки приветствия</b>\n\n🖼 Картинка: {has_image}\n🔗 Ссылка: {link}",
        reply_markup=get_welcome_settings_keyboard(), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "preview_welcome")
async def preview_welcome(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer("Отправляю...")
    
    text = get_welcome_text()
    image = get_setting('welcome_image')
    
    try:
        if image:
            await bot.send_photo(callback.from_user.id, image, caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{text}",
                               reply_markup=get_welcome_inline_keyboard(), parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(callback.from_user.id, f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{text}",
                                  reply_markup=get_welcome_inline_keyboard(), parse_mode=ParseMode.HTML)
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")

@router.callback_query(F.data == "edit_welcome_text")
async def edit_welcome_text(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.edit_welcome_text)
    await callback.message.edit_text(
        f"📝 <b>Текущий текст:</b>\n\n{get_text('welcome')}\n\n💡 Используйте {{instruction_link}} для ссылки.\n\nОтправьте новый:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")]]),
        parse_mode=ParseMode.HTML)

@router.message(AdminStates.edit_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    update_text('welcome', message.text)
    await state.clear()
    await message.answer("✅ Текст обновлён!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_welcome_image")
async def edit_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.edit_welcome_image)
    
    buttons = []
    if get_setting('welcome_image'):
        buttons.append([InlineKeyboardButton(text="🗑 Удалить", callback_data="remove_welcome_image")])
    buttons.append([InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")])
    
    await callback.message.edit_text("🖼 Отправьте картинку:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data == "remove_welcome_image")
async def remove_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    update_setting('welcome_image', None)
    await state.clear()
    await callback.message.edit_text("✅ Картинка удалена!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_welcome_image, F.photo)
async def process_welcome_image(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    update_setting('welcome_image', message.photo[-1].file_id)
    await state.clear()
    await message.answer("✅ Картинка обновлена!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_instruction_link")
async def edit_instruction_link(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.edit_instruction_link)
    await callback.message.edit_text(
        f"🔗 <b>Текущая ссылка:</b>\n{get_setting('instruction_link') or 'не задана'}\n\nОтправьте новую:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")]]),
        parse_mode=ParseMode.HTML)

@router.message(AdminStates.edit_instruction_link)
async def process_instruction_link(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    update_setting('instruction_link', message.text.strip())
    await state.clear()
    await message.answer("✅ Ссылка обновлена!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== РАССЫЛКА ====================
@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text("📨 <b>Создание рассылки</b>\n\nОтправьте сообщение:",
                                     reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

@router.message(AdminStates.broadcast_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = {}
    if message.photo:
        data['photo'] = message.photo[-1].file_id
        data['caption'] = message.caption or ""
    elif message.video:
        data['video'] = message.video.file_id
        data['caption'] = message.caption or ""
    else:
        data['text'] = message.text
    
    await state.update_data(message_data=data)
    await state.set_state(AdminStates.broadcast_buttons)
    
    await message.answer("🔘 Кнопки:\n<code>Текст | https://link.com</code>",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="⏭ Без кнопок", callback_data="broadcast_no_buttons")],
                            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
                        ]), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "broadcast_no_buttons")
async def broadcast_no_buttons(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.update_data(buttons=None)
    await goto_date_selection(callback.message, state)

@router.message(AdminStates.broadcast_buttons)
async def process_broadcast_buttons(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    buttons = parse_buttons_text(message.text)
    if not buttons:
        await message.answer("❌ Не удалось распознать!")
        return
    
    await state.update_data(buttons=buttons)
    await message.answer(f"✅ Кнопок: {len(buttons)}")
    await goto_date_selection(message, state)

async def goto_date_selection(message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_date)
    now = datetime.utcnow() + timedelta(hours=3)
    
    await message.answer("📅 Дата:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 Сегодня ({now.strftime('%d.%m')})", callback_data="broadcast_date_today")],
        [InlineKeyboardButton(text=f"📅 Завтра ({(now+timedelta(days=1)).strftime('%d.%m')})", callback_data="broadcast_date_tomorrow")],
        [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data="broadcast_now")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
    ]))

@router.callback_query(F.data == "broadcast_date_today")
async def broadcast_date_today(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.update_data(scheduled_date=(datetime.utcnow()+timedelta(hours=3)).strftime('%Y-%m-%d'))
    await goto_time_selection(callback.message, state)

@router.callback_query(F.data == "broadcast_date_tomorrow")
async def broadcast_date_tomorrow(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.update_data(scheduled_date=(datetime.utcnow()+timedelta(hours=3)+timedelta(days=1)).strftime('%Y-%m-%d'))
    await goto_time_selection(callback.message, state)

@router.message(AdminStates.broadcast_date)
async def process_broadcast_date(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    date = parse_date(message.text)
    if not date:
        await message.answer("❌ Неверный формат!")
        return
    await state.update_data(scheduled_date=date)
    await goto_time_selection(message, state)

async def goto_time_selection(message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_time)
    await message.answer("⏰ Время (МСК):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="09:00", callback_data="bt_09:00"),
         InlineKeyboardButton(text="12:00", callback_data="bt_12:00"),
         InlineKeyboardButton(text="15:00", callback_data="bt_15:00")],
        [InlineKeyboardButton(text="18:00", callback_data="bt_18:00"),
         InlineKeyboardButton(text="20:00", callback_data="bt_20:00"),
         InlineKeyboardButton(text="22:00", callback_data="bt_22:00")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_date")]
    ]))

@router.callback_query(F.data == "back_to_date")
async def back_to_date(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await goto_date_selection(callback.message, state)

@router.callback_query(F.data.startswith("bt_"))
async def broadcast_time_preset(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.update_data(scheduled_time=callback.data.replace("bt_", ""))
    await goto_confirmation(callback.message, state)

@router.message(AdminStates.broadcast_time)
async def process_broadcast_time(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    if not re.match(r'^([01]?[0-9]|2[0-3]):([0-5][0-9])$', message.text.strip()):
        await message.answer("❌ Неверный формат!")
        return
    parts = message.text.strip().split(':')
    await state.update_data(scheduled_time=f"{int(parts[0]):02d}:{parts[1]}")
    await goto_confirmation(message, state)

async def goto_confirmation(message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_confirm)
    data = await state.get_data()
    
    await send_preview(message.chat.id, data['message_data'], data.get('buttons'))
    
    await message.answer(
        f"📋 <b>Подтверждение</b>\n\n📅 {format_date_display(data['scheduled_date'])} в {data['scheduled_time']} МСК\n👥 Получателей: {len(get_all_users())}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="broadcast_confirm")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ]), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "broadcast_confirm")
async def broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    bid = save_scheduled_broadcast(data['message_data'], data.get('buttons'), data['scheduled_date'], data['scheduled_time'])
    start_broadcast_task(bid)
    await state.clear()
    await callback.message.edit_text(f"✅ Рассылка #{bid} запланирована!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "broadcast_now")
async def broadcast_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    await callback.message.edit_text("📨 Выполняю...")
    success, failed = await do_broadcast(data.get('message_data', {}), data.get('buttons'))
    await state.clear()
    await callback.message.edit_text(f"✅ Готово!\n📨 Успешно: {success}\n❌ Ошибок: {failed}")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ОТЛОЖЕННЫЕ ====================
@router.callback_query(F.data == "admin_scheduled")
async def admin_scheduled(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    broadcasts = get_pending_broadcasts()
    
    if not broadcasts:
        await callback.message.edit_text("📋 Нет рассылок", reply_markup=get_back_keyboard())
        return
    
    buttons = [[InlineKeyboardButton(text=f"#{b['id']} - {format_date_display(b['scheduled_date'])} {b['scheduled_time']}", 
                                     callback_data=f"vb_{b['id']}")] for b in broadcasts]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    await callback.message.edit_text("📋 <b>Рассылки:</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("vb_"))
async def view_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    bid = int(callback.data.replace("vb_", ""))
    b = get_broadcast_by_id(bid)
    if not b:
        await callback.answer("Не найдена!", show_alert=True)
        return
    
    await send_preview(callback.message.chat.id, json.loads(b['message_data']), 
                      json.loads(b['buttons']) if b['buttons'] else None)
    
    await callback.message.answer(
        f"📋 <b>Рассылка #{bid}</b>\n📅 {format_date_display(b['scheduled_date'])} {b['scheduled_time']}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Отправить", callback_data=f"sb_{bid}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"db_{bid}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_scheduled")]
        ]), parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("sb_"))
async def send_bc_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    bid = int(callback.data.replace("sb_", ""))
    b = get_broadcast_by_id(bid)
    if not b:
        return
    
    if bid in active_tasks:
        active_tasks[bid].cancel()
        del active_tasks[bid]
    
    await callback.message.edit_text("📨 Выполняю...")
    success, failed = await do_broadcast(json.loads(b['message_data']), json.loads(b['buttons']) if b['buttons'] else None)
    mark_broadcast_completed(bid)
    await callback.message.edit_text(f"✅ Готово!\n📨 {success}\n❌ {failed}")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("db_"))
async def delete_bc(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    bid = int(callback.data.replace("db_", ""))
    if bid in active_tasks:
        active_tasks[bid].cancel()
        del active_tasks[bid]
    delete_broadcast(bid)
    await callback.message.edit_text(f"🗑 Удалена #{bid}")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ФИЛЬМЫ ====================
@router.callback_query(F.data == "admin_films_list")
async def admin_films_list(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    films = get_all_films()
    
    if not films:
        await callback.message.edit_text("📋 Список пуст", reply_markup=get_back_keyboard())
        return
    
    text = "📋 <b>Фильмы:</b>\n\n" + "\n".join([f"{'🖼' if f[2] else '📄'} <code>{f[0]}</code> - {f[1]}" for f in films])
    if len(text) > 4000:
        text = text[:4000] + "\n..."
    await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "admin_add_film")
async def admin_add_film(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.add_film_name)
    await callback.message.edit_text("🎬 Введите название:", reply_markup=get_back_keyboard())

@router.message(AdminStates.add_film_name)
async def process_film_name(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.update_data(film_name=message.text)
    code = generate_unique_code()
    await state.update_data(generated_code=code)
    await state.set_state(AdminStates.add_film_code)
    
    await message.answer(f"📝 Код: <code>{code}</code>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎲 Использовать {code}", callback_data="use_gen_code")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
    ]), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "use_gen_code")
async def use_gen_code(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    await state.update_data(film_code=data['generated_code'])
    await state.set_state(AdminStates.add_film_image)
    
    await callback.message.edit_text(f"🖼 Картинка:\n📝 <code>{data['generated_code']}</code>",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
                                         [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
                                     ]), parse_mode=ParseMode.HTML)

@router.message(AdminStates.add_film_code)
async def process_film_code(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    code = message.text.strip()
    if get_film_by_code(code):
        await message.answer("❌ Код существует!")
        return
    await state.update_data(film_code=code)
    await state.set_state(AdminStates.add_film_image)
    
    await message.answer(f"🖼 Картинка:\n📝 <code>{code}</code>",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
                            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
                        ]), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "skip_img")
async def skip_img(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    add_film(data['film_code'], data['film_name'])
    await state.clear()
    await callback.message.edit_text(f"✅ Добавлен!\n🎬 {data['film_name']}\n📝 <code>{data['film_code']}</code>", parse_mode=ParseMode.HTML)
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.add_film_image, F.photo)
async def process_film_img(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    add_film(data['film_code'], data['film_name'], message.photo[-1].file_id)
    await state.clear()
    await message.answer(f"✅ Добавлен с картинкой!\n🎬 {data['film_name']}\n📝 <code>{data['film_code']}</code>", parse_mode=ParseMode.HTML)
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_delete_film")
async def admin_delete_film(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    films = get_all_films()
    
    if not films:
        await callback.message.edit_text("📋 Пусто", reply_markup=get_back_keyboard())
        return
    
    buttons = [[InlineKeyboardButton(text=f"🗑 {f[0]} - {f[1][:20]}", callback_data=f"df_{f[0]}")] for f in films[:30]]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    await callback.message.edit_text("🗑 Выберите:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("df_"))
async def confirm_del_film(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    delete_film_by_code(callback.data.replace("df_", ""))
    await callback.message.edit_text("✅ Удалён!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== КАНАЛЫ ====================
@router.callback_query(F.data == "admin_add_channels")
async def admin_add_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.add_channels)
    await callback.message.edit_text(
        "📢 <b>Формат:</b>\n<code>1. Текст (https://ссылка)</code>\n<code>2. С проверкой (https://t.me/ch) -1001234</code>",
        reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

@router.message(AdminStates.add_channels)
async def process_channels(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    channels = parse_channels_text(message.text)
    if not channels:
        await message.answer("❌ Не удалось распознать формат!\n\nИспользуйте:\n<code>1. Текст кнопки (https://ссылка)</code>", parse_mode=ParseMode.HTML)
        return
    
    clear_all_channels()
    for ch in channels:
        add_channel(ch['button_text'], ch['link'], ch['channel_id'])
    
    await state.clear()
    await message.answer(f"✅ Добавлено: {len(channels)}")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_view_channels")
async def admin_view_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    channels = get_all_channels()
    
    if not channels:
        await callback.message.edit_text("📢 Пусто", reply_markup=get_back_keyboard())
        return
    
    text = "📢 <b>Каналы:</b>\n\n" + "\n".join([
        f"<b>{c[1]}</b>\n🔗 {c[2]}\n{'✓ '+c[3] if c[3] else '○ без проверки'}\n" for c in channels
    ])
    await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@router.callback_query(F.data == "admin_delete_channels")
async def admin_delete_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    channels = get_all_channels()
    
    if not channels:
        await callback.message.edit_text("📢 Пусто", reply_markup=get_back_keyboard())
        return
    
    buttons = [[InlineKeyboardButton(text="🗑 Удалить ВСЕ", callback_data="del_all_ch")]]
    buttons += [[InlineKeyboardButton(text=f"🗑 {c[1][:25]}", callback_data=f"dc_{c[0]}")] for c in channels]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    await callback.message.edit_text("🗑 Выберите:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data == "del_all_ch")
async def del_all_ch(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    clear_all_channels()
    await callback.message.edit_text("✅ Удалены!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("dc_"))
async def del_channel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    delete_channel_by_id(int(callback.data.replace("dc_", "")))
    await callback.message.edit_text("✅ Удалён!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ТЕКСТЫ ====================
@router.callback_query(F.data == "admin_texts")
async def admin_texts(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.message.edit_text("📝 Выберите:", reply_markup=get_texts_keyboard())

@router.callback_query(F.data.startswith("edit_text_"))
async def edit_text_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    key = callback.data.replace("edit_text_", "")
    await state.update_data(text_key=key)
    await state.set_state(AdminStates.edit_text_value)
    
    names = {'subscribe_required': 'Подписка', 'film_not_found': 'Не найден', 'ad_text': 'Реклама', 'search_prompt': 'Поиск'}
    await callback.message.edit_text(f"📝 <b>{names.get(key, key)}</b>\n\nТекущий:\n{get_text(key)}\n\nНовый:",
                                     reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

@router.message(AdminStates.edit_text_value)
async def process_edit_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    update_text(data['text_key'], message.text)
    await state.clear()
    await message.answer("✅ Обновлено!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== СТАТИСТИКА ====================
@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    
    stats = get_users_stats()
    films = get_all_films()
    channels = get_all_channels()
    pending = get_pending_broadcasts()
    ch_check = sum(1 for c in channels if c[3])
    now = datetime.utcnow() + timedelta(hours=3)
    
    await callback.message.edit_text(
        f"📊 <b>Статистика</b>\n\n🕐 {now.strftime('%d.%m.%Y %H:%M')} МСК\n\n"
        f"👥 <b>Пользователи:</b>\n   Всего: {stats['total']}\n   Сегодня: {stats['today']}\n   Неделя: {stats['week']}\n   Месяц: {stats['month']}\n\n"
        f"🎬 Фильмов: {len(films)}\n📢 Каналов: {len(channels)} (проверка: {ch_check})\n📨 Отложенных: {len(pending)}",
        reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

# ==================== ОБРАБОТКА ТЕКСТА ====================
@router.message(~F.text.startswith('/'), StateFilter(None))
async def process_code(message: Message, state: FSMContext):
    if message.text in ["🔍 Поиск по коду", "🎲 Случайный код", "🔥 Купить рекламу в этом боте"]:
        return
    
    user_id = message.from_user.id
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await message.answer(get_text('subscribe_required'), reply_markup=get_subscribe_keyboard())
        return
    
    code = message.text.strip()
    film = get_film_by_code(code)
    
    if film:
        name, image_id = film[0], film[1] if len(film) > 1 else None
        response = f"🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
        
        if image_id:
            try:
                await message.answer_photo(photo=image_id, caption=response, parse_mode=ParseMode.HTML)
            except:
                await message.answer(response, parse_mode=ParseMode.HTML)
        else:
            await message.answer(response, parse_mode=ParseMode.HTML)
    else:
        await message.answer(get_text('film_not_found'))

# ==================== ЗАПУСК ====================
async def main():
    init_db()
    logger.info(f"Films in DB: {get_films_count()}")
    await restart_pending_broadcasts()
    logger.info("Bot started!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
