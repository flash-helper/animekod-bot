import asyncio
import logging
import random
import re
import json
import sqlite3
import os
import time
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
BOT_TOKEN = "8435441500:AAHU4ie7Tz3NTrvPnbhQu6xwMzqRhP_NTd8"
ADMIN_ID = 1736344274

if os.path.exists('/data'):
    DB_PATH = '/data/bot_database.db'
else:
    DB_PATH = 'bot_database.db'

# ==================== ЛОГИРОВАНИЕ ====================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

active_tasks = {}

# ==================== БАЗА ДАННЫХ ====================
class Database:
    """
    Исправленный класс для работы с SQLite.
    Каждая операция использует отдельное соединение.
    """
    
    def execute(self, query, params=(), fetch=False, fetchone=False):
        max_retries = 5
        last_error = None
        
        for attempt in range(max_retries):
            conn = None
            try:
                conn = sqlite3.connect(DB_PATH, timeout=30.0)
                conn.row_factory = sqlite3.Row
                
                # Настройки для лучшей конкурентности
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA busy_timeout=30000")
                
                cursor = conn.cursor()
                cursor.execute(query, params)
                
                if fetchone:
                    row = cursor.fetchone()
                    result = dict(row) if row else None
                elif fetch:
                    rows = cursor.fetchall()
                    result = [dict(row) for row in rows] if rows else []
                else:
                    conn.commit()
                    result = cursor.lastrowid
                
                return result
                
            except sqlite3.OperationalError as e:
                last_error = e
                if "locked" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Database locked, retry {attempt + 1}/{max_retries}")
                    time.sleep(0.1 * (2 ** attempt))
                    continue
                logger.error(f"Database OperationalError: {e}")
                break
                
            except sqlite3.Error as e:
                last_error = e
                logger.error(f"Database error: {e}")
                break
                
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
        
        if last_error:
            logger.error(f"Database failed after {max_retries} retries: {last_error}")
        
        return [] if fetch else None if fetchone else 0

db = Database()

def init_db():
    logger.info(f"Initializing database at {DB_PATH}")
    
    db.execute('''CREATE TABLE IF NOT EXISTS films (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        image_id TEXT
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        button_text TEXT NOT NULL,
        link TEXT NOT NULL,
        channel_id TEXT
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        is_new INTEGER DEFAULT 1,
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS texts (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    
    db.execute('''CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_data TEXT NOT NULL,
        buttons TEXT,
        scheduled_date TEXT NOT NULL,
        scheduled_time TEXT NOT NULL,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    defaults = {
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
    
    for key, value in defaults.items():
        db.execute('INSERT OR IGNORE INTO texts (key, value) VALUES (?, ?)', (key, value))
    
    db.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', 
               ('instruction_link', 'https://t.me/+fsafas34'))
    
    logger.info("Database initialized")

# ==================== ФУНКЦИИ БД ====================
def add_user(user_id, username, first_name):
    existing = db.execute(
        'SELECT user_id, is_new FROM users WHERE user_id = ?', 
        (user_id,), fetchone=True
    )
    if not existing:
        db.execute(
            'INSERT INTO users (user_id, username, first_name, is_new) VALUES (?, ?, ?, 1)',
            (user_id, username, first_name)
        )
        return True
    else:
        db.execute(
            'UPDATE users SET username = ?, first_name = ? WHERE user_id = ?',
            (username, first_name, user_id)
        )
        return existing.get('is_new') == 1

def mark_user_not_new(user_id):
    db.execute('UPDATE users SET is_new = 0 WHERE user_id = ?', (user_id,))

def is_user_new(user_id):
    result = db.execute(
        'SELECT is_new FROM users WHERE user_id = ?', 
        (user_id,), fetchone=True
    )
    return result.get('is_new') == 1 if result else True

def get_all_users():
    result = db.execute('SELECT user_id FROM users', fetch=True)
    return [row.get('user_id') for row in result] if result else []

def get_users_stats():
    stats = {'total': 0, 'today': 0, 'week': 0, 'month': 0}
    
    r = db.execute('SELECT COUNT(*) as cnt FROM users', fetchone=True)
    stats['total'] = r.get('cnt', 0) if r else 0
    
    r = db.execute(
        'SELECT COUNT(*) as cnt FROM users WHERE date(joined_at, "+3 hours") = date("now", "+3 hours")', 
        fetchone=True
    )
    stats['today'] = r.get('cnt', 0) if r else 0
    
    r = db.execute(
        'SELECT COUNT(*) as cnt FROM users WHERE datetime(joined_at, "+3 hours") >= datetime("now", "+3 hours", "-7 days")', 
        fetchone=True
    )
    stats['week'] = r.get('cnt', 0) if r else 0
    
    r = db.execute(
        'SELECT COUNT(*) as cnt FROM users WHERE datetime(joined_at, "+3 hours") >= datetime("now", "+3 hours", "-30 days")', 
        fetchone=True
    )
    stats['month'] = r.get('cnt', 0) if r else 0
    
    return stats

def add_film(code, name, image_id=None):
    return db.execute(
        'INSERT INTO films (code, name, image_id) VALUES (?, ?, ?)', 
        (code, name, image_id)
    )

def delete_film_by_code(code):
    db.execute('DELETE FROM films WHERE code = ?', (code,))

def get_film_by_code(code):
    result = db.execute(
        'SELECT name, image_id FROM films WHERE code = ?', 
        (code,), fetchone=True
    )
    return result

def get_all_films():
    result = db.execute(
        'SELECT code, name, image_id FROM films ORDER BY id DESC', 
        fetch=True
    )
    return result if result else []

def get_random_film():
    result = db.execute(
        'SELECT code, name, image_id FROM films ORDER BY RANDOM() LIMIT 1', 
        fetchone=True
    )
    return result

def get_films_count():
    result = db.execute('SELECT COUNT(*) as cnt FROM films', fetchone=True)
    return result.get('cnt', 0) if result else 0

def generate_unique_code():
    existing = db.execute('SELECT code FROM films', fetch=True)
    existing_codes = [r.get('code') for r in existing] if existing else []
    for _ in range(100):
        code = str(random.randint(1000, 9999))
        if code not in existing_codes:
            return code
    return str(random.randint(10000, 99999))

def add_channel(button_text, link, channel_id=None):
    db.execute(
        'INSERT INTO channels (button_text, link, channel_id) VALUES (?, ?, ?)', 
        (button_text, link, channel_id)
    )

def clear_all_channels():
    db.execute('DELETE FROM channels')

def delete_channel_by_id(id):
    db.execute('DELETE FROM channels WHERE id = ?', (id,))

def get_all_channels():
    result = db.execute(
        'SELECT id, button_text, link, channel_id FROM channels', 
        fetch=True
    )
    return result if result else []

def get_text(key):
    result = db.execute(
        'SELECT value FROM texts WHERE key = ?', 
        (key,), fetchone=True
    )
    return result.get('value', '') if result else ''

def update_text(key, value):
    db.execute('INSERT OR REPLACE INTO texts (key, value) VALUES (?, ?)', (key, value))

def get_setting(key):
    result = db.execute(
        'SELECT value FROM settings WHERE key = ?', 
        (key,), fetchone=True
    )
    return result.get('value') if result else None

def update_setting(key, value):
    db.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))

# Рассылки
def save_scheduled_broadcast(message_data, buttons, scheduled_date, scheduled_time):
    return db.execute(
        'INSERT INTO scheduled_broadcasts (message_data, buttons, scheduled_date, scheduled_time, status) VALUES (?, ?, ?, ?, "pending")',
        (json.dumps(message_data), json.dumps(buttons) if buttons else None, scheduled_date, scheduled_time)
    )

def get_pending_broadcasts():
    result = db.execute(
        'SELECT id, message_data, buttons, scheduled_date, scheduled_time FROM scheduled_broadcasts WHERE status = "pending" ORDER BY scheduled_date, scheduled_time',
        fetch=True
    )
    return result if result else []

def get_broadcast_by_id(broadcast_id):
    result = db.execute(
        'SELECT id, message_data, buttons, scheduled_date, scheduled_time, status FROM scheduled_broadcasts WHERE id = ?', 
        (broadcast_id,), fetchone=True
    )
    return result

def update_broadcast(broadcast_id, **kwargs):
    for key, value in kwargs.items():
        if key == 'message_data':
            db.execute(
                'UPDATE scheduled_broadcasts SET message_data = ? WHERE id = ?', 
                (json.dumps(value), broadcast_id)
            )
        elif key == 'buttons':
            db.execute(
                'UPDATE scheduled_broadcasts SET buttons = ? WHERE id = ?', 
                (json.dumps(value) if value else None, broadcast_id)
            )
        elif key in ('scheduled_date', 'scheduled_time'):
            db.execute(
                f'UPDATE scheduled_broadcasts SET {key} = ? WHERE id = ?', 
                (value, broadcast_id)
            )

def delete_broadcast(broadcast_id):
    db.execute('DELETE FROM scheduled_broadcasts WHERE id = ?', (broadcast_id,))

def mark_broadcast_completed(broadcast_id):
    db.execute(
        'UPDATE scheduled_broadcasts SET status = "completed" WHERE id = ?', 
        (broadcast_id,)
    )

# ==================== FSM ====================
class AdminStates(StatesGroup):
    broadcast_message = State()
    broadcast_buttons = State()
    broadcast_date = State()
    broadcast_time = State()
    broadcast_confirm = State()
    
    edit_bc_message = State()
    edit_bc_buttons = State()
    edit_bc_date = State()
    edit_bc_time = State()
    
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
    
    for ch in channels:
        channel_id = ch.get('channel_id')
        if not channel_id:
            continue
        try:
            member = await bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
            if member.status in ['left', 'kicked']:
                return False
        except Exception as e:
            logger.warning(f"Check sub error {channel_id}: {e}")
            continue
    return True

def get_subscribe_keyboard():
    channels = get_all_channels()
    buttons = [
        [InlineKeyboardButton(text=ch.get('button_text', 'Канал'), url=ch.get('link', ''))] 
        for ch in channels
    ]
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
    pattern = r'^[\d.]*\s*(.+?)\s*\((https?://[^\)]+)\)\s*(-?\d+)?$'
    for line in text.strip().split('\n'):
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
    for line in text.strip().split('\n'):
        if '|' in line:
            parts = line.split('|', 1)
            if len(parts) == 2 and parts[0].strip() and parts[1].strip():
                buttons.append({'text': parts[0].strip(), 'url': parts[1].strip()})
    return buttons

def create_inline_keyboard_from_buttons(buttons):
    if not buttons:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=btn.get('text', ''), url=btn.get('url', ''))] 
        for btn in buttons
    ])

def parse_date(date_str):
    date_str = date_str.strip()
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', date_str)
    if match:
        try:
            return datetime(
                int(match.group(3)), int(match.group(2)), int(match.group(1))
            ).strftime('%Y-%m-%d')
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
    success, failed = 0, 0
    keyboard = create_inline_keyboard_from_buttons(buttons)
    
    for user_id in users:
        try:
            if message_data.get('photo'):
                await bot.send_photo(
                    user_id, message_data['photo'], 
                    caption=message_data.get('caption'),
                    reply_markup=keyboard, parse_mode=ParseMode.HTML
                )
            elif message_data.get('video'):
                await bot.send_video(
                    user_id, message_data['video'], 
                    caption=message_data.get('caption'),
                    reply_markup=keyboard, parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_message(
                    user_id, message_data.get('text', ''), 
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML, disable_web_page_preview=True
                )
            success += 1
        except Exception as e:
            logger.debug(f"Broadcast to {user_id} failed: {e}")
            failed += 1
        await asyncio.sleep(0.05)
    return success, failed

async def send_preview(chat_id, message_data, buttons=None):
    keyboard = create_inline_keyboard_from_buttons(buttons)
    try:
        text = message_data.get('caption') or message_data.get('text', '')
        preview = f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{text}"
        
        if message_data.get('photo'):
            await bot.send_photo(
                chat_id, message_data['photo'], 
                caption=preview, reply_markup=keyboard, parse_mode=ParseMode.HTML
            )
        elif message_data.get('video'):
            await bot.send_video(
                chat_id, message_data['video'], 
                caption=preview, reply_markup=keyboard, parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                chat_id, preview, reply_markup=keyboard, 
                parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка предпросмотра: {e}")

async def scheduled_broadcast_task(broadcast_id):
    try:
        broadcast = get_broadcast_by_id(broadcast_id)
        if not broadcast or broadcast.get('status') != 'pending':
            return
        
        now_msk = datetime.utcnow() + timedelta(hours=3)
        target = datetime.strptime(
            f"{broadcast['scheduled_date']} {broadcast['scheduled_time']}", 
            "%Y-%m-%d %H:%M"
        )
        delay = max(0, (target - now_msk).total_seconds())
        
        if delay > 0:
            logger.info(f"Broadcast #{broadcast_id} scheduled in {delay:.0f} seconds")
            await asyncio.sleep(delay)
        
        # Повторная проверка после ожидания
        broadcast = get_broadcast_by_id(broadcast_id)
        if not broadcast or broadcast.get('status') != 'pending':
            logger.info(f"Broadcast #{broadcast_id} was cancelled or completed")
            return
        
        message_data = json.loads(broadcast['message_data'])
        buttons = json.loads(broadcast['buttons']) if broadcast.get('buttons') else None
        
        success, failed = await do_broadcast(message_data, buttons)
        mark_broadcast_completed(broadcast_id)
        
        logger.info(f"Broadcast #{broadcast_id} completed: {success} success, {failed} failed")
        
        try:
            await bot.send_message(
                ADMIN_ID, 
                f"✅ Рассылка #{broadcast_id} завершена!\n📨 Успешно: {success}\n❌ Ошибок: {failed}"
            )
        except:
            pass
            
    except asyncio.CancelledError:
        logger.info(f"Broadcast #{broadcast_id} cancelled")
    except Exception as e:
        logger.error(f"Broadcast #{broadcast_id} error: {e}")
    finally:
        if broadcast_id in active_tasks:
            del active_tasks[broadcast_id]

def start_broadcast_task(broadcast_id):
    if broadcast_id in active_tasks:
        try:
            active_tasks[broadcast_id].cancel()
        except:
            pass
    active_tasks[broadcast_id] = asyncio.create_task(scheduled_broadcast_task(broadcast_id))

async def restart_pending_broadcasts():
    broadcasts = get_pending_broadcasts()
    for b in broadcasts:
        start_broadcast_task(b['id'])
    if broadcasts:
        logger.info(f"Restarted {len(broadcasts)} pending broadcasts")

# ==================== ПРИВЕТСТВИЕ ====================
def get_welcome_text():
    text = get_text('welcome')
    link = get_setting('instruction_link') or 'https://t.me'
    return text.replace('{instruction_link}', link)

async def send_welcome_message(user_id):
    try:
        text = get_welcome_text()
        image = get_setting('welcome_image')
        
        if image:
            await bot.send_photo(
                user_id, image, caption=text, 
                reply_markup=get_welcome_inline_keyboard(), 
                parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                user_id, text, 
                reply_markup=get_welcome_inline_keyboard(), 
                parse_mode=ParseMode.HTML, disable_web_page_preview=True
            )
        
        await bot.send_message(
            user_id, "👇 Меню:", 
            reply_markup=get_user_reply_keyboard()
        )
    except Exception as e:
        logger.error(f"Welcome error for {user_id}: {e}")

async def send_subscribe_message(user_id):
    try:
        await bot.send_message(
            user_id, get_text('subscribe_required'), 
            reply_markup=get_subscribe_keyboard()
        )
    except Exception as e:
        logger.error(f"Subscribe message error for {user_id}: {e}")

# ==================== ХЕНДЛЕРЫ ====================
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    add_user(user_id, message.from_user.username, message.from_user.first_name)
    
    if user_id == ADMIN_ID:
        await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())
        return
    
    await send_welcome_message(user_id)

@router.callback_query(F.data == "check_sub")
async def check_sub_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    
    user_id = callback.from_user.id
    
    if await check_subscription(user_id):
        mark_user_not_new(user_id)
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(
            "✅ Отлично! Теперь вы можете пользоваться ботом.", 
            reply_markup=get_user_reply_keyboard()
        )
    else:
        await callback.answer("❌ Вы не подписаны на все каналы!", show_alert=True)

@router.callback_query(F.data == "search_code")
async def search_code_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    user_id = callback.from_user.id
    
    mark_user_not_new(user_id)
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    await state.set_state(UserStates.waiting_code)
    await callback.message.answer(get_text('search_prompt'))

@router.callback_query(F.data == "open_menu")
async def open_menu_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    user_id = callback.from_user.id
    
    mark_user_not_new(user_id)
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    await callback.message.answer("👇 Меню:", reply_markup=get_user_reply_keyboard())

@router.message(F.text == "🔍 Поиск по коду")
async def search_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    mark_user_not_new(user_id)
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    await state.set_state(UserStates.waiting_code)
    await message.answer(get_text('search_prompt'))

@router.message(F.text == "🎲 Случайный код")
async def random_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    mark_user_not_new(user_id)
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    film = get_random_film()
    if not film:
        await message.answer(get_text('random_empty'))
        return
    
    text = f"🎲 <b>Случайный:</b>\n\n🎬 <b>{film.get('name', 'Неизвестно')}</b>\n\n📝 Код: <code>{film.get('code', '')}</code>"
    
    if film.get('image_id'):
        try:
            await message.answer_photo(
                photo=film['image_id'], caption=text, 
                parse_mode=ParseMode.HTML
            )
            return
        except:
            pass
    await message.answer(text, parse_mode=ParseMode.HTML)

@router.message(F.text == "🔥 Купить рекламу в этом боте")
async def ad_button(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    if user_id == ADMIN_ID:
        await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())
        return
    
    await message.answer(get_text('ad_text'))

@router.message(UserStates.waiting_code)
async def process_search_code(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    
    mark_user_not_new(user_id)
    
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    code = message.text.strip()
    film = get_film_by_code(code)
    
    if not film:
        await message.answer(get_text('film_not_found'))
        return
    
    text = f"🎬 <b>{film.get('name', 'Неизвестно')}</b>\n\n📝 Код: <code>{code}</code>"
    
    if film.get('image_id'):
        try:
            await message.answer_photo(
                photo=film['image_id'], caption=text, 
                parse_mode=ParseMode.HTML
            )
            return
        except:
            pass
    await message.answer(text, parse_mode=ParseMode.HTML)

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
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ПРИВЕТСТВИЕ НАСТРОЙКИ ====================
@router.callback_query(F.data == "admin_welcome_settings")
async def admin_welcome_settings(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    has_image = "✅" if get_setting('welcome_image') else "❌"
    link = get_setting('instruction_link') or '-'
    
    await callback.message.edit_text(
        f"👋 <b>Приветствие</b>\n\n🖼 Картинка: {has_image}\n🔗 Ссылка: {link}",
        reply_markup=get_welcome_settings_keyboard(), 
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "preview_welcome")
async def preview_welcome(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer("Отправляю...")
    
    text = get_welcome_text()
    image = get_setting('welcome_image')
    
    try:
        if image:
            await bot.send_photo(
                callback.from_user.id, image, 
                caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{text}",
                reply_markup=get_welcome_inline_keyboard(), 
                parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                callback.from_user.id, 
                f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{text}",
                reply_markup=get_welcome_inline_keyboard(), 
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")

@router.callback_query(F.data == "edit_welcome_text")
async def edit_welcome_text(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.edit_welcome_text)
    
    current_text = get_text('welcome')
    await callback.message.edit_text(
        f"📝 <b>Текущий:</b>\n\n{current_text}\n\n💡 {{instruction_link}} = ссылка\n\nОтправьте новый текст:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️", callback_data="admin_welcome_settings")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    update_text('welcome', message.text)
    await state.clear()
    await message.answer("✅ Текст сохранён!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_welcome_image")
async def edit_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.edit_welcome_image)
    
    buttons = []
    if get_setting('welcome_image'):
        buttons.append([InlineKeyboardButton(text="🗑 Удалить картинку", callback_data="remove_welcome_image")])
    buttons.append([InlineKeyboardButton(text="◀️", callback_data="admin_welcome_settings")])
    
    await callback.message.edit_text(
        "🖼 Отправьте новую картинку:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data == "remove_welcome_image")
async def remove_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
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
    await message.answer("✅ Картинка сохранена!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_instruction_link")
async def edit_instruction_link(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.edit_instruction_link)
    
    current_link = get_setting('instruction_link') or '-'
    await callback.message.edit_text(
        f"🔗 <b>Текущая ссылка:</b> {current_link}\n\nОтправьте новую ссылку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️", callback_data="admin_welcome_settings")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_instruction_link)
async def process_instruction_link(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    update_setting('instruction_link', message.text.strip())
    await state.clear()
    await message.answer("✅ Ссылка сохранена!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== РАССЫЛКА ====================
@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text(
        "📨 Отправьте сообщение для рассылки (текст, фото или видео):", 
        reply_markup=get_back_keyboard()
    )

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
    elif message.text:
        data['text'] = message.text
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения!")
        return
    
    await state.update_data(message_data=data)
    await state.set_state(AdminStates.broadcast_buttons)
    
    await message.answer(
        "🔘 Добавить кнопки?\n\nФормат:\n<code>Текст кнопки | https://ссылка</code>\n\nКаждая кнопка с новой строки.\nИли нажмите «Без кнопок»",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Без кнопок", callback_data="bc_no_btn")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]), 
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "bc_no_btn")
async def bc_no_buttons(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.update_data(buttons=None)
    await goto_date(callback.message, state)

@router.message(AdminStates.broadcast_buttons)
async def process_bc_buttons(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    buttons = parse_buttons_text(message.text)
    if not buttons:
        await message.answer("❌ Неверный формат кнопок!")
        return
    await state.update_data(buttons=buttons)
    await message.answer(f"✅ Добавлено {len(buttons)} кнопок")
    await goto_date(message, state)

async def goto_date(message, state):
    await state.set_state(AdminStates.broadcast_date)
    now = datetime.utcnow() + timedelta(hours=3)
    
    await message.answer(
        "📅 Выберите дату или введите в формате ДД.ММ:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"📅 Сегодня ({now.strftime('%d.%m')})", 
                callback_data="bc_today"
            )],
            [InlineKeyboardButton(
                text=f"📅 Завтра ({(now + timedelta(days=1)).strftime('%d.%m')})", 
                callback_data="bc_tomorrow"
            )],
            [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data="bc_now")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )

@router.callback_query(F.data == "bc_today")
async def bc_today(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    today = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')
    await state.update_data(scheduled_date=today)
    await goto_time(callback.message, state)

@router.callback_query(F.data == "bc_tomorrow")
async def bc_tomorrow(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    tomorrow = (datetime.utcnow() + timedelta(hours=3) + timedelta(days=1)).strftime('%Y-%m-%d')
    await state.update_data(scheduled_date=tomorrow)
    await goto_time(callback.message, state)

@router.message(AdminStates.broadcast_date)
async def process_bc_date(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    date = parse_date(message.text)
    if not date:
        await message.answer("❌ Неверный формат даты! Используйте ДД.ММ")
        return
    await state.update_data(scheduled_date=date)
    await goto_time(message, state)

async def goto_time(message, state):
    await state.set_state(AdminStates.broadcast_time)
    await message.answer(
        "⏰ Выберите время (МСК) или введите в формате ЧЧ:ММ:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="09:00", callback_data="bt_09:00"),
                InlineKeyboardButton(text="12:00", callback_data="bt_12:00"),
                InlineKeyboardButton(text="15:00", callback_data="bt_15:00")
            ],
            [
                InlineKeyboardButton(text="18:00", callback_data="bt_18:00"),
                InlineKeyboardButton(text="20:00", callback_data="bt_20:00"),
                InlineKeyboardButton(text="22:00", callback_data="bt_22:00")
            ],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )

@router.callback_query(F.data.startswith("bt_"))
async def bc_time_preset(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    time_value = callback.data[3:]
    await state.update_data(scheduled_time=time_value)
    await goto_confirm(callback.message, state)

@router.message(AdminStates.broadcast_time)
async def process_bc_time(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    time_text = message.text.strip()
    if not re.match(r'^([01]?[0-9]|2[0-3]):([0-5][0-9])$', time_text):
        await message.answer("❌ Неверный формат времени! Используйте ЧЧ:ММ")
        return
    parts = time_text.split(':')
    formatted_time = f"{int(parts[0]):02d}:{parts[1]}"
    await state.update_data(scheduled_time=formatted_time)
    await goto_confirm(message, state)

async def goto_confirm(message, state):
    await state.set_state(AdminStates.broadcast_confirm)
    data = await state.get_data()
    
    await send_preview(message.chat.id, data['message_data'], data.get('buttons'))
    
    users_count = len(get_all_users())
    await message.answer(
        f"📋 <b>Подтверждение рассылки</b>\n\n"
        f"📅 Дата: {format_date_display(data['scheduled_date'])}\n"
        f"⏰ Время: {data['scheduled_time']} МСК\n"
        f"👥 Получателей: {users_count}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="bc_confirm")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "bc_confirm")
async def bc_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    data = await state.get_data()
    
    bid = save_scheduled_broadcast(
        data['message_data'], 
        data.get('buttons'), 
        data['scheduled_date'], 
        data['scheduled_time']
    )
    start_broadcast_task(bid)
    
    await state.clear()
    await callback.message.edit_text(
        f"✅ Рассылка #{bid} запланирована!\n\n"
        f"📅 {format_date_display(data['scheduled_date'])} в {data['scheduled_time']} МСК"
    )
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "bc_now")
async def bc_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    data = await state.get_data()
    
    if not data.get('message_data'):
        await callback.message.edit_text("❌ Сообщение не найдено. Начните заново.")
        await state.clear()
        return
    
    await callback.message.edit_text("📨 Отправка рассылки...")
    success, failed = await do_broadcast(data['message_data'], data.get('buttons'))
    
    await state.clear()
    await callback.message.edit_text(
        f"✅ Рассылка завершена!\n\n📨 Успешно: {success}\n❌ Ошибок: {failed}"
    )
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ОТЛОЖЕННЫЕ РАССЫЛКИ ====================
@router.callback_query(F.data == "admin_scheduled")
async def admin_scheduled(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    broadcasts = get_pending_broadcasts()
    if not broadcasts:
        await callback.message.edit_text(
            "📋 Нет отложенных рассылок", 
            reply_markup=get_back_keyboard()
        )
        return
    
    buttons = [
        [InlineKeyboardButton(
            text=f"#{b['id']} | {format_date_display(b['scheduled_date'])} {b['scheduled_time']}", 
            callback_data=f"vb_{b['id']}"
        )] 
        for b in broadcasts
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text(
        "📋 Отложенные рассылки:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data.startswith("vb_"))
async def view_bc(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    bid = int(callback.data[3:])
    b = get_broadcast_by_id(bid)
    if not b:
        await callback.message.edit_text("❌ Рассылка не найдена", reply_markup=get_back_keyboard())
        return
    
    await state.update_data(editing_bc_id=bid)
    
    message_data = json.loads(b['message_data'])
    buttons = json.loads(b['buttons']) if b.get('buttons') else None
    
    await send_preview(callback.message.chat.id, message_data, buttons)
    
    btn_count = len(buttons) if buttons else 0
    has_media = "🖼 Да" if message_data.get('photo') or message_data.get('video') else "📝 Нет"
    
    await callback.message.answer(
        f"📋 <b>Рассылка #{bid}</b>\n\n"
        f"📅 Дата: {format_date_display(b['scheduled_date'])}\n"
        f"⏰ Время: {b['scheduled_time']} МСК\n"
        f"🔘 Кнопок: {btn_count}\n"
        f"📷 Медиа: {has_media}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📝 Изменить сообщение", callback_data=f"ebc_msg_{bid}")],
            [InlineKeyboardButton(text="🔘 Изменить кнопки", callback_data=f"ebc_btn_{bid}")],
            [InlineKeyboardButton(text="📅 Изменить дату", callback_data=f"ebc_date_{bid}")],
            [InlineKeyboardButton(text="⏰ Изменить время", callback_data=f"ebc_time_{bid}")],
            [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"sb_{bid}")],
            [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"db_{bid}")],
            [InlineKeyboardButton(text="◀️ К списку", callback_data="admin_scheduled")]
        ]),
        parse_mode=ParseMode.HTML
    )

# Редактирование сообщения рассылки
@router.callback_query(F.data.startswith("ebc_msg_"))
async def edit_bc_msg(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[8:])
    await state.update_data(editing_bc_id=bid)
    await state.set_state(AdminStates.edit_bc_message)
    
    await callback.message.edit_text(
        "📝 Отправьте новое сообщение (текст, фото или видео):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"vb_{bid}")]
        ])
    )

@router.message(AdminStates.edit_bc_message)
async def process_edit_bc_msg(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    bid = data.get('editing_bc_id')
    
    if not bid:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    msg_data = {}
    if message.photo:
        msg_data['photo'] = message.photo[-1].file_id
        msg_data['caption'] = message.caption or ""
    elif message.video:
        msg_data['video'] = message.video.file_id
        msg_data['caption'] = message.caption or ""
    elif message.text:
        msg_data['text'] = message.text
    else:
        await message.answer("❌ Неподдерживаемый тип сообщения!")
        return
    
    update_broadcast(bid, message_data=msg_data)
    start_broadcast_task(bid)
    
    await state.clear()
    await message.answer(f"✅ Сообщение рассылки #{bid} обновлено!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# Редактирование кнопок рассылки
@router.callback_query(F.data.startswith("ebc_btn_"))
async def edit_bc_btn(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    # Проверяем что это не удаление кнопок
    if callback.data.startswith("ebc_btn_del_"):
        return
    
    await callback.answer()
    
    bid = int(callback.data[8:])
    await state.update_data(editing_bc_id=bid)
    await state.set_state(AdminStates.edit_bc_buttons)
    
    await callback.message.edit_text(
        "🔘 Отправьте новые кнопки:\n<code>Текст | https://ссылка</code>\n\nКаждая кнопка с новой строки",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить кнопки", callback_data=f"ebc_btn_del_{bid}")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"vb_{bid}")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data.startswith("ebc_btn_del_"))
async def del_bc_buttons(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[12:])
    update_broadcast(bid, buttons=None)
    start_broadcast_task(bid)
    
    await state.clear()
    await callback.message.edit_text(f"✅ Кнопки рассылки #{bid} удалены!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_bc_buttons)
async def process_edit_bc_btn(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    bid = data.get('editing_bc_id')
    
    if not bid:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    buttons = parse_buttons_text(message.text)
    if not buttons:
        await message.answer("❌ Не удалось распознать кнопки! Проверьте формат.")
        return
    
    update_broadcast(bid, buttons=buttons)
    start_broadcast_task(bid)
    
    await state.clear()
    await message.answer(f"✅ Кнопки рассылки #{bid} обновлены ({len(buttons)} шт.)")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# Редактирование даты рассылки
@router.callback_query(F.data.startswith("ebc_date_"))
async def edit_bc_date(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[9:])
    await state.update_data(editing_bc_id=bid)
    await state.set_state(AdminStates.edit_bc_date)
    
    now = datetime.utcnow() + timedelta(hours=3)
    
    await callback.message.edit_text(
        "📅 Выберите новую дату или введите в формате ДД.ММ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"Сегодня ({now.strftime('%d.%m')})", 
                callback_data=f"ebc_d_today_{bid}"
            )],
            [InlineKeyboardButton(
                text=f"Завтра ({(now + timedelta(days=1)).strftime('%d.%m')})", 
                callback_data=f"ebc_d_tomorrow_{bid}"
            )],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"vb_{bid}")]
        ])
    )

@router.callback_query(F.data.startswith("ebc_d_today_"))
async def ebc_date_today(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[12:])
    new_date = (datetime.utcnow() + timedelta(hours=3)).strftime('%Y-%m-%d')
    update_broadcast(bid, scheduled_date=new_date)
    start_broadcast_task(bid)
    
    await state.clear()
    await callback.message.edit_text(f"✅ Дата рассылки #{bid} изменена на сегодня!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("ebc_d_tomorrow_"))
async def ebc_date_tomorrow(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[15:])
    new_date = (datetime.utcnow() + timedelta(hours=3) + timedelta(days=1)).strftime('%Y-%m-%d')
    update_broadcast(bid, scheduled_date=new_date)
    start_broadcast_task(bid)
    
    await state.clear()
    await callback.message.edit_text(f"✅ Дата рассылки #{bid} изменена на завтра!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_bc_date)
async def process_edit_bc_date(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    bid = data.get('editing_bc_id')
    
    if not bid:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    new_date = parse_date(message.text)
    if not new_date:
        await message.answer("❌ Неверный формат! Используйте ДД.ММ")
        return
    
    update_broadcast(bid, scheduled_date=new_date)
    start_broadcast_task(bid)
    
    await state.clear()
    await message.answer(f"✅ Дата рассылки #{bid} изменена на {format_date_display(new_date)}")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# Редактирование времени рассылки
@router.callback_query(F.data.startswith("ebc_time_"))
async def edit_bc_time(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    bid = int(callback.data[9:])
    await state.update_data(editing_bc_id=bid)
    await state.set_state(AdminStates.edit_bc_time)
    
    await callback.message.edit_text(
        "⏰ Выберите новое время или введите в формате ЧЧ:ММ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="09:00", callback_data=f"ebc_t_09:00_{bid}"),
                InlineKeyboardButton(text="12:00", callback_data=f"ebc_t_12:00_{bid}"),
                InlineKeyboardButton(text="15:00", callback_data=f"ebc_t_15:00_{bid}")
            ],
            [
                InlineKeyboardButton(text="18:00", callback_data=f"ebc_t_18:00_{bid}"),
                InlineKeyboardButton(text="20:00", callback_data=f"ebc_t_20:00_{bid}"),
                InlineKeyboardButton(text="22:00", callback_data=f"ebc_t_22:00_{bid}")
            ],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data=f"vb_{bid}")]
        ])
    )

@router.callback_query(F.data.startswith("ebc_t_"))
async def ebc_time_preset(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    # ebc_t_09:00_123 -> извлекаем время и bid
    parts = callback.data[6:].rsplit('_', 1)
    if len(parts) != 2:
        return
    
    time_value = parts[0]
    bid = int(parts[1])
    
    update_broadcast(bid, scheduled_time=time_value)
    start_broadcast_task(bid)
    
    await state.clear()
    await callback.message.edit_text(f"✅ Время рассылки #{bid} изменено на {time_value} МСК!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_bc_time)
async def process_edit_bc_time(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    bid = data.get('editing_bc_id')
    
    if not bid:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    time_text = message.text.strip()
    if not re.match(r'^([01]?[0-9]|2[0-3]):([0-5][0-9])$', time_text):
        await message.answer("❌ Неверный формат! Используйте ЧЧ:ММ")
        return
    
    parts = time_text.split(':')
    time_formatted = f"{int(parts[0]):02d}:{parts[1]}"
    
    update_broadcast(bid, scheduled_time=time_formatted)
    start_broadcast_task(bid)
    
    await state.clear()
    await message.answer(f"✅ Время рассылки #{bid} изменено на {time_formatted} МСК!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# Отправить сейчас
@router.callback_query(F.data.startswith("sb_"))
async def send_bc(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    bid = int(callback.data[3:])
    b = get_broadcast_by_id(bid)
    if not b:
        await callback.message.edit_text("❌ Рассылка не найдена")
        return
    
    # Отменяем запланированную задачу
    if bid in active_tasks:
        try:
            active_tasks[bid].cancel()
        except:
            pass
        del active_tasks[bid]
    
    await callback.message.edit_text("📨 Отправка рассылки...")
    
    message_data = json.loads(b['message_data'])
    buttons = json.loads(b['buttons']) if b.get('buttons') else None
    
    success, failed = await do_broadcast(message_data, buttons)
    mark_broadcast_completed(bid)
    
    await callback.message.edit_text(
        f"✅ Рассылка #{bid} отправлена!\n\n📨 Успешно: {success}\n❌ Ошибок: {failed}"
    )
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# Удалить рассылку
@router.callback_query(F.data.startswith("db_"))
async def del_bc(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    bid = int(callback.data[3:])
    
    # Отменяем задачу если есть
    if bid in active_tasks:
        try:
            active_tasks[bid].cancel()
        except:
            pass
        del active_tasks[bid]
    
    delete_broadcast(bid)
    await callback.message.edit_text(f"🗑 Рассылка #{bid} удалена!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ФИЛЬМЫ ====================
@router.callback_query(F.data == "admin_films_list")
async def admin_films_list(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    films = get_all_films()
    if not films:
        await callback.message.edit_text("📋 Список фильмов пуст", reply_markup=get_back_keyboard())
        return
    
    text = "📋 <b>Список фильмов:</b>\n\n" + "\n".join([
        f"{'🖼' if f.get('image_id') else '📄'} <code>{f.get('code', '')}</code> - {f.get('name', '')}" 
        for f in films
    ])
    
    if len(text) > 4000:
        text = text[:4000] + "\n\n... и ещё"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML)

@router.callback_query(F.data == "admin_add_film")
async def admin_add_film(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.add_film_name)
    await callback.message.edit_text("🎬 Введите название фильма:", reply_markup=get_back_keyboard())

@router.message(AdminStates.add_film_name)
async def process_film_name(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    code = generate_unique_code()
    await state.update_data(film_name=message.text, generated_code=code)
    await state.set_state(AdminStates.add_film_code)
    
    await message.answer(
        f"📝 Сгенерированный код: <code>{code}</code>\n\nИспользовать его или введите свой:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🎲 Использовать {code}", callback_data="use_code")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]), 
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "use_code")
async def use_code(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    data = await state.get_data()
    code = data.get('generated_code')
    
    if not code:
        await state.clear()
        await callback.message.edit_text("❌ Ошибка. Попробуйте заново.")
        return
    
    await state.update_data(film_code=code)
    await state.set_state(AdminStates.add_film_image)
    
    await callback.message.edit_text(
        f"🖼 Отправьте картинку для фильма или пропустите\n\n📝 Код: {code}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )

@router.message(AdminStates.add_film_code)
async def process_film_code(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    code = message.text.strip()
    
    if get_film_by_code(code):
        await message.answer("❌ Фильм с таким кодом уже существует!")
        return
    
    await state.update_data(film_code=code)
    await state.set_state(AdminStates.add_film_image)
    
    await message.answer(
        f"🖼 Отправьте картинку для фильма или пропустите\n\n📝 Код: {code}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ])
    )

@router.callback_query(F.data == "skip_img")
async def skip_img(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    data = await state.get_data()
    code = data.get('film_code')
    name = data.get('film_name')
    
    if not code or not name:
        await state.clear()
        await callback.message.edit_text("❌ Ошибка. Попробуйте заново.")
        return
    
    add_film(code, name)
    await state.clear()
    
    await callback.message.edit_text(
        f"✅ Фильм добавлен!\n\n📝 Код: <code>{code}</code>\n🎬 Название: {name}", 
        parse_mode=ParseMode.HTML
    )
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.add_film_image, F.photo)
async def process_film_img(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    code = data.get('film_code')
    name = data.get('film_name')
    
    if not code or not name:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    image_id = message.photo[-1].file_id
    add_film(code, name, image_id)
    await state.clear()
    
    await message.answer(
        f"✅ Фильм добавлен с картинкой!\n\n📝 Код: <code>{code}</code>\n🎬 Название: {name}", 
        parse_mode=ParseMode.HTML
    )
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_delete_film")
async def admin_delete_film(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    films = get_all_films()
    if not films:
        await callback.message.edit_text("📋 Список фильмов пуст", reply_markup=get_back_keyboard())
        return
    
    buttons = [
        [InlineKeyboardButton(
            text=f"🗑 {f.get('code', '')} - {f.get('name', '')[:20]}", 
            callback_data=f"df_{f.get('code', '')}"
        )] 
        for f in films[:20]
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text(
        "🗑 Выберите фильм для удаления:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data.startswith("df_"))
async def del_film(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    code = callback.data[3:]
    delete_film_by_code(code)
    
    await callback.message.edit_text(f"✅ Фильм с кодом {code} удалён!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== КАНАЛЫ ====================
@router.callback_query(F.data == "admin_add_channels")
async def admin_add_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.set_state(AdminStates.add_channels)
    
    await callback.message.edit_text(
        "📢 Отправьте список каналов в формате:\n\n"
        "<code>1. Название канала (https://t.me/channel)</code>\n"
        "<code>2. С проверкой (https://t.me/channel2) -100123456789</code>\n\n"
        "⚠️ Старые каналы будут заменены новыми!",
        reply_markup=get_back_keyboard(), 
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.add_channels)
async def process_channels(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    channels = parse_channels_text(message.text)
    if not channels:
        await message.answer("❌ Неверный формат! Проверьте и попробуйте снова.")
        return
    
    clear_all_channels()
    for ch in channels:
        add_channel(ch['button_text'], ch['link'], ch['channel_id'])
    
    await state.clear()
    await message.answer(f"✅ Добавлено {len(channels)} каналов!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_view_channels")
async def admin_view_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    channels = get_all_channels()
    if not channels:
        await callback.message.edit_text("📢 Список каналов пуст", reply_markup=get_back_keyboard())
        return
    
    text = "📢 <b>Каналы для подписки:</b>\n\n" + "\n".join([
        f"<b>{c.get('button_text', '')}</b>\n"
        f"🔗 {c.get('link', '')}\n"
        f"{'✓ ID: ' + c['channel_id'] if c.get('channel_id') else '○ Без проверки подписки'}\n" 
        for c in channels
    ])
    
    await callback.message.edit_text(
        text, reply_markup=get_back_keyboard(), 
        parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )

@router.callback_query(F.data == "admin_delete_channels")
async def admin_delete_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    channels = get_all_channels()
    if not channels:
        await callback.message.edit_text("📢 Список каналов пуст", reply_markup=get_back_keyboard())
        return
    
    buttons = [[InlineKeyboardButton(text="🗑 Удалить ВСЕ каналы", callback_data="del_all_ch")]]
    buttons += [
        [InlineKeyboardButton(
            text=f"🗑 {c.get('button_text', '')[:25]}", 
            callback_data=f"dc_{c.get('id', '')}"
        )] 
        for c in channels
    ]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text(
        "🗑 Выберите канал для удаления:", 
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data == "del_all_ch")
async def del_all_ch(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    clear_all_channels()
    
    await callback.message.edit_text("✅ Все каналы удалены!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("dc_"))
async def del_ch(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    channel_id = int(callback.data[3:])
    delete_channel_by_id(channel_id)
    
    await callback.message.edit_text("✅ Канал удалён!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ТЕКСТЫ ====================
@router.callback_query(F.data == "admin_texts")
async def admin_texts(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    await callback.message.edit_text(
        "📝 Выберите текст для редактирования:", 
        reply_markup=get_texts_keyboard()
    )

@router.callback_query(F.data.startswith("edit_text_"))
async def edit_text_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    
    key = callback.data[10:]
    await state.update_data(text_key=key)
    await state.set_state(AdminStates.edit_text_value)
    
    names = {
        'subscribe_required': 'Требование подписки',
        'film_not_found': 'Фильм не найден',
        'ad_text': 'Текст рекламы',
        'search_prompt': 'Текст поиска'
    }
    
    current_text = get_text(key)
    await callback.message.edit_text(
        f"📝 <b>{names.get(key, key)}</b>\n\n"
        f"Текущий текст:\n{current_text}\n\n"
        f"Отправьте новый текст:",
        reply_markup=get_back_keyboard(), 
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_text_value)
async def process_edit_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    key = data.get('text_key')
    
    if not key:
        await state.clear()
        await message.answer("❌ Ошибка. Попробуйте заново.")
        return
    
    update_text(key, message.text)
    await state.clear()
    await message.answer("✅ Текст обновлён!")
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== СТАТИСТИКА ====================
@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.answer()
    await state.clear()
    
    stats = get_users_stats()
    films = get_all_films()
    channels = get_all_channels()
    pending = get_pending_broadcasts()
    now = datetime.utcnow() + timedelta(hours=3)
    
    await callback.message.edit_text(
        f"📊 <b>Статистика бота</b>\n\n"
        f"🕐 Время (МСК): {now.strftime('%d.%m.%Y %H:%M')}\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"   📊 Всего: {stats['total']}\n"
        f"   📅 Сегодня: {stats['today']}\n"
        f"   📆 За неделю: {stats['week']}\n"
        f"   🗓 За месяц: {stats['month']}\n\n"
        f"🎬 Фильмов в базе: {len(films)}\n"
        f"📢 Каналов для подписки: {len(channels)}\n"
        f"📨 Отложенных рассылок: {len(pending)}",
        reply_markup=get_back_keyboard(), 
        parse_mode=ParseMode.HTML
    )

# ==================== ОБРАБОТКА ЛЮБОГО ТЕКСТА ====================
@router.message(~F.text.startswith('/'), StateFilter(None))
async def process_any_text(message: Message, state: FSMContext):
    # Пропускаем известные кнопки меню
    if message.text in ["🔍 Поиск по коду", "🎲 Случайный код", "🔥 Купить рекламу в этом боте"]:
        return
    
    user_id = message.from_user.id
    
    # Помечаем что пользователь не новый
    mark_user_not_new(user_id)
    
    # Проверяем подписку (кроме админа)
    if user_id != ADMIN_ID and not await check_subscription(user_id):
        await send_subscribe_message(user_id)
        return
    
    # Ищем фильм по введённому коду
    code = message.text.strip()
    film = get_film_by_code(code)
    
    if not film:
        await message.answer(get_text('film_not_found'))
        return
    
    text = f"🎬 <b>{film.get('name', 'Неизвестно')}</b>\n\n📝 Код: <code>{code}</code>"
    
    if film.get('image_id'):
        try:
            await message.answer_photo(
                photo=film['image_id'], caption=text, 
                parse_mode=ParseMode.HTML
            )
            return
        except:
            pass
    
    await message.answer(text, parse_mode=ParseMode.HTML)

# ==================== ЗАПУСК БОТА ====================
async def main():
    # Инициализация базы данных
    init_db()
    
    films_count = get_films_count()
    users_count = len(get_all_users())
    logger.info(f"Database: {DB_PATH}")
    logger.info(f"Films: {films_count}, Users: {users_count}")
    
    # Восстанавливаем отложенные рассылки
    await restart_pending_broadcasts()
    
    logger.info("Bot started successfully!")
    
    # Запускаем polling
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
