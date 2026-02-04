import asyncio
import logging
import random
import re
import json
import sqlite3
from datetime import datetime, timedelta
from contextlib import contextmanager
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
DB_PATH = 'bot_database.db'

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()
dp.include_router(router)

active_tasks = {}

# ==================== БАЗА ДАННЫХ ====================
@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
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
        default_settings = {
            'welcome_image': None,
            'instruction_link': 'https://t.me/+fsafas34'
        }
        
        for key, value in default_settings.items():
            cursor.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, value))

# ==================== ФУНКЦИИ БД ====================
def add_user(user_id, username, first_name):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users WHERE user_id = ?', (user_id,))
        if cursor.fetchone() is None:
            cursor.execute('''
                INSERT INTO users (user_id, username, first_name, joined_at) 
                VALUES (?, ?, ?, datetime('now'))
            ''', (user_id, username, first_name))
        else:
            cursor.execute('''
                UPDATE users SET username = ?, first_name = ? WHERE user_id = ?
            ''', (username, first_name, user_id))

def get_all_users():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id FROM users')
        return [row[0] for row in cursor.fetchall()]

def get_users_stats():
    with get_db() as conn:
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM users')
        total = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM users 
            WHERE date(joined_at, '+3 hours') = date('now', '+3 hours')
        ''')
        today = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM users 
            WHERE datetime(joined_at, '+3 hours') >= datetime('now', '+3 hours', '-7 days')
        ''')
        week = cursor.fetchone()[0]
        
        cursor.execute('''
            SELECT COUNT(*) FROM users 
            WHERE datetime(joined_at, '+3 hours') >= datetime('now', '+3 hours', '-30 days')
        ''')
        month = cursor.fetchone()[0]
        
        return {'total': total, 'today': today, 'week': week, 'month': month}

def add_film(code, name, image_id=None):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO films (code, name, image_id) VALUES (?, ?, ?)', (code, name, image_id))

def delete_film_by_code(code):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM films WHERE code = ?', (code,))
        return cursor.rowcount > 0

def get_film_by_code(code):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT name, image_id FROM films WHERE code = ?', (code,))
        return cursor.fetchone()

def get_all_films():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT code, name, image_id FROM films')
        return cursor.fetchall()

def get_random_film():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT code, name, image_id FROM films ORDER BY RANDOM() LIMIT 1')
        return cursor.fetchone()

def generate_unique_code():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT code FROM films')
        existing_codes = [row[0] for row in cursor.fetchall()]
    
    while True:
        code = str(random.randint(1000, 9999))
        if code not in existing_codes:
            return code

def add_channel(button_text, link, channel_id=None):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT INTO channels (button_text, link, channel_id) VALUES (?, ?, ?)', 
                       (button_text, link, channel_id))

def clear_all_channels():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM channels')

def delete_channel_by_id(id):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM channels WHERE id = ?', (id,))
        return cursor.rowcount > 0

def get_all_channels():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, link, channel_id FROM channels')
        return cursor.fetchall()

def get_text(key):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM texts WHERE key = ?', (key,))
        result = cursor.fetchone()
        return result[0] if result else ''

def update_text(key, value):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO texts (key, value) VALUES (?, ?)', (key, value))

def get_setting(key):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        result = cursor.fetchone()
        return result[0] if result else None

def update_setting(key, value):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))

# ==================== ФУНКЦИИ РАССЫЛОК ====================
def save_scheduled_broadcast(message_data, buttons, scheduled_date, scheduled_time):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO scheduled_broadcasts (message_data, buttons, scheduled_date, scheduled_time, status)
            VALUES (?, ?, ?, ?, 'pending')
        ''', (json.dumps(message_data), json.dumps(buttons) if buttons else None, scheduled_date, scheduled_time))
        return cursor.lastrowid

def get_pending_broadcasts():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, message_data, buttons, scheduled_date, scheduled_time, created_at
            FROM scheduled_broadcasts WHERE status = 'pending'
            ORDER BY scheduled_date, scheduled_time
        ''')
        return cursor.fetchall()

def get_broadcast_by_id(broadcast_id):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT id, message_data, buttons, scheduled_date, scheduled_time, status
            FROM scheduled_broadcasts WHERE id = ?
        ''', (broadcast_id,))
        return cursor.fetchone()

def update_broadcast(broadcast_id, message_data=None, buttons=None, scheduled_date=None, scheduled_time=None):
    with get_db() as conn:
        cursor = conn.cursor()
        if message_data is not None:
            cursor.execute('UPDATE scheduled_broadcasts SET message_data = ? WHERE id = ?', 
                          (json.dumps(message_data), broadcast_id))
        if buttons is not None:
            cursor.execute('UPDATE scheduled_broadcasts SET buttons = ? WHERE id = ?', 
                          (json.dumps(buttons) if buttons else None, broadcast_id))
        if scheduled_date is not None:
            cursor.execute('UPDATE scheduled_broadcasts SET scheduled_date = ? WHERE id = ?', 
                          (scheduled_date, broadcast_id))
        if scheduled_time is not None:
            cursor.execute('UPDATE scheduled_broadcasts SET scheduled_time = ? WHERE id = ?', 
                          (scheduled_time, broadcast_id))

def delete_broadcast(broadcast_id):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM scheduled_broadcasts WHERE id = ?', (broadcast_id,))
        return cursor.rowcount > 0

def mark_broadcast_completed(broadcast_id):
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE scheduled_broadcasts SET status = ? WHERE id = ?', ('completed', broadcast_id))

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
        return True, []
    
    not_subscribed = []
    
    for id, button_text, link, channel_id in channels:
        if not channel_id:
            continue
        
        try:
            member = await bot.get_chat_member(chat_id=int(channel_id), user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append((button_text, link))
        except Exception as e:
            logging.error(f"Error checking {channel_id}: {e}")
            continue
    
    return len(not_subscribed) == 0, not_subscribed

def get_subscribe_keyboard():
    channels = get_all_channels()
    buttons = []
    for id, button_text, link, channel_id in channels:
        buttons.append([InlineKeyboardButton(text=button_text, url=link)])
    buttons.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== КЛАВИАТУРЫ ====================
def get_user_reply_keyboard():
    """Reply клавиатура для пользователя (внизу экрана)"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Поиск по коду"), KeyboardButton(text="🎲 Случайный код")],
            [KeyboardButton(text="🔥 Купить рекламу в этом боте")]
        ],
        resize_keyboard=True
    )

def get_welcome_inline_keyboard():
    """Inline кнопки приветствия (2 кнопки)"""
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
        [InlineKeyboardButton(text="🔗 Изменить ссылку инструкции", callback_data="edit_instruction_link")],
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
    keyboard = []
    for btn in buttons:
        keyboard.append([InlineKeyboardButton(text=btn['text'], url=btn['url'])])
    return InlineKeyboardMarkup(inline_keyboard=keyboard) if keyboard else None

def parse_date(date_str):
    date_str = date_str.strip()
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', date_str)
    if match:
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime(year, month, day).strftime('%Y-%m-%d')
        except:
            return None
    
    match = re.match(r'^(\d{1,2})\.(\d{1,2})$', date_str)
    if match:
        day, month = int(match.group(1)), int(match.group(2))
        year = datetime.now().year
        try:
            target = datetime(year, month, day)
            if target.date() < datetime.now().date():
                target = datetime(year + 1, month, day)
            return target.strftime('%Y-%m-%d')
        except:
            return None
    
    return None

def format_date_display(date_str):
    try:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        return dt.strftime('%d.%m.%Y')
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
                await bot.send_photo(
                    user_id, message_data['photo'],
                    caption=message_data.get('caption'),
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
            elif message_data.get('video'):
                await bot.send_video(
                    user_id, message_data['video'],
                    caption=message_data.get('caption'),
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_message(
                    user_id, message_data['text'],
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True
                )
            success += 1
        except Exception as e:
            logging.error(f"Broadcast error for {user_id}: {e}")
            failed += 1
        await asyncio.sleep(0.05)
    
    return success, failed

async def send_preview(chat_id, message_data, buttons=None):
    keyboard = create_inline_keyboard_from_buttons(buttons) if buttons else None
    
    try:
        if message_data.get('photo'):
            await bot.send_photo(
                chat_id, message_data['photo'],
                caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('caption', '')}",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        elif message_data.get('video'):
            await bot.send_video(
                chat_id, message_data['video'],
                caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('caption', '')}",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                chat_id,
                f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{message_data.get('text', '')}",
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        return True
    except Exception as e:
        await bot.send_message(chat_id, f"❌ Ошибка: {e}")
        return False

async def scheduled_broadcast_task(broadcast_id):
    broadcast = get_broadcast_by_id(broadcast_id)
    if not broadcast or broadcast['status'] != 'pending':
        return
    
    scheduled_date = broadcast['scheduled_date']
    scheduled_time = broadcast['scheduled_time']
    
    now_msk = datetime.utcnow() + timedelta(hours=3)
    target_datetime = datetime.strptime(f"{scheduled_date} {scheduled_time}", "%Y-%m-%d %H:%M")
    
    if target_datetime <= now_msk:
        delay = 0
    else:
        delay = (target_datetime - now_msk).total_seconds()
    
    logging.info(f"Broadcast {broadcast_id} scheduled, delay: {delay}s")
    
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
        await bot.send_message(
            ADMIN_ID,
            f"✅ Рассылка #{broadcast_id} выполнена!\n\n"
            f"📅 {format_date_display(scheduled_date)} {scheduled_time} МСК\n"
            f"📨 Успешно: {success}\n❌ Ошибок: {failed}"
        )
    except:
        pass

def start_broadcast_task(broadcast_id):
    if broadcast_id in active_tasks:
        active_tasks[broadcast_id].cancel()
    task = asyncio.create_task(scheduled_broadcast_task(broadcast_id))
    active_tasks[broadcast_id] = task

async def restart_pending_broadcasts():
    broadcasts = get_pending_broadcasts()
    for broadcast in broadcasts:
        start_broadcast_task(broadcast['id'])
    logging.info(f"Restarted {len(broadcasts)} pending broadcasts")

# ==================== ОТПРАВКА ПРИВЕТСТВИЯ ====================
def get_welcome_text():
    """Получает текст приветствия с подставленной ссылкой"""
    text = get_text('welcome')
    instruction_link = get_setting('instruction_link') or 'https://t.me/+fsafas34'
    return text.replace('{instruction_link}', instruction_link)

async def send_welcome_message(user_id):
    """Отправляет приветственное сообщение"""
    welcome_text = get_welcome_text()
    welcome_image = get_setting('welcome_image')
    inline_keyboard = get_welcome_inline_keyboard()
    reply_keyboard = get_user_reply_keyboard()
    
    try:
        if welcome_image:
            await bot.send_photo(
                user_id,
                welcome_image,
                caption=welcome_text,
                reply_markup=inline_keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                user_id,
                welcome_text,
                reply_markup=inline_keyboard,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
        
        # Отправляем сообщение с reply клавиатурой
        await bot.send_message(
            user_id,
            "👇 Меню открыто! Используй кнопки ниже:",
            reply_markup=reply_keyboard
        )
    except Exception as e:
        logging.error(f"Error sending welcome: {e}")

# ==================== ХЕНДЛЕРЫ ПОЛЬЗОВАТЕЛЯ ====================
@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    user_id = message.from_user.id
    add_user(user_id, message.from_user.username, message.from_user.first_name)
    await state.clear()
    
    if user_id == ADMIN_ID:
        await message.answer(
            "👑 Добро пожаловать, Админ!",
            reply_markup=get_admin_keyboard()
        )
        return
    
    is_subscribed, _ = await check_subscription(user_id)
    if not is_subscribed:
        await message.answer(
            get_text('subscribe_required'),
            reply_markup=get_subscribe_keyboard()
        )
        return
    
    await send_welcome_message(user_id)

@router.callback_query(F.data == "check_sub")
async def check_sub_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    is_subscribed, _ = await check_subscription(user_id)
    
    if is_subscribed:
        await callback.message.delete()
        await send_welcome_message(user_id)
    else:
        await callback.answer("❌ Вы не подписаны на все каналы!", show_alert=True)

@router.callback_query(F.data == "search_code")
async def search_code_callback(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await callback.message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            await callback.answer()
            return
    
    await state.set_state(UserStates.waiting_code)
    await callback.message.answer(get_text('search_prompt'))
    await callback.answer()

@router.callback_query(F.data == "open_menu")
async def open_menu_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await callback.message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            await callback.answer()
            return
    
    await callback.message.answer(
        "👇 Меню открыто! Используй кнопки ниже:",
        reply_markup=get_user_reply_keyboard()
    )
    await callback.answer()

# Reply кнопка "Поиск по коду"
@router.message(F.text == "🔍 Поиск по коду")
async def search_button(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            return
    
    await state.set_state(UserStates.waiting_code)
    await message.answer(get_text('search_prompt'))

# Reply кнопка "Случайный код"
@router.message(F.text == "🎲 Случайный код")
async def random_button(message: Message):
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            return
    
    film = get_random_film()
    
    if not film:
        await message.answer(get_text('random_empty'))
        return
    
    code, name, image_id = film
    response_text = f"🎲 <b>Случайный фильм:</b>\n\n🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
    
    if image_id:
        try:
            await message.answer_photo(photo=image_id, caption=response_text, parse_mode=ParseMode.HTML)
        except:
            await message.answer(response_text, parse_mode=ParseMode.HTML)
    else:
        await message.answer(response_text, parse_mode=ParseMode.HTML)

# Reply кнопка "Купить рекламу"
@router.message(F.text == "🔥 Купить рекламу в этом боте")
async def ad_button(message: Message):
    user_id = message.from_user.id
    
    if user_id == ADMIN_ID:
        await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())
        return
    
    is_subscribed, _ = await check_subscription(user_id)
    if not is_subscribed:
        await message.answer(
            get_text('subscribe_required'),
            reply_markup=get_subscribe_keyboard()
        )
        return
    
    await message.answer(get_text('ad_text'))

# Обработка кода в состоянии поиска
@router.message(UserStates.waiting_code)
async def process_search_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            await state.clear()
            return
    
    code = message.text.strip()
    film = get_film_by_code(code)
    
    if film:
        name, image_id = film
        response_text = f"🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
        
        if image_id:
            try:
                await message.answer_photo(photo=image_id, caption=response_text, parse_mode=ParseMode.HTML)
            except:
                await message.answer(response_text, parse_mode=ParseMode.HTML)
        else:
            await message.answer(response_text, parse_mode=ParseMode.HTML)
    else:
        await message.answer(get_text('film_not_found'))
    
    await state.clear()

# Обработка любого текста (код фильма)
@router.message(~F.text.startswith('/'), StateFilter(None))
async def process_code(message: Message):
    user_id = message.from_user.id
    
    # Пропускаем известные кнопки
    if message.text in ["🔍 Поиск по коду", "🎲 Случайный код", "🔥 Купить рекламу в этом боте"]:
        return
    
    if user_id != ADMIN_ID:
        is_subscribed, _ = await check_subscription(user_id)
        if not is_subscribed:
            await message.answer(
                get_text('subscribe_required'),
                reply_markup=get_subscribe_keyboard()
            )
            return
    
    code = message.text.strip()
    film = get_film_by_code(code)
    
    if film:
        name, image_id = film
        response_text = f"🎬 <b>{name}</b>\n\n📝 Код: <code>{code}</code>"
        
        if image_id:
            try:
                await message.answer_photo(photo=image_id, caption=response_text, parse_mode=ParseMode.HTML)
            except:
                await message.answer(response_text, parse_mode=ParseMode.HTML)
        else:
            await message.answer(response_text, parse_mode=ParseMode.HTML)
    else:
        await message.answer(get_text('film_not_found'))

# ==================== АДМИН ПАНЕЛЬ ====================
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

# ==================== НАСТРОЙКИ ПРИВЕТСТВИЯ ====================
@router.callback_query(F.data == "admin_welcome_settings")
async def admin_welcome_settings(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    welcome_image = get_setting('welcome_image')
    instruction_link = get_setting('instruction_link') or 'не задана'
    has_image = "✅ Есть" if welcome_image else "❌ Нет"
    
    await callback.message.edit_text(
        f"👋 <b>Настройки приветствия</b>\n\n"
        f"🖼 Картинка: {has_image}\n"
        f"🔗 Ссылка инструкции: {instruction_link}\n\n"
        f"Кнопки приветствия:\n"
        f"• 🔍 Поиск по коду\n"
        f"• 📖 Открыть меню",
        reply_markup=get_welcome_settings_keyboard(),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "preview_welcome")
async def preview_welcome(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await callback.answer("Отправляю предпросмотр...")
    
    welcome_text = get_welcome_text()
    welcome_image = get_setting('welcome_image')
    inline_keyboard = get_welcome_inline_keyboard()
    
    try:
        if welcome_image:
            await bot.send_photo(
                callback.from_user.id,
                welcome_image,
                caption=f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{welcome_text}",
                reply_markup=inline_keyboard,
                parse_mode=ParseMode.HTML
            )
        else:
            await bot.send_message(
                callback.from_user.id,
                f"👁 <b>ПРЕДПРОСМОТР:</b>\n\n{welcome_text}",
                reply_markup=inline_keyboard,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")

@router.callback_query(F.data == "edit_welcome_text")
async def edit_welcome_text(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    current_text = get_text('welcome')
    await state.set_state(AdminStates.edit_welcome_text)
    
    await callback.message.edit_text(
        f"📝 <b>Текущий текст приветствия:</b>\n\n{current_text}\n\n"
        f"💡 Используйте <code>{{instruction_link}}</code> для вставки ссылки на инструкцию.\n\n"
        f"Отправьте новый текст (можно использовать HTML):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    update_text('welcome', message.text)
    await message.answer("✅ Текст приветствия обновлён!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_welcome_image")
async def edit_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminStates.edit_welcome_image)
    
    current_image = get_setting('welcome_image')
    
    buttons = []
    if current_image:
        buttons.append([InlineKeyboardButton(text="🗑 Удалить картинку", callback_data="remove_welcome_image")])
    buttons.append([InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")])
    
    await callback.message.edit_text(
        "🖼 Отправьте новую картинку для приветствия:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

@router.callback_query(F.data == "remove_welcome_image")
async def remove_welcome_image(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    update_setting('welcome_image', None)
    await callback.message.edit_text("✅ Картинка удалена!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_welcome_image, F.photo)
async def process_welcome_image(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    image_id = message.photo[-1].file_id
    update_setting('welcome_image', image_id)
    
    await message.answer("✅ Картинка приветствия обновлена!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "edit_instruction_link")
async def edit_instruction_link(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    current_link = get_setting('instruction_link') or 'не задана'
    await state.set_state(AdminStates.edit_instruction_link)
    
    await callback.message.edit_text(
        f"🔗 <b>Текущая ссылка на инструкцию:</b>\n{current_link}\n\n"
        f"Отправьте новую ссылку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_welcome_settings")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_instruction_link)
async def process_instruction_link(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    link = message.text.strip()
    update_setting('instruction_link', link)
    
    await message.answer(f"✅ Ссылка обновлена: {link}")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== РАССЫЛКА ====================
@router.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text(
        "📨 <b>Создание рассылки</b>\n\n"
        "Отправьте сообщение (текст, фото или видео):\n\n"
        "💡 HTML: <code>&lt;b&gt;жирный&lt;/b&gt;</code>, <code>&lt;i&gt;курсив&lt;/i&gt;</code>",
        reply_markup=get_back_keyboard(),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.broadcast_message)
async def process_broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    message_data = {}
    if message.photo:
        message_data['photo'] = message.photo[-1].file_id
        message_data['caption'] = message.caption or ""
    elif message.video:
        message_data['video'] = message.video.file_id
        message_data['caption'] = message.caption or ""
    else:
        message_data['text'] = message.text
    
    await state.update_data(message_data=message_data)
    await state.set_state(AdminStates.broadcast_buttons)
    
    await message.answer(
        "🔘 <b>Кнопки-ссылки</b>\n\n"
        "Формат: <code>Текст | https://link.com</code>\n\n"
        "Или нажмите «Без кнопок»",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Без кнопок", callback_data="broadcast_no_buttons")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "broadcast_no_buttons", AdminStates.broadcast_buttons)
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
        await message.answer("❌ Не удалось распознать кнопки!")
        return
    
    await state.update_data(buttons=buttons)
    await message.answer(f"✅ Кнопок: {len(buttons)}")
    await goto_date_selection(message, state)

async def goto_date_selection(message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_date)
    
    now_msk = datetime.utcnow() + timedelta(hours=3)
    today = now_msk.strftime('%d.%m')
    tomorrow = (now_msk + timedelta(days=1)).strftime('%d.%m')
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📅 Сегодня ({today})", callback_data="broadcast_date_today")],
        [InlineKeyboardButton(text=f"📅 Завтра ({tomorrow})", callback_data="broadcast_date_tomorrow")],
        [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data="broadcast_now")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
    ])
    
    await message.answer(
        "📅 <b>Дата рассылки</b>\n\nВведите <code>ДД.ММ</code> или выберите:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "broadcast_date_today", AdminStates.broadcast_date)
async def broadcast_date_today(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    now_msk = datetime.utcnow() + timedelta(hours=3)
    await state.update_data(scheduled_date=now_msk.strftime('%Y-%m-%d'))
    await goto_time_selection(callback.message, state)

@router.callback_query(F.data == "broadcast_date_tomorrow", AdminStates.broadcast_date)
async def broadcast_date_tomorrow(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    tomorrow = datetime.utcnow() + timedelta(hours=3) + timedelta(days=1)
    await state.update_data(scheduled_date=tomorrow.strftime('%Y-%m-%d'))
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
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="09:00", callback_data="broadcast_time_09:00"),
            InlineKeyboardButton(text="12:00", callback_data="broadcast_time_12:00"),
            InlineKeyboardButton(text="15:00", callback_data="broadcast_time_15:00"),
        ],
        [
            InlineKeyboardButton(text="18:00", callback_data="broadcast_time_18:00"),
            InlineKeyboardButton(text="20:00", callback_data="broadcast_time_20:00"),
            InlineKeyboardButton(text="22:00", callback_data="broadcast_time_22:00"),
        ],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_date")],
    ])
    
    await message.answer(
        "⏰ <b>Время (МСК)</b>\n\nВведите <code>ЧЧ:ММ</code> или выберите:",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "back_to_date")
async def back_to_date(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await goto_date_selection(callback.message, state)

@router.callback_query(F.data.startswith("broadcast_time_"), AdminStates.broadcast_time)
async def broadcast_time_preset(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    time = callback.data.replace("broadcast_time_", "")
    await state.update_data(scheduled_time=time)
    await goto_confirmation(callback.message, state)

@router.message(AdminStates.broadcast_time)
async def process_broadcast_time(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    time_text = message.text.strip()
    if not re.match(r'^([01]?[0-9]|2[0-3]):([0-5][0-9])$', time_text):
        await message.answer("❌ Неверный формат!")
        return
    
    parts = time_text.split(':')
    time_text = f"{int(parts[0]):02d}:{parts[1]}"
    
    await state.update_data(scheduled_time=time_text)
    await goto_confirmation(message, state)

async def goto_confirmation(message, state: FSMContext):
    await state.set_state(AdminStates.broadcast_confirm)
    
    data = await state.get_data()
    message_data = data.get('message_data', {})
    buttons = data.get('buttons')
    scheduled_date = data.get('scheduled_date')
    scheduled_time = data.get('scheduled_time')
    
    await send_preview(message.chat.id, message_data, buttons)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="broadcast_confirm")],
        [InlineKeyboardButton(text="📝 Изменить сообщение", callback_data="broadcast_edit_message")],
        [InlineKeyboardButton(text="🔘 Изменить кнопки", callback_data="broadcast_edit_buttons")],
        [InlineKeyboardButton(text="📅 Изменить дату/время", callback_data="back_to_date")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_back")]
    ])
    
    await message.answer(
        f"📋 <b>Подтверждение</b>\n\n"
        f"📅 {format_date_display(scheduled_date)} в {scheduled_time} МСК\n"
        f"👥 Получателей: {len(get_all_users())}\n"
        f"🔘 Кнопок: {len(buttons) if buttons else 0}",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "broadcast_edit_message", AdminStates.broadcast_confirm)
async def broadcast_edit_message(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text("📝 Отправьте новое сообщение:", reply_markup=get_back_keyboard())

@router.callback_query(F.data == "broadcast_edit_buttons", AdminStates.broadcast_confirm)
async def broadcast_edit_buttons(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(AdminStates.broadcast_buttons)
    await callback.message.edit_text(
        "🔘 Отправьте кнопки:\n<code>Текст | https://link.com</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Без кнопок", callback_data="broadcast_no_buttons")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "broadcast_confirm", AdminStates.broadcast_confirm)
async def broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    broadcast_id = save_scheduled_broadcast(
        data['message_data'], data.get('buttons'),
        data['scheduled_date'], data['scheduled_time']
    )
    start_broadcast_task(broadcast_id)
    
    await callback.message.edit_text(
        f"✅ Рассылка #{broadcast_id} запланирована!\n\n"
        f"📅 {format_date_display(data['scheduled_date'])} в {data['scheduled_time']} МСК"
    )
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "broadcast_now")
async def broadcast_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    await callback.message.edit_text("📨 Выполняю рассылку...")
    
    success, failed = await do_broadcast(data.get('message_data', {}), data.get('buttons'))
    
    await callback.message.edit_text(f"✅ Готово!\n📨 Успешно: {success}\n❌ Ошибок: {failed}")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ОТЛОЖЕННЫЕ РАССЫЛКИ ====================
@router.callback_query(F.data == "admin_scheduled")
async def admin_scheduled(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    broadcasts = get_pending_broadcasts()
    
    if not broadcasts:
        await callback.message.edit_text("📋 Нет запланированных рассылок", reply_markup=get_back_keyboard())
        return
    
    text = "📋 <b>Запланированные:</b>\n\n"
    buttons = []
    
    for b in broadcasts:
        b_id = b['id']
        date_display = format_date_display(b['scheduled_date'])
        time = b['scheduled_time']
        text += f"#{b_id} | {date_display} {time}\n"
        buttons.append([InlineKeyboardButton(text=f"#{b_id} - {date_display} {time}", callback_data=f"view_bc_{b_id}")])
    
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode=ParseMode.HTML)

@router.callback_query(F.data.startswith("view_bc_"))
async def view_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    broadcast_id = int(callback.data.replace("view_bc_", ""))
    broadcast = get_broadcast_by_id(broadcast_id)
    
    if not broadcast:
        await callback.answer("Не найдена!", show_alert=True)
        return
    
    message_data = json.loads(broadcast['message_data'])
    buttons = json.loads(broadcast['buttons']) if broadcast['buttons'] else None
    
    await send_preview(callback.message.chat.id, message_data, buttons)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Сообщение", callback_data=f"ebc_msg_{broadcast_id}")],
        [InlineKeyboardButton(text="🔘 Кнопки", callback_data=f"ebc_btn_{broadcast_id}")],
        [InlineKeyboardButton(text="📅 Дата", callback_data=f"ebc_date_{broadcast_id}"),
         InlineKeyboardButton(text="⏰ Время", callback_data=f"ebc_time_{broadcast_id}")],
        [InlineKeyboardButton(text="🚀 Отправить сейчас", callback_data=f"send_bc_{broadcast_id}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_bc_{broadcast_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_scheduled")]
    ])
    
    await callback.message.answer(
        f"📋 <b>Рассылка #{broadcast_id}</b>\n\n"
        f"📅 {format_date_display(broadcast['scheduled_date'])} в {broadcast['scheduled_time']} МСК",
        reply_markup=keyboard,
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data.startswith("ebc_msg_"))
async def edit_bc_msg(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_msg_", ""))
    await state.update_data(editing_broadcast_id=broadcast_id)
    await state.set_state(AdminStates.edit_broadcast_message)
    await callback.message.edit_text("📝 Отправьте новое сообщение:", reply_markup=get_back_keyboard())

@router.message(AdminStates.edit_broadcast_message)
async def process_edit_bc_msg(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    broadcast_id = data['editing_broadcast_id']
    
    message_data = {}
    if message.photo:
        message_data['photo'] = message.photo[-1].file_id
        message_data['caption'] = message.caption or ""
    elif message.video:
        message_data['video'] = message.video.file_id
        message_data['caption'] = message.caption or ""
    else:
        message_data['text'] = message.text
    
    update_broadcast(broadcast_id, message_data=message_data)
    start_broadcast_task(broadcast_id)
    
    await message.answer(f"✅ Рассылка #{broadcast_id} обновлена!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("ebc_btn_"))
async def edit_bc_btn(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_btn_", ""))
    await state.update_data(editing_broadcast_id=broadcast_id)
    await state.set_state(AdminStates.edit_broadcast_buttons)
    await callback.message.edit_text(
        "🔘 Отправьте кнопки:\n<code>Текст | https://link.com</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Без кнопок", callback_data=f"ebc_btn_none_{broadcast_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_scheduled")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data.startswith("ebc_btn_none_"))
async def edit_bc_btn_none(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_btn_none_", ""))
    update_broadcast(broadcast_id, buttons=[])
    start_broadcast_task(broadcast_id)
    await callback.message.edit_text(f"✅ Кнопки удалены!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_broadcast_buttons)
async def process_edit_bc_btn(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    buttons = parse_buttons_text(message.text)
    if not buttons:
        await message.answer("❌ Не удалось распознать!")
        return
    
    update_broadcast(data['editing_broadcast_id'], buttons=buttons)
    start_broadcast_task(data['editing_broadcast_id'])
    
    await message.answer("✅ Кнопки обновлены!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("ebc_date_"))
async def edit_bc_date(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_date_", ""))
    await state.update_data(editing_broadcast_id=broadcast_id)
    await state.set_state(AdminStates.edit_broadcast_date)
    
    now_msk = datetime.utcnow() + timedelta(hours=3)
    today = now_msk.strftime('%d.%m')
    tomorrow = (now_msk + timedelta(days=1)).strftime('%d.%m')
    
    await callback.message.edit_text(
        "📅 Введите дату <code>ДД.ММ</code>:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Сегодня ({today})", callback_data=f"ebc_date_today_{broadcast_id}"),
             InlineKeyboardButton(text=f"Завтра ({tomorrow})", callback_data=f"ebc_date_tomorrow_{broadcast_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_scheduled")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data.startswith("ebc_date_today_"))
async def ebc_date_today(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_date_today_", ""))
    now_msk = datetime.utcnow() + timedelta(hours=3)
    update_broadcast(broadcast_id, scheduled_date=now_msk.strftime('%Y-%m-%d'))
    start_broadcast_task(broadcast_id)
    await callback.message.edit_text("✅ Дата изменена на сегодня!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("ebc_date_tomorrow_"))
async def ebc_date_tomorrow(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_date_tomorrow_", ""))
    tomorrow = datetime.utcnow() + timedelta(hours=3) + timedelta(days=1)
    update_broadcast(broadcast_id, scheduled_date=tomorrow.strftime('%Y-%m-%d'))
    start_broadcast_task(broadcast_id)
    await callback.message.edit_text("✅ Дата изменена на завтра!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_broadcast_date)
async def process_edit_bc_date(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    date = parse_date(message.text)
    if not date:
        await message.answer("❌ Неверный формат!")
        return
    
    data = await state.get_data()
    update_broadcast(data['editing_broadcast_id'], scheduled_date=date)
    start_broadcast_task(data['editing_broadcast_id'])
    
    await message.answer(f"✅ Дата изменена на {format_date_display(date)}!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("ebc_time_"))
async def edit_bc_time(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    broadcast_id = int(callback.data.replace("ebc_time_", ""))
    await state.update_data(editing_broadcast_id=broadcast_id)
    await state.set_state(AdminStates.edit_broadcast_time)
    
    await callback.message.edit_text(
        "⏰ Введите время <code>ЧЧ:ММ</code> (МСК):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="09:00", callback_data=f"ebc_time_set_09:00_{broadcast_id}"),
             InlineKeyboardButton(text="12:00", callback_data=f"ebc_time_set_12:00_{broadcast_id}"),
             InlineKeyboardButton(text="15:00", callback_data=f"ebc_time_set_15:00_{broadcast_id}")],
            [InlineKeyboardButton(text="18:00", callback_data=f"ebc_time_set_18:00_{broadcast_id}"),
             InlineKeyboardButton(text="20:00", callback_data=f"ebc_time_set_20:00_{broadcast_id}"),
             InlineKeyboardButton(text="22:00", callback_data=f"ebc_time_set_22:00_{broadcast_id}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin_scheduled")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data.startswith("ebc_time_set_"))
async def ebc_time_preset(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    parts = callback.data.replace("ebc_time_set_", "").split("_")
    time = parts[0]
    broadcast_id = int(parts[1])
    update_broadcast(broadcast_id, scheduled_time=time)
    start_broadcast_task(broadcast_id)
    await callback.message.edit_text(f"✅ Время изменено на {time} МСК!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.edit_broadcast_time)
async def process_edit_bc_time(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    time_text = message.text.strip()
    if not re.match(r'^([01]?[0-9]|2[0-3]):([0-5][0-9])$', time_text):
        await message.answer("❌ Неверный формат!")
        return
    
    parts = time_text.split(':')
    time_text = f"{int(parts[0]):02d}:{parts[1]}"
    
    data = await state.get_data()
    update_broadcast(data['editing_broadcast_id'], scheduled_time=time_text)
    start_broadcast_task(data['editing_broadcast_id'])
    
    await message.answer(f"✅ Время изменено на {time_text} МСК!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("send_bc_"))
async def send_bc_now(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    broadcast_id = int(callback.data.replace("send_bc_", ""))
    broadcast = get_broadcast_by_id(broadcast_id)
    
    if not broadcast:
        await callback.answer("Не найдена!", show_alert=True)
        return
    
    if broadcast_id in active_tasks:
        active_tasks[broadcast_id].cancel()
        del active_tasks[broadcast_id]
    
    message_data = json.loads(broadcast['message_data'])
    buttons = json.loads(broadcast['buttons']) if broadcast['buttons'] else None
    
    await callback.message.edit_text("📨 Выполняю рассылку...")
    success, failed = await do_broadcast(message_data, buttons)
    mark_broadcast_completed(broadcast_id)
    
    await callback.message.edit_text(f"✅ Готово!\n📨 Успешно: {success}\n❌ Ошибок: {failed}")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("del_bc_"))
async def delete_bc(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    broadcast_id = int(callback.data.replace("del_bc_", ""))
    if broadcast_id in active_tasks:
        active_tasks[broadcast_id].cancel()
        del active_tasks[broadcast_id]
    
    delete_broadcast(broadcast_id)
    await callback.message.edit_text(f"🗑 Рассылка #{broadcast_id} удалена!")
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ФИЛЬМЫ ====================
@router.callback_query(F.data == "admin_films_list")
async def admin_films_list(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    films = get_all_films()
    if not films:
        await callback.message.edit_text("📋 Список пуст", reply_markup=get_back_keyboard())
        return
    
    text = "📋 <b>Фильмы:</b>\n\n"
    for code, name, image_id in films:
        icon = "🖼" if image_id else "📄"
        text += f"{icon} <code>{code}</code> - {name}\n"
    
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
    await state.set_state(AdminStates.add_film_code)
    
    code = generate_unique_code()
    await state.update_data(generated_code=code)
    
    await message.answer(
        f"📝 Введите код или используйте: <code>{code}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"🎲 Использовать {code}", callback_data="use_gen_code")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "use_gen_code")
async def use_gen_code(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    await state.update_data(film_code=data['generated_code'])
    await state.set_state(AdminStates.add_film_image)
    
    await callback.message.edit_text(
        f"🖼 Отправьте картинку или пропустите:\n📝 Код: <code>{data['generated_code']}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.add_film_code)
async def process_film_code(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    code = message.text.strip()
    if get_film_by_code(code):
        await message.answer("❌ Код уже существует!")
        return
    
    await state.update_data(film_code=code)
    await state.set_state(AdminStates.add_film_image)
    
    await message.answer(
        f"🖼 Отправьте картинку:\n📝 Код: <code>{code}</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⏭ Пропустить", callback_data="skip_img")],
            [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin_back")]
        ]),
        parse_mode=ParseMode.HTML
    )

@router.callback_query(F.data == "skip_img")
async def skip_img(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    add_film(data['film_code'], data['film_name'])
    
    await callback.message.edit_text(
        f"✅ Добавлен!\n🎬 {data['film_name']}\n📝 <code>{data['film_code']}</code>",
        parse_mode=ParseMode.HTML
    )
    await state.clear()
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.message(AdminStates.add_film_image, F.photo)
async def process_film_img(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    add_film(data['film_code'], data['film_name'], message.photo[-1].file_id)
    
    await message.answer(
        f"✅ Добавлен с картинкой!\n🎬 {data['film_name']}\n📝 <code>{data['film_code']}</code>",
        parse_mode=ParseMode.HTML
    )
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_delete_film")
async def admin_delete_film(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    films = get_all_films()
    if not films:
        await callback.message.edit_text("📋 Список пуст", reply_markup=get_back_keyboard())
        return
    
    buttons = []
    for code, name, _ in films:
        short = name[:20] + "..." if len(name) > 20 else name
        buttons.append([InlineKeyboardButton(text=f"🗑 {code} - {short}", callback_data=f"delf_{code}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text("🗑 Выберите фильм:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("delf_"))
async def confirm_del_film(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    code = callback.data.replace("delf_", "")
    delete_film_by_code(code)
    await callback.message.edit_text(f"✅ Удалён: {code}")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== КАНАЛЫ ====================
@router.callback_query(F.data == "admin_add_channels")
async def admin_add_channels(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    await state.set_state(AdminStates.add_channels)
    await callback.message.edit_text(
        "📢 <b>Формат:</b>\n"
        "<code>1. Текст (https://ссылка)</code>\n"
        "<code>2. С проверкой (https://t.me/ch) -1001234</code>\n\n"
        "🔄 Старые будут заменены!",
        reply_markup=get_back_keyboard(),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.add_channels)
async def process_channels(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    channels = parse_channels_text(message.text)
    if not channels:
        await message.answer("❌ Не удалось распознать!")
        return
    
    clear_all_channels()
    for ch in channels:
        add_channel(ch['button_text'], ch['link'], ch['channel_id'])
    
    await message.answer(f"✅ Добавлено: {len(channels)}")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data == "admin_view_channels")
async def admin_view_channels(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    channels = get_all_channels()
    if not channels:
        await callback.message.edit_text("📢 Список пуст", reply_markup=get_back_keyboard())
        return
    
    text = "📢 <b>Каналы:</b>\n\n"
    for id, btn, link, ch_id in channels:
        status = f"✓ {ch_id}" if ch_id else "○ без проверки"
        text += f"<b>{btn}</b>\n🔗 {link}\n{status}\n\n"
    
    await callback.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@router.callback_query(F.data == "admin_delete_channels")
async def admin_delete_channels(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    channels = get_all_channels()
    if not channels:
        await callback.message.edit_text("📢 Список пуст", reply_markup=get_back_keyboard())
        return
    
    buttons = [[InlineKeyboardButton(text="🗑 Удалить ВСЕ", callback_data="del_all_ch")]]
    for id, btn, _, _ in channels:
        short = btn[:25] + "..." if len(btn) > 25 else btn
        buttons.append([InlineKeyboardButton(text=f"🗑 {short}", callback_data=f"delch_{id}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="admin_back")])
    
    await callback.message.edit_text("🗑 Выберите:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data == "del_all_ch")
async def del_all_ch(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    clear_all_channels()
    await callback.message.edit_text("✅ Все удалены!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

@router.callback_query(F.data.startswith("delch_"))
async def del_channel(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    delete_channel_by_id(int(callback.data.replace("delch_", "")))
    await callback.message.edit_text("✅ Удалён!")
    await callback.message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== ТЕКСТЫ ====================
@router.callback_query(F.data == "admin_texts")
async def admin_texts(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    await callback.message.edit_text("📝 Выберите текст:", reply_markup=get_texts_keyboard())

@router.callback_query(F.data.startswith("edit_text_"))
async def edit_text_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    
    key = callback.data.replace("edit_text_", "")
    current = get_text(key)
    
    await state.update_data(text_key=key)
    await state.set_state(AdminStates.edit_text_value)
    
    names = {
        'subscribe_required': 'Подписка',
        'film_not_found': 'Не найден',
        'ad_text': 'Реклама',
        'search_prompt': 'Текст поиска'
    }
    
    await callback.message.edit_text(
        f"📝 <b>{names.get(key, key)}</b>\n\nТекущий:\n{current}\n\nОтправьте новый:",
        reply_markup=get_back_keyboard(),
        parse_mode=ParseMode.HTML
    )

@router.message(AdminStates.edit_text_value)
async def process_edit_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    update_text(data['text_key'], message.text)
    
    await message.answer("✅ Обновлено!")
    await state.clear()
    await message.answer("👑 Админ панель:", reply_markup=get_admin_keyboard())

# ==================== СТАТИСТИКА ====================
@router.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    
    stats = get_users_stats()
    films = get_all_films()
    channels = get_all_channels()
    pending = get_pending_broadcasts()
    
    ch_check = sum(1 for _, _, _, ch_id in channels if ch_id)
    now_msk = datetime.utcnow() + timedelta(hours=3)
    
    await callback.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"🕐 {now_msk.strftime('%d.%m.%Y %H:%M')} МСК\n\n"
        f"👥 <b>Пользователи:</b>\n"
        f"   Всего: {stats['total']}\n"
        f"   Сегодня: {stats['today']}\n"
        f"   Неделя: {stats['week']}\n"
        f"   Месяц: {stats['month']}\n\n"
        f"🎬 Фильмов: {len(films)}\n"
        f"📢 Каналов: {len(channels)} (проверка: {ch_check})\n"
        f"📨 Отложенных: {len(pending)}",
        reply_markup=get_back_keyboard(),
        parse_mode=ParseMode.HTML
    )

# ==================== ЗАПУСК ====================
async def main():
    init_db()
    logging.info("БД инициализирована")
    await restart_pending_broadcasts()
    logging.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
