import logging
import asyncio
import time
import os
import re
import sqlite3
import threading
import json
from datetime import datetime
from typing import Optional, Dict
import warnings
warnings.filterwarnings("ignore", message='Field "model_.*".*')

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message, ErrorEvent,
    BotCommand, BotCommandScopeDefault
)
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramForbiddenError, TelegramRetryAfter,
    TelegramConflictError, TelegramBadRequest
)
from aiogram.client.session.aiohttp import AiohttpSession

# ─── إعدادات ──────────────────────────────────────────────────────────────────
FACTORY_TOKEN = os.environ.get("BOT_TOKEN", "8406985927:AAENc1WXjG8Kp9geOwUXyIPLoqQTE49MqwQ")
ADMIN_IDS     = [8774463579, 8605977767]
PORT          = int(os.environ.get("PORT", 8080))
DB_PATH       = os.environ.get("DB_PATH", "bot_data.db")

# بروكسي لحل مشكلة الاتصال
PROXY_URL = os.environ.get("PROXY_URL", None)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[
        logging.FileHandler("bot_errors.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── إنشاء البوت الرئيسي مع بروكسي ─────────────────────────────────────────
def create_bot(token: str) -> Bot:
    if PROXY_URL:
        session = AiohttpSession(proxy=PROXY_URL)
        return Bot(
            token=token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML),
            session=session
        )
    return Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

factory_bot = create_bot(FACTORY_TOKEN)
factory_dp  = Dispatcher(storage=MemoryStorage())

# البوتات الفرعية الجارية
running_bots: Dict[str, asyncio.Task] = {}

# ─── كاش ─────────────────────────────────────────────────────────────────────
_cache:    dict = {}
_cache_ts: dict = {}
CACHE_TTL = 30

def cache_get(key):
    if key in _cache and time.time() - _cache_ts.get(key, 0) < CACHE_TTL:
        return _cache[key]
    return None

def cache_set(key, val):
    _cache[key]    = val
    _cache_ts[key] = time.time()

def cache_del(*keys):
    for k in keys:
        _cache.pop(k, None)
        _cache_ts.pop(k, None)

def cache_clear_all():
    _cache.clear()
    _cache_ts.clear()

# ─── قاعدة البيانات ───────────────────────────────────────────────────────────
_db_lock = threading.Lock()

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    with _db_lock:
        conn = get_conn()
        c = conn.cursor()
        c.executescript("""
            -- مستخدمو صانع البوتات
            CREATE TABLE IF NOT EXISTS factory_users (
                user_id   INTEGER PRIMARY KEY,
                username  TEXT,
                joined_at TEXT,
                is_active INTEGER DEFAULT 1
            );

            -- البوتات المصنوعة
            CREATE TABLE IF NOT EXISTS bots (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                token        TEXT UNIQUE,
                owner_id     INTEGER,
                owner_name   TEXT,
                bot_type     TEXT,
                bot_username TEXT,
                bot_name     TEXT,
                created_at   TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime')),
                is_active    INTEGER DEFAULT 1
            );

            -- إعدادات صانع البوتات
            CREATE TABLE IF NOT EXISTS factory_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            -- قنوات الاشتراك الإجباري لصانع البوتات (تنطبق على كل البوتات)
            CREATE TABLE IF NOT EXISTS factory_channels (
                id       TEXT PRIMARY KEY,
                url      TEXT,
                username TEXT
            );

            -- ========= جداول بوت الملازم =========
            -- مستخدمو البوتات الفرعية
            CREATE TABLE IF NOT EXISTS sub_users (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_token TEXT,
                user_id   INTEGER,
                username  TEXT,
                joined_at TEXT,
                is_active INTEGER DEFAULT 1,
                UNIQUE(bot_token, user_id)
            );

            -- أقسام بوت الملازم
            CREATE TABLE IF NOT EXISTS sub_sections (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_token TEXT,
                name      TEXT,
                parent_id INTEGER DEFAULT NULL
            );

            -- محتوى بوت الملازم
            CREATE TABLE IF NOT EXISTS sub_content (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_token  TEXT,
                section_id INTEGER,
                name       TEXT,
                type       TEXT,
                data       TEXT,
                file_id    TEXT,
                pinned     INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime'))
            );

            -- إعدادات البوتات الفرعية
            CREATE TABLE IF NOT EXISTS sub_settings (
                bot_token TEXT,
                key       TEXT,
                value     TEXT,
                PRIMARY KEY (bot_token, key)
            );

            -- قنوات الاشتراك الإجباري للبوتات الفرعية
            CREATE TABLE IF NOT EXISTS sub_channels (
                bot_token TEXT,
                id        TEXT,
                url       TEXT,
                username  TEXT,
                PRIMARY KEY (bot_token, id)
            );

            -- مشرفو البوتات الفرعية
            CREATE TABLE IF NOT EXISTS sub_admins (
                bot_token TEXT,
                user_id   INTEGER,
                PRIMARY KEY (bot_token, user_id)
            );

            -- ========= جداول بوت التواصل =========
            CREATE TABLE IF NOT EXISTS contact_messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_token  TEXT,
                user_id    INTEGER,
                username   TEXT,
                full_name  TEXT,
                message    TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime')),
                answered   INTEGER DEFAULT 0
            );

            -- طلبات الملازم
            CREATE TABLE IF NOT EXISTS sub_requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_token  TEXT,
                user_id    INTEGER,
                username   TEXT,
                full_name  TEXT,
                message    TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime')),
                answered   INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_sub_sections_token ON sub_sections(bot_token);
            CREATE INDEX IF NOT EXISTS idx_sub_content_token  ON sub_content(bot_token);
            CREATE INDEX IF NOT EXISTS idx_sub_users_token    ON sub_users(bot_token);
        """)

        # إعدادات افتراضية
        for k, v in [
            ("sub_notify",   "OFF"),
            ("entry_notify", "OFF"),
            ("help_text",    "🤖 <b>مرحباً بك في صانع البوتات الدراسية!</b>\n\nيمكنك من هنا إنشاء بوت دراسي خاص بك في دقائق معدودة."),
        ]:
            c.execute("INSERT OR IGNORE INTO factory_settings (key,value) VALUES (?,?)", (k, v))

        conn.commit()
        conn.close()
    logger.warning("✅ قاعدة البيانات جاهزة")

# ─── دوال قاعدة البيانات ─────────────────────────────────────────────────────
async def db_read(query: str, params=(), one=False):
    loop = asyncio.get_event_loop()
    def _read():
        with _db_lock:
            conn = get_conn()
            try:
                c = conn.execute(query, params)
                rows = c.fetchone() if one else c.fetchall()
                if one:
                    return dict(rows) if rows else None
                return [dict(r) for r in rows]
            finally:
                conn.close()
    return await loop.run_in_executor(None, _read)

async def db_write(query: str, params=(), ret_id=False):
    loop = asyncio.get_event_loop()
    def _write():
        with _db_lock:
            conn = get_conn()
            try:
                c = conn.execute(query, params)
                last_id = c.lastrowid
                conn.commit()
                return last_id if ret_id else None
            finally:
                conn.close()
    return await loop.run_in_executor(None, _write)

# ─── HTTP Health Check ────────────────────────────────────────────────────────
async def health_handler(request):
    active = len(running_bots)
    return web.Response(text=f"OK - Factory Bot Running - Active sub-bots: {active}")

async def start_health_server():
    app = web.Application()
    app.router.add_get("/",       health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    for attempt in range(10):
        try:
            site = web.TCPSite(runner, "0.0.0.0", PORT)
            await site.start()
            logger.warning(f"✅ HTTP server على 0.0.0.0:{PORT}")
            return
        except OSError as e:
            logger.warning(f"⚠️ محاولة {attempt+1}/10: {e}")
            await asyncio.sleep(3)
    logger.error("❌ فشل تشغيل health server")

# ─── مساعدات عامة ─────────────────────────────────────────────────────────────
async def get_bot_info_safe(token: str):
    """جلب معلومات البوت بأمان"""
    try:
        tmp_bot = create_bot(token)
        me = await tmp_bot.get_me()
        await tmp_bot.session.close()
        return me
    except Exception as e:
        logger.error(f"get_bot_info: {e}")
        return None

async def get_chat_id_direct(username: str, token: str = None):
    """جلب معرف القناة"""
    use_token = token or FACTORY_TOKEN
    url = f"https://api.telegram.org/bot{use_token}/getChat"
    try:
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(url, data={"chat_id": f"@{username}"}, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                res = await resp.json()
                if res.get("ok"):
                    return res["result"]["id"], res["result"].get("title", username)
                return None, res.get("description", "Unknown error")
    except Exception as e:
        return None, str(e)

# ─── حالات صانع البوتات ───────────────────────────────────────────────────────
class FactorySt(StatesGroup):
    waiting_token       = State()
    waiting_bot_type    = State()
    broadcast_all       = State()
    add_factory_channel = State()
    edit_factory_help   = State()

# ─── لوحة تحكم صانع البوتات ──────────────────────────────────────────────────
def factory_admin_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📊 الإحصائيات",           callback_data="fadm_stats"))
    b.row(
        InlineKeyboardButton(text="🤖 إدارة البوتات",         callback_data="fadm_bots"),
        InlineKeyboardButton(text="📣 إذاعة للكل",            callback_data="fadm_bc"),
    )
    b.row(
        InlineKeyboardButton(text="📢 اشتراك إجباري عام",     callback_data="fadm_force"),
        InlineKeyboardButton(text="🔔 التنبيهات",             callback_data="fadm_notify"),
    )
    b.row(InlineKeyboardButton(text="📝 تعديل رسالة الترحيب", callback_data="fadm_help"))
    return b.as_markup()

# ─── /start الرئيسي ──────────────────────────────────────────────────────────
@factory_dp.message(Command("start"))
async def factory_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    existing = await db_read("SELECT user_id FROM factory_users WHERE user_id=?", (u.id,), one=True)
    if not existing:
        await db_write(
            "INSERT OR IGNORE INTO factory_users (user_id,username,joined_at) VALUES (?,?,?)",
            (u.id, u.username, datetime.now().isoformat())
        )
        # إشعار المطورين
        notify = await db_read("SELECT value FROM factory_settings WHERE key='entry_notify'", one=True)
        if notify and notify["value"] == "ON":
            for aid in ADMIN_IDS:
                try:
                    await factory_bot.send_message(
                        aid,
                        f"👤 <b>مستخدم جديد:</b>\n{u.full_name}\n@{u.username or '—'}\n<code>{u.id}</code>"
                    )
                except:
                    pass

    welcome = (
        "🤖 <b>مرحباً بك في صانع البوتات الدراسية!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✨ <b>ما الذي يمكنك فعله هنا؟</b>\n\n"
        "🔹 <b>بوت الملازم:</b> بوت دراسي متكامل يمكنك من خلاله رفع الملازم والملخصات والمحتوى الدراسي وتنظيمه في أقسام\n\n"
        "🔹 <b>بوت التواصل:</b> بوت خاص للتواصل مع طلابك أو متابعيك بشكل منظم واحترافي\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ كل ما تحتاجه هو <b>توكن البوت</b> فقط!\n\n"
        "💡 <b>تريد بوتاً فريداً ومخصصاً؟</b>\n"
        "تواصل مع المطور: @YEYYL\n\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🆕 صنع بوت جديد",         callback_data="create_new_bot"))
    b.row(InlineKeyboardButton(text="🤖 بوتاتي",               callback_data="my_bots"))
    b.row(InlineKeyboardButton(text="📢 قناة المطور @YEYYF",   url="https://t.me/YEYYF"))

    if u.id in ADMIN_IDS:
        b.row(InlineKeyboardButton(text="🛠 لوحة التحكم السرية", callback_data="secret_panel"))

    await msg.answer(welcome, reply_markup=b.as_markup())

@factory_dp.callback_query(F.data == "main_menu")
async def back_to_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.delete()
    class FakeMsg:
        from_user = cb.from_user
        async def answer(self, *a, **kw):
            return await cb.message.answer(*a, **kw)
    await factory_start(FakeMsg(), state)

# ─── إنشاء بوت جديد ──────────────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "create_new_bot")
async def cb_create_new_bot(cb: CallbackQuery, state: FSMContext):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📚 بوت الملازم",   callback_data="select_type_study"))
    b.row(InlineKeyboardButton(text="💬 بوت التواصل",  callback_data="select_type_contact"))
    b.row(InlineKeyboardButton(text="🔙 رجوع",         callback_data="go_back_main"))
    try:
        await cb.message.edit_text(
            "🤖 <b>اختر نوع البوت:</b>\n\n"
            "📚 <b>بوت الملازم:</b> لرفع وتنظيم المواد الدراسية\n"
            "💬 <b>بوت التواصل:</b> للتواصل مع الطلاب والمتابعين",
            reply_markup=b.as_markup()
        )
    except:
        await cb.message.answer(
            "🤖 <b>اختر نوع البوت:</b>",
            reply_markup=b.as_markup()
        )

@factory_dp.callback_query(F.data.startswith("select_type_"))
async def cb_select_type(cb: CallbackQuery, state: FSMContext):
    bot_type = "study" if "study" in cb.data else "contact"
    await state.update_data(bot_type=bot_type)
    type_name = "📚 بوت الملازم" if bot_type == "study" else "💬 بوت التواصل"

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔙 رجوع", callback_data="create_new_bot"))

    try:
        await cb.message.edit_text(
            f"✅ اخترت: <b>{type_name}</b>\n\n"
            "🔑 <b>أرسل توكن البوت:</b>\n\n"
            "📌 للحصول على التوكن:\n"
            "1️⃣ افتح @BotFather\n"
            "2️⃣ أرسل /newbot\n"
            "3️⃣ اتبع التعليمات\n"
            "4️⃣ انسخ التوكن وأرسله هنا\n\n"
            "⚠️ <b>ملاحظة:</b> تأكد أن البوت لم يُستخدم مسبقاً",
            reply_markup=b.as_markup()
        )
    except:
        pass

    await state.set_state(FactorySt.waiting_token)

@factory_dp.message(FactorySt.waiting_token)
async def process_token(msg: Message, state: FSMContext):
    token = msg.text.strip()
    if not re.match(r'^\d+:[\w-]{35,}$', token):
        return await msg.answer(
            "❌ التوكن غير صحيح!\n"
            "يجب أن يكون بالشكل: <code>123456789:ABCdef...</code>"
        )

    # التحقق من عدم التكرار
    existing = await db_read("SELECT id FROM bots WHERE token=?", (token,), one=True)
    if existing:
        return await msg.answer(
            "❌ هذا التوكن مسجل مسبقاً!\n"
            "إذا كنت تريد إعادة تشغيله اضغط على <b>بوتاتي</b>"
        )

    wm = await msg.answer("⏳ جاري التحقق من التوكن...")
    me = await get_bot_info_safe(token)
    if not me:
        try:
            await wm.delete()
        except:
            pass
        return await msg.answer(
            "❌ التوكن غير صالح أو البوت محظور!\n"
            "تأكد من صحة التوكن وحاول مرة أخرى."
        )

    data = await state.get_data()
    bot_type = data.get("bot_type", "study")
    await state.clear()

    # حفظ البوت في قاعدة البيانات
    await db_write(
        "INSERT INTO bots (token,owner_id,owner_name,bot_type,bot_username,bot_name) VALUES (?,?,?,?,?,?)",
        (token, msg.from_user.id, msg.from_user.full_name, bot_type,
         me.username, me.full_name)
    )

    # إضافة إعدادات افتراضية
    type_name = "بوت الملازم" if bot_type == "study" else "بوت التواصل"
    default_help = (
        f"🎓 <b>مرحباً بك في {me.full_name}!</b>\n\n"
        f"هذا البوت تم إنشاؤه بواسطة @YIYYFBOT"
    )
    for k, v in [("help_text", default_help), ("sub_notify", "OFF"), ("entry_notify", "OFF")]:
        await db_write(
            "INSERT OR IGNORE INTO sub_settings (bot_token,key,value) VALUES (?,?,?)",
            (token, k, v)
        )

    # إنشاء أقسام افتراضية لبوت الملازم
    if bot_type == "study":
        for sec in ["الملازم", "التحفيز", "ارشادات للدراسة", "الملخصات"]:
            await db_write(
                "INSERT INTO sub_sections (bot_token,name) VALUES (?,?)",
                (token, sec)
            )

    try:
        await wm.delete()
    except:
        pass

    type_emoji = "📚" if bot_type == "study" else "💬"
    await msg.answer(
        f"✅ <b>تم إنشاء البوت بنجاح!</b>\n\n"
        f"{type_emoji} <b>النوع:</b> {type_name}\n"
        f"🤖 <b>البوت:</b> @{me.username}\n"
        f"🆔 <b>المعرف:</b> <code>{me.id}</code>\n\n"
        f"🚀 <b>جاري تشغيل البوت...</b>"
    )

    # إشعار المطورين
    for aid in ADMIN_IDS:
        try:
            await factory_bot.send_message(
                aid,
                f"🆕 <b>بوت جديد!</b>\n"
                f"👤 {msg.from_user.full_name} (@{msg.from_user.username or '—'})\n"
                f"{type_emoji} {type_name}\n"
                f"🤖 @{me.username}"
            )
        except:
            pass

    # تشغيل البوت
    await start_sub_bot(token, bot_type)

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text=f"🤖 فتح @{me.username}", url=f"https://t.me/{me.username}"))
    b.row(InlineKeyboardButton(text="🔙 القائمة الرئيسية", callback_data="go_back_main"))
    await msg.answer(
        f"🎉 <b>البوت جاهز!</b>\n"
        f"افتح @{me.username} وأرسل /start",
        reply_markup=b.as_markup()
    )

# ─── بوتاتي ───────────────────────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "my_bots")
async def cb_my_bots(cb: CallbackQuery):
    bots = await db_read(
        "SELECT * FROM bots WHERE owner_id=? ORDER BY id DESC",
        (cb.from_user.id,)
    )
    b = InlineKeyboardBuilder()
    if not bots:
        b.row(InlineKeyboardButton(text="🆕 صنع بوت جديد", callback_data="create_new_bot"))
        b.row(InlineKeyboardButton(text="🔙 رجوع",         callback_data="go_back_main"))
        try:
            await cb.message.edit_text("📭 لا يوجد لديك بوتات بعد!", reply_markup=b.as_markup())
        except:
            await cb.message.answer("📭 لا يوجد لديك بوتات بعد!", reply_markup=b.as_markup())
        return

    text = f"🤖 <b>بوتاتك ({len(bots)}):</b>\n"
    for bot_rec in bots:
        type_icon = "📚" if bot_rec["bot_type"] == "study" else "💬"
        status = "🟢" if bot_rec["token"] in running_bots else "🔴"
        b.row(InlineKeyboardButton(
            text=f"{status} {type_icon} {bot_rec['bot_name'] or bot_rec['bot_username']}",
            callback_data=f"manage_bot_{bot_rec['id']}"
        ))

    b.row(InlineKeyboardButton(text="🆕 صنع بوت جديد", callback_data="create_new_bot"))
    b.row(InlineKeyboardButton(text="🔙 رجوع",         callback_data="go_back_main"))
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

@factory_dp.callback_query(F.data.startswith("manage_bot_"))
async def cb_manage_bot(cb: CallbackQuery):
    bot_id  = int(cb.data.split("manage_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ البوت غير موجود", show_alert=True)

    # التحقق من الصلاحية
    is_owner = bot_rec["owner_id"] == cb.from_user.id
    is_admin = cb.from_user.id in ADMIN_IDS
    if not is_owner and not is_admin:
        return await cb.answer("❌ لا صلاحية", show_alert=True)

    token     = bot_rec["token"]
    is_run    = token in running_bots
    type_icon = "📚" if bot_rec["bot_type"] == "study" else "💬"
    status    = "🟢 يعمل" if is_run else "🔴 متوقف"

    b = InlineKeyboardBuilder()
    if is_run:
        b.row(InlineKeyboardButton(text="⏹ إيقاف البوت",     callback_data=f"stop_bot_{bot_id}"))
    else:
        b.row(InlineKeyboardButton(text="▶️ تشغيل البوت",    callback_data=f"run_bot_{bot_id}"))

    b.row(InlineKeyboardButton(text=f"🤖 فتح @{bot_rec['bot_username']}", url=f"https://t.me/{bot_rec['bot_username']}"))
    b.row(InlineKeyboardButton(text="🗑 حذف البوت",           callback_data=f"delete_bot_{bot_id}"))
    b.row(InlineKeyboardButton(text="🔙 رجوع",                callback_data="my_bots"))

    text = (
        f"{type_icon} <b>{bot_rec['bot_name']}</b>\n"
        f"━━━━━━━━━━━━\n"
        f"📊 الحالة: {status}\n"
        f"🆔 @{bot_rec['bot_username']}\n"
        f"📅 {bot_rec['created_at']}"
    )
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

@factory_dp.callback_query(F.data.startswith("run_bot_"))
async def cb_run_bot(cb: CallbackQuery):
    bot_id  = int(cb.data.split("run_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)

    token = bot_rec["token"]
    if token in running_bots:
        return await cb.answer("✅ البوت يعمل بالفعل", show_alert=True)

    await start_sub_bot(token, bot_rec["bot_type"])
    await cb.answer("✅ تم تشغيل البوت")
    await cb_manage_bot(cb)

@factory_dp.callback_query(F.data.startswith("stop_bot_"))
async def cb_stop_bot(cb: CallbackQuery):
    bot_id  = int(cb.data.split("stop_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)

    token = bot_rec["token"]
    await stop_sub_bot(token)
    await cb.answer("⏹ تم الإيقاف")
    await cb_manage_bot(cb)

@factory_dp.callback_query(F.data.startswith("delete_bot_"))
async def cb_delete_bot(cb: CallbackQuery):
    bot_id  = int(cb.data.split("delete_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)

    is_owner = bot_rec["owner_id"] == cb.from_user.id
    is_admin_user = cb.from_user.id in ADMIN_IDS
    if not is_owner and not is_admin_user:
        return await cb.answer("❌ لا صلاحية", show_alert=True)

    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ نعم، احذف", callback_data=f"confirm_del_bot_{bot_id}"),
        InlineKeyboardButton(text="❌ إلغاء",     callback_data=f"manage_bot_{bot_id}"),
    )
    try:
        await cb.message.edit_text(
            f"⚠️ حذف بوت <b>{bot_rec['bot_name']}</b>؟\n"
            "سيتم حذف كل بياناته نهائياً!",
            reply_markup=b.as_markup()
        )
    except:
        pass

@factory_dp.callback_query(F.data.startswith("confirm_del_bot_"))
async def cb_confirm_del_bot(cb: CallbackQuery):
    bot_id  = int(cb.data.split("confirm_del_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)

    token = bot_rec["token"]
    await stop_sub_bot(token)

    # حذف كل البيانات
    for tbl in ["sub_users","sub_sections","sub_content","sub_settings","sub_channels","sub_admins","contact_messages","sub_requests"]:
        await db_write(f"DELETE FROM {tbl} WHERE bot_token=?", (token,))
    await db_write("DELETE FROM bots WHERE id=?", (bot_id,))

    await cb.answer("✅ تم الحذف نهائياً")
    await cb_my_bots(cb)

@factory_dp.callback_query(F.data == "go_back_main")
async def cb_go_back_main(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await cb.message.delete()
    except:
        pass

    u = cb.from_user
    welcome = (
        "🤖 <b>مرحباً بك في صانع البوتات الدراسية!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "✨ <b>ما الذي يمكنك فعله هنا؟</b>\n\n"
        "🔹 <b>بوت الملازم:</b> بوت دراسي متكامل لرفع وتنظيم المحتوى الدراسي\n\n"
        "🔹 <b>بوت التواصل:</b> بوت احترافي للتواصل مع طلابك\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ كل ما تحتاجه هو <b>توكن البوت</b> فقط!\n\n"
        "💡 <b>تريد بوتاً فريداً ومخصصاً؟</b>\n"
        "تواصل مع المطور: @YEYYL\n\n"
        "━━━━━━━━━━━━━━━━━━━━"
    )

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🆕 صنع بوت جديد",         callback_data="create_new_bot"))
    b.row(InlineKeyboardButton(text="🤖 بوتاتي",               callback_data="my_bots"))
    b.row(InlineKeyboardButton(text="📢 قناة المطور @YEYYF",   url="https://t.me/YEYYF"))
    if u.id in ADMIN_IDS:
        b.row(InlineKeyboardButton(text="🛠 لوحة التحكم السرية", callback_data="secret_panel"))

    await cb.message.answer(welcome, reply_markup=b.as_markup())

# ─── لوحة التحكم السرية (للمطورين فقط) ──────────────────────────────────────
@factory_dp.callback_query(F.data == "secret_panel")
async def cb_secret_panel(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    try:
        await cb.message.edit_text(
            "🛠 <b>لوحة التحكم السرية</b>",
            reply_markup=factory_admin_kb()
        )
    except:
        await cb.message.answer(
            "🛠 <b>لوحة التحكم السرية</b>",
            reply_markup=factory_admin_kb()
        )

@factory_dp.callback_query(F.data == "fadm_back")
async def cb_fadm_back(cb: CallbackQuery):
    try:
        await cb.message.edit_text(
            "🛠 <b>لوحة التحكم السرية</b>",
            reply_markup=factory_admin_kb()
        )
    except:
        await cb.message.answer(
            "🛠 <b>لوحة التحكم السرية</b>",
            reply_markup=factory_admin_kb()
        )

# ─── إحصائيات صانع البوتات ────────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_stats")
async def cb_fadm_stats(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)

    total_users  = await db_read("SELECT COUNT(*) c FROM factory_users", one=True)
    total_bots   = await db_read("SELECT COUNT(*) c FROM bots", one=True)
    study_bots   = await db_read("SELECT COUNT(*) c FROM bots WHERE bot_type='study'", one=True)
    contact_bots = await db_read("SELECT COUNT(*) c FROM bots WHERE bot_type='contact'", one=True)
    active_now   = len(running_bots)
    total_msgs   = await db_read("SELECT COUNT(*) c FROM contact_messages", one=True)
    total_reqs   = await db_read("SELECT COUNT(*) c FROM sub_requests", one=True)

    text = (
        f"📊 <b>إحصائيات صانع البوتات:</b>\n\n"
        f"👥 مستخدمو الصانع: <b>{total_users['c'] if total_users else 0}</b>\n"
        f"🤖 إجمالي البوتات: <b>{total_bots['c'] if total_bots else 0}</b>\n"
        f"  📚 بوتات الملازم: <b>{study_bots['c'] if study_bots else 0}</b>\n"
        f"  💬 بوتات التواصل: <b>{contact_bots['c'] if contact_bots else 0}</b>\n"
        f"🟢 يعمل الآن: <b>{active_now}</b>\n"
        f"💬 رسائل التواصل: <b>{total_msgs['c'] if total_msgs else 0}</b>\n"
        f"📩 طلبات الملازم: <b>{total_reqs['c'] if total_reqs else 0}</b>"
    )

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="fadm_back"))
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

# ─── إدارة البوتات (للمطورين) ────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_bots")
async def cb_fadm_bots(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)

    all_bots = await db_read("SELECT * FROM bots ORDER BY id DESC LIMIT 20")
    b = InlineKeyboardBuilder()
    for bot_rec in (all_bots or []):
        type_icon = "📚" if bot_rec["bot_type"] == "study" else "💬"
        status    = "🟢" if bot_rec["token"] in running_bots else "🔴"
        b.row(InlineKeyboardButton(
            text=f"{status}{type_icon} {bot_rec['bot_name']}",
            callback_data=f"fadm_bot_{bot_rec['id']}"
        ))

    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="fadm_back"))
    try:
        await cb.message.edit_text(
            f"🤖 <b>كل البوتات ({len(all_bots or [])}):</b>",
            reply_markup=b.as_markup()
        )
    except:
        await cb.message.answer(
            f"🤖 <b>كل البوتات:</b>",
            reply_markup=b.as_markup()
        )

@factory_dp.callback_query(F.data.startswith("fadm_bot_"))
async def cb_fadm_bot(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)

    bot_id  = int(cb.data.split("fadm_bot_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)

    token     = bot_rec["token"]
    is_run    = token in running_bots
    type_icon = "📚" if bot_rec["bot_type"] == "study" else "💬"

    # عدد المستخدمين
    users_count = await db_read(
        "SELECT COUNT(*) c FROM sub_users WHERE bot_token=?", (token,), one=True
    )

    b = InlineKeyboardBuilder()
    if is_run:
        b.row(InlineKeyboardButton(text="⏹ إيقاف",   callback_data=f"fadm_stop_{bot_id}"))
    else:
        b.row(InlineKeyboardButton(text="▶️ تشغيل",  callback_data=f"fadm_run_{bot_id}"))
    b.row(InlineKeyboardButton(text="🔙 عودة",       callback_data="fadm_bots"))

    text = (
        f"{type_icon} <b>{bot_rec['bot_name']}</b>\n"
        f"━━━━━━━━━━━━\n"
        f"📊 {'🟢 يعمل' if is_run else '🔴 متوقف'}\n"
        f"👤 المالك: {bot_rec['owner_name']}\n"
        f"👥 المستخدمون: {users_count['c'] if users_count else 0}\n"
        f"🆔 @{bot_rec['bot_username']}\n"
        f"📅 {bot_rec['created_at']}"
    )
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

@factory_dp.callback_query(F.data.startswith("fadm_run_"))
async def cb_fadm_run(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    bot_id  = int(cb.data.split("fadm_run_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)
    await start_sub_bot(bot_rec["token"], bot_rec["bot_type"])
    await cb.answer("✅ تم التشغيل")
    await cb_fadm_bot(cb)

@factory_dp.callback_query(F.data.startswith("fadm_stop_"))
async def cb_fadm_stop(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    bot_id  = int(cb.data.split("fadm_stop_")[1])
    bot_rec = await db_read("SELECT * FROM bots WHERE id=?", (bot_id,), one=True)
    if not bot_rec:
        return await cb.answer("❌ غير موجود", show_alert=True)
    await stop_sub_bot(bot_rec["token"])
    await cb.answer("⏹ تم الإيقاف")
    await cb_fadm_bot(cb)

# ─── إذاعة لكل مستخدمي جميع البوتات ──────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_bc")
async def cb_fadm_bc(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    await cb.message.answer("📣 أرسل رسالة الإذاعة لكل مستخدمي جميع البوتات:")
    await state.set_state(FactorySt.broadcast_all)

@factory_dp.message(FactorySt.broadcast_all)
async def process_fadm_bc(msg: Message, state: FSMContext):
    await state.clear()

    # جمع كل المستخدمين
    factory_users = await db_read("SELECT user_id FROM factory_users WHERE is_active=1")
    sub_users_raw = await db_read("SELECT DISTINCT user_id FROM sub_users WHERE is_active=1")

    all_users = set()
    for u in (factory_users or []):
        all_users.add(u["user_id"])
    for u in (sub_users_raw or []):
        all_users.add(u["user_id"])

    total   = len(all_users)
    sm      = await msg.answer(f"📣 جاري الإذاعة لـ <b>{total}</b> مستخدم...")
    success = 0
    failed  = 0

    for uid in all_users:
        try:
            await msg.copy_to(uid)
            success += 1
        except TelegramForbiddenError:
            failed += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await msg.copy_to(uid)
                success += 1
            except:
                failed += 1
        except:
            failed += 1
        await asyncio.sleep(0.05)

    try:
        await sm.edit_text(
            f"✅ <b>انتهت الإذاعة:</b>\n"
            f"📤 نجح: <b>{success}</b>\n"
            f"❌ فشل: <b>{failed}</b>"
        )
    except:
        pass

# ─── الاشتراك الإجباري العام ──────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_force")
async def cb_fadm_force(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)

    chans = await db_read("SELECT * FROM factory_channels")
    b = InlineKeyboardBuilder()
    for c in (chans or []):
        b.row(InlineKeyboardButton(
            text=f"❌ {c['username'] or c['id']}",
            callback_data=f"fdel_chan_{c['id']}"
        ))
    b.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="fadd_chan"))
    b.row(InlineKeyboardButton(text="🔙 عودة",       callback_data="fadm_back"))

    txt = (
        f"📢 <b>الاشتراك الإجباري العام</b>\n"
        f"عدد القنوات: <b>{len(chans or [])}</b>\n\n"
        f"⚡ هذه القنوات تنطبق على <b>جميع</b> البوتات المصنوعة"
    )
    try:
        await cb.message.edit_text(txt, reply_markup=b.as_markup())
    except:
        await cb.message.answer(txt, reply_markup=b.as_markup())

@factory_dp.callback_query(F.data == "fadd_chan")
async def cb_fadd_chan(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "📢 أرسل رابط القناة أو يوزرنيمها:\n"
        "• <code>https://t.me/channel</code>\n"
        "• <code>@channel</code>\n\n"
        "⚠️ تأكد أن صانع البوتات مشرف في القناة!"
    )
    await state.set_state(FactorySt.add_factory_channel)

@factory_dp.message(FactorySt.add_factory_channel)
async def process_fadd_chan(msg: Message, state: FSMContext):
    await state.clear()
    raw = msg.text.strip()
    if "t.me/" in raw:
        username = raw.rstrip("/").split("t.me/")[-1].split("/")[0]
    elif raw.startswith("@"):
        username = raw[1:]
    else:
        username = raw

    if not username:
        return await msg.answer("❌ يوزرنيم غير صحيح.")

    wm = await msg.answer(f"⏳ جاري التحقق من @{username}...")
    chat_id_val, chat_title = await get_chat_id_direct(username)

    if not chat_id_val:
        try:
            await wm.delete()
        except:
            pass
        return await msg.answer(f"❌ فشل جلب بيانات القناة:\n<code>{chat_title}</code>")

    try:
        me     = await factory_bot.get_me()
        member = await factory_bot.get_chat_member(chat_id=chat_id_val, user_id=me.id)
        if member.status not in ("administrator", "creator"):
            try:
                await wm.delete()
            except:
                pass
            return await msg.answer(f"❌ البوت ليس مشرفاً في @{username}.")
    except Exception as e:
        try:
            await wm.delete()
        except:
            pass
        return await msg.answer(f"❌ خطأ في التحقق: <code>{str(e)[:200]}</code>")

    await db_write(
        "INSERT OR REPLACE INTO factory_channels (id,url,username) VALUES (?,?,?)",
        (str(chat_id_val), f"https://t.me/{username}", f"@{username}")
    )
    cache_del("factory_channels")

    try:
        await wm.delete()
    except:
        pass
    await msg.answer(f"✅ تمت إضافة القناة!\n📢 {chat_title}\n🆔 <code>{chat_id_val}</code>")

@factory_dp.callback_query(F.data.startswith("fdel_chan_"))
async def cb_fdel_chan(cb: CallbackQuery):
    c_id = cb.data[len("fdel_chan_"):]
    await db_write("DELETE FROM factory_channels WHERE id=?", (c_id,))
    cache_del("factory_channels")
    await cb.answer("✅ تم حذف القناة")
    await cb_fadm_force(cb)

# ─── التنبيهات لصانع البوتات ──────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_notify")
async def cb_fadm_notify(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)

    sub = await db_read("SELECT value FROM factory_settings WHERE key='sub_notify'",   one=True)
    ent = await db_read("SELECT value FROM factory_settings WHERE key='entry_notify'", one=True)
    sv  = sub["value"] if sub else "OFF"
    ev  = ent["value"] if ent else "OFF"

    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text=f"{'🟢' if sv=='ON' else '🔴'} تنبيه الاشتراك: {sv}",
        callback_data="ftoggle_sub"
    ))
    b.row(InlineKeyboardButton(
        text=f"{'🟢' if ev=='ON' else '🔴'} تنبيه الدخول: {ev}",
        callback_data="ftoggle_entry"
    ))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="fadm_back"))

    try:
        await cb.message.edit_text("🔔 <b>إعدادات التنبيهات:</b>", reply_markup=b.as_markup())
    except:
        await cb.message.answer("🔔 <b>إعدادات التنبيهات:</b>", reply_markup=b.as_markup())

@factory_dp.callback_query(F.data.startswith("ftoggle_"))
async def cb_ftoggle(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    key     = "sub_notify" if "sub" in cb.data else "entry_notify"
    res     = await db_read("SELECT value FROM factory_settings WHERE key=?", (key,), one=True)
    new_val = "OFF" if res and res["value"] == "ON" else "ON"
    await db_write("UPDATE factory_settings SET value=? WHERE key=?", (new_val, key))
    await cb.answer(f"✅ {new_val}")
    await cb_fadm_notify(cb)

# ─── تعديل رسالة الترحيب ──────────────────────────────────────────────────────
@factory_dp.callback_query(F.data == "fadm_help")
async def cb_fadm_help(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("❌", show_alert=True)
    res = await db_read("SELECT value FROM factory_settings WHERE key='help_text'", one=True)
    await cb.message.answer(
        f"📝 النص الحالي:\n<i>{res['value'] if res else '—'}</i>\n\nأرسل النص الجديد:"
    )
    await state.set_state(FactorySt.edit_factory_help)

@factory_dp.message(FactorySt.edit_factory_help)
async def process_edit_factory_help(msg: Message, state: FSMContext):
    await state.clear()
    if not msg.text.strip():
        return await msg.answer("❌ النص فارغ.")
    await db_write(
        "UPDATE factory_settings SET value=? WHERE key=?",
        (msg.text.strip(), "help_text")
    )
    await msg.answer("✅ تم تحديث نص الترحيب.")

# ─── معالج الأخطاء ────────────────────────────────────────────────────────────
@factory_dp.error()
async def factory_on_error(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, (TelegramForbiddenError, TelegramConflictError, TelegramBadRequest)):
        return
    if isinstance(exc, TelegramRetryAfter):
        await asyncio.sleep(exc.retry_after)
        return
    logger.error(f"Factory DP Error: {exc}")

# ═══════════════════════════════════════════════════════════════════════════════
# ─── بوت الملازم الفرعي ──────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

class StudySt(StatesGroup):
    add_sec_name  = State()
    add_con_name  = State()
    add_con_data  = State()
    add_chan      = State()
    broadcast     = State()
    edit_help     = State()
    add_sub_admin = State()
    search_query  = State()
    send_request  = State()
    reply_request = State()

async def study_get_owner(token: str) -> int:
    rec = await db_read("SELECT owner_id FROM bots WHERE token=?", (token,), one=True)
    return rec["owner_id"] if rec else 0

async def study_get_admins(token: str) -> list:
    rows = await db_read("SELECT user_id FROM sub_admins WHERE bot_token=?", (token,))
    return [r["user_id"] for r in (rows or [])]

async def study_is_admin(token: str, uid: int) -> bool:
    owner = await study_get_owner(token)
    if uid == owner:
        return True
    admins = await study_get_admins(token)
    return uid in admins

async def study_check_sub(bot_inst: Bot, token: str, uid: int) -> bool:
    owner = await study_get_owner(token)
    if uid == owner or await study_is_admin(token, uid):
        return True

    # قنوات الاشتراك الإجباري للبوت الفرعي
    sub_chans = await db_read("SELECT id FROM sub_channels WHERE bot_token=?", (token,))
    # قنوات صانع البوتات (إجبارية على الكل)
    factory_chans = await db_read("SELECT id FROM factory_channels")

    all_chans = list(sub_chans or []) + list(factory_chans or [])
    if not all_chans:
        return True

    for c in all_chans:
        try:
            m = await bot_inst.get_chat_member(chat_id=c["id"], user_id=uid)
            if m.status in ("left", "kicked"):
                return False
        except:
            pass
    return True

async def study_get_sub_kb(bot_inst: Bot, token: str) -> InlineKeyboardMarkup:
    sub_chans     = await db_read("SELECT url FROM sub_channels WHERE bot_token=?", (token,))
    factory_chans = await db_read("SELECT url FROM factory_channels")
    all_chans     = list(sub_chans or []) + list(factory_chans or [])
    b = InlineKeyboardBuilder()
    for c in all_chans:
        b.row(InlineKeyboardButton(text="📢 انضم للقناة", url=c["url"]))
    b.row(InlineKeyboardButton(text="✅ تحققت من الاشتراك", callback_data="study_check_sub"))
    return b.as_markup()

async def study_get_main_kb(token: str, uid: int) -> ReplyKeyboardMarkup:
    secs = await db_read(
        "SELECT name FROM sub_sections WHERE bot_token=? AND parent_id IS NULL",
        (token,)
    )
    icons = {"الملازم":"🗂","التحفيز":"💡","ارشادات":"📝","الملخصات":"📚"}
    btns  = []
    for s in (secs or []):
        icon = next((v for k, v in icons.items() if k in s["name"]), "📁")
        btns.append(KeyboardButton(text=f"{icon} {s['name']}"))

    btns += [
        KeyboardButton(text="🔍 بحث"),
        KeyboardButton(text="ℹ️ شرح البوت"),
        KeyboardButton(text="📩 طلب ملزمة"),
    ]
    b = ReplyKeyboardBuilder()
    for i in range(0, len(btns), 2):
        b.row(*btns[i: i + 2])
    if await study_is_admin(token, uid):
        b.row(KeyboardButton(text="🛠 لوحة التحكم"))
    return b.as_markup(resize_keyboard=True)

def study_admin_kb(token: str, is_owner: bool) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📂 إدارة الأقسام", callback_data="s_adm_secs"))
    b.row(
        InlineKeyboardButton(text="📊 الإحصائيات", callback_data="s_adm_stats"),
        InlineKeyboardButton(text="📣 إذاعة",       callback_data="s_adm_bc"),
    )
    b.row(InlineKeyboardButton(text="📩 الطلبات",   callback_data="s_adm_reqs"))
    if is_owner:
        b.row(
            InlineKeyboardButton(text="👥 المشرفين",          callback_data="s_adm_sub_admins"),
            InlineKeyboardButton(text="📢 اشتراك إجباري",     callback_data="s_adm_force"),
        )
        b.row(
            InlineKeyboardButton(text="🔔 التنبيهات",   callback_data="s_adm_notify"),
            InlineKeyboardButton(text="📝 تعديل الشرح", callback_data="s_adm_help"),
        )
    return b.as_markup()

def make_study_router(token: str, bot_inst: Bot) -> Router:
    """إنشاء روتر لبوت الملازم"""
    r = Router()

    ICON_RE = re.compile(r"^(🗂|💡|📝|📚|📁)\s+")

    # ── /start ─────────────────────────────────────────────────────────────────
    @r.message(Command("start"))
    async def s_start(msg: Message, state: FSMContext):
        await state.clear()
        u = msg.from_user
        existing = await db_read(
            "SELECT id FROM sub_users WHERE bot_token=? AND user_id=?",
            (token, u.id), one=True
        )
        if not existing:
            await db_write(
                "INSERT OR IGNORE INTO sub_users (bot_token,user_id,username,joined_at) VALUES (?,?,?,?)",
                (token, u.id, u.username, datetime.now().isoformat())
            )
            notify = await db_read(
                "SELECT value FROM sub_settings WHERE bot_token=? AND key='entry_notify'",
                (token,), one=True
            )
            if notify and notify["value"] == "ON":
                owner = await study_get_owner(token)
                try:
                    await bot_inst.send_message(
                        owner,
                        f"👤 <b>مستخدم جديد:</b>\n{u.full_name}\n@{u.username or '—'}\n<code>{u.id}</code>"
                    )
                except:
                    pass

        if not await study_check_sub(bot_inst, token, u.id):
            return await msg.answer(
                "⚠️ يجب الاشتراك أولاً:",
                reply_markup=await study_get_sub_kb(bot_inst, token)
            )

        ht = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        await msg.answer(
            ht["value"] if ht else "مرحباً 🎓",
            reply_markup=await study_get_main_kb(token, u.id)
        )

    @r.message(Command("cancel"))
    async def s_cancel(msg: Message, state: FSMContext):
        await state.clear()
        await msg.answer("❌ تم الإلغاء.", reply_markup=await study_get_main_kb(token, msg.from_user.id))

    @r.message(Command("help"))
    async def s_help(msg: Message):
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        await msg.answer(res["value"] if res else "لا يوجد شرح.")

    # ── التحقق من الاشتراك ──────────────────────────────────────────────────────
    @r.callback_query(F.data == "study_check_sub")
    async def s_check_sub(cb: CallbackQuery):
        if await study_check_sub(bot_inst, token, cb.from_user.id):
            notify = await db_read(
                "SELECT value FROM sub_settings WHERE bot_token=? AND key='sub_notify'",
                (token,), one=True
            )
            if notify and notify["value"] == "ON":
                owner = await study_get_owner(token)
                try:
                    await bot_inst.send_message(
                        owner,
                        f"✅ انضم: {cb.from_user.full_name} | <code>{cb.from_user.id}</code>"
                    )
                except:
                    pass
            try:
                await cb.message.delete()
            except:
                pass
            await cb.message.answer(
                "✅ أهلاً بك 🎓",
                reply_markup=await study_get_main_kb(token, cb.from_user.id)
            )
        else:
            await cb.answer("❌ لم تشترك بعد في جميع القنوات.", show_alert=True)

    # ── لوحة التحكم ─────────────────────────────────────────────────────────────
    @r.message(F.text == "🛠 لوحة التحكم")
    async def s_admin_panel(msg: Message, state: FSMContext):
        uid = msg.from_user.id
        if not await study_is_admin(token, uid):
            return
        await state.clear()
        owner = await study_get_owner(token)
        await msg.answer(
            "🛠 <b>لوحة التحكم:</b>",
            reply_markup=study_admin_kb(token, uid == owner)
        )

    @r.callback_query(F.data == "s_adm_back")
    async def s_back(cb: CallbackQuery):
        owner = await study_get_owner(token)
        try:
            await cb.message.edit_text(
                "🛠 <b>لوحة التحكم:</b>",
                reply_markup=study_admin_kb(token, cb.from_user.id == owner)
            )
        except:
            await cb.message.answer(
                "🛠 <b>لوحة التحكم:</b>",
                reply_markup=study_admin_kb(token, cb.from_user.id == owner)
            )

    # ── الإحصائيات ───────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_stats")
    async def s_stats(cb: CallbackQuery):
        if not await study_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)

        total  = await db_read("SELECT COUNT(*) c FROM sub_users WHERE bot_token=?", (token,), one=True)
        secs   = await db_read("SELECT COUNT(*) c FROM sub_sections WHERE bot_token=?", (token,), one=True)
        cons   = await db_read("SELECT COUNT(*) c FROM sub_content WHERE bot_token=?", (token,), one=True)
        pinned = await db_read("SELECT COUNT(*) c FROM sub_content WHERE bot_token=? AND pinned=1", (token,), one=True)
        reqs   = await db_read("SELECT COUNT(*) c FROM sub_requests WHERE bot_token=?", (token,), one=True)

        text = (
            f"📊 <b>إحصائيات البوت:</b>\n\n"
            f"👥 المستخدمون: <b>{total['c'] if total else 0}</b>\n"
            f"📂 الأقسام: <b>{secs['c'] if secs else 0}</b>\n"
            f"📄 المحتويات: <b>{cons['c'] if cons else 0}</b>\n"
            f"📌 المثبتة: <b>{pinned['c'] if pinned else 0}</b>\n"
            f"📩 الطلبات: <b>{reqs['c'] if reqs else 0}</b>"
        )
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="s_adm_back"))
        try:
            await cb.message.edit_text(text, reply_markup=b.as_markup())
        except:
            await cb.message.answer(text, reply_markup=b.as_markup())

    # ── الطلبات ──────────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_reqs")
    async def s_adm_reqs(cb: CallbackQuery):
        if not await study_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        rows = await db_read(
            "SELECT * FROM sub_requests WHERE bot_token=? ORDER BY id DESC LIMIT 20",
            (token,)
        )
        b = InlineKeyboardBuilder()
        if not rows:
            b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="s_adm_back"))
            try:
                await cb.message.edit_text("📩 لا توجد طلبات.", reply_markup=b.as_markup())
            except:
                await cb.message.answer("📩 لا توجد طلبات.", reply_markup=b.as_markup())
            return
        for req in rows:
            status = "✅" if req["answered"] else "🔵"
            b.row(InlineKeyboardButton(
                text=f"{status} #{req['id']} — {req['full_name']}",
                callback_data=f"s_view_req_{req['id']}"
            ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="s_adm_back"))
        try:
            await cb.message.edit_text(
                f"📩 <b>الطلبات ({len(rows)}):</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"📩 <b>الطلبات ({len(rows)}):</b>",
                reply_markup=b.as_markup()
            )

    @r.callback_query(F.data.startswith("s_view_req_"))
    async def s_view_req(cb: CallbackQuery):
        req_id = int(cb.data.split("s_view_req_")[1])
        req = await db_read("SELECT * FROM sub_requests WHERE id=? AND bot_token=?", (req_id, token), one=True)
        if not req:
            return await cb.answer("❌ غير موجود", show_alert=True)
        text = (
            f"📩 <b>طلب #{req['id']}</b>\n👤 {req['full_name']}\n"
            f"🆔 <code>{req['user_id']}</code>\n"
            f"🕐 {req['created_at']}\n"
            f"{'✅ تمت الإجابة' if req['answered'] else '🔵 لم يُجب بعد'}\n\n"
            f"💬 {req['message']}"
        )
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text="↩️ رد",
            callback_data=f"s_reply_req_{req['id']}_{req['user_id']}"
        ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="s_adm_reqs"))
        try:
            await cb.message.edit_text(text, reply_markup=b.as_markup())
        except:
            await cb.message.answer(text, reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("s_reply_req_"))
    async def s_reply_req_cb(cb: CallbackQuery, state: FSMContext):
        parts  = cb.data.split("_")
        req_id = parts[3]
        uid    = parts[4]
        await state.update_data(reply_req_id=req_id, reply_user_id=uid)
        await cb.message.answer(f"↩️ أرسل ردك على الطلب #{req_id}:")
        await state.set_state(StudySt.reply_request)

    @r.message(StudySt.reply_request)
    async def s_process_reply(msg: Message, state: FSMContext):
        data    = await state.get_data()
        req_id  = data["reply_req_id"]
        user_id = int(data["reply_user_id"])
        await state.clear()
        try:
            await bot_inst.send_message(
                user_id,
                f"📩 <b>رد على طلبك #{req_id}:</b>\n\n{msg.text}"
            )
            await db_write(
                "UPDATE sub_requests SET answered=1 WHERE id=?",
                (int(req_id),)
            )
            await msg.answer("✅ تم إرسال الرد.")
        except Exception as e:
            await msg.answer(f"❌ فشل الإرسال: {e}")

    # ── المشرفون الفرعيون ────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_sub_admins")
    async def s_sub_admins(cb: CallbackQuery):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        admins = await study_get_admins(token)
        b = InlineKeyboardBuilder()
        for sa_id in admins:
            try:
                name = (await bot_inst.get_chat(sa_id)).full_name
            except:
                name = f"ID:{sa_id}"
            b.row(InlineKeyboardButton(
                text=f"❌ {name}",
                callback_data=f"s_del_sub_{sa_id}"
            ))
        if not admins:
            b.row(InlineKeyboardButton(text="لا يوجد مشرفون", callback_data="s_noop"))
        b.row(InlineKeyboardButton(text="➕ إضافة مشرف", callback_data="s_add_sub"))
        b.row(InlineKeyboardButton(text="🔙 عودة",        callback_data="s_adm_back"))
        txt = f"👥 <b>المشرفون الفرعيون: {len(admins)}</b>"
        try:
            await cb.message.edit_text(txt, reply_markup=b.as_markup())
        except:
            await cb.message.answer(txt, reply_markup=b.as_markup())

    @r.callback_query(F.data == "s_add_sub")
    async def s_add_sub(cb: CallbackQuery, state: FSMContext):
        await cb.message.answer("➕ أرسل ID المشرف الجديد:")
        await state.set_state(StudySt.add_sub_admin)

    @r.message(StudySt.add_sub_admin)
    async def s_process_add_sub(msg: Message, state: FSMContext):
        owner = await study_get_owner(token)
        if msg.from_user.id != owner:
            return
        await state.clear()
        try:
            uid = int(msg.text.strip())
            admins = await study_get_admins(token)
            if uid in admins:
                return await msg.answer("❌ مشرف بالفعل.")
            await db_write(
                "INSERT OR IGNORE INTO sub_admins (bot_token,user_id) VALUES (?,?)",
                (token, uid)
            )
            await msg.answer(f"✅ تم إضافة <code>{uid}</code> كمشرف.")
        except ValueError:
            await msg.answer("❌ أرسل رقم صحيح.")

    @r.callback_query(F.data.startswith("s_del_sub_"))
    async def s_del_sub(cb: CallbackQuery):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        sa_id = int(cb.data.split("s_del_sub_")[1])
        await db_write(
            "DELETE FROM sub_admins WHERE bot_token=? AND user_id=?",
            (token, sa_id)
        )
        await cb.answer("✅ تم الحذف")
        await s_sub_admins(cb)

    # ── إدارة الأقسام ────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_secs")
    async def s_manage_secs(cb: CallbackQuery):
        if not await study_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        secs = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND parent_id IS NULL",
            (token,)
        )
        b = InlineKeyboardBuilder()
        for s in (secs or []):
            b.row(InlineKeyboardButton(
                text=f"📁 {s['name']}",
                callback_data=f"s_sec_{s['id']}"
            ))
        b.row(InlineKeyboardButton(text="➕ إضافة قسم",  callback_data="s_add_sec_main"))
        b.row(InlineKeyboardButton(text="🔙 عودة",        callback_data="s_adm_back"))
        try:
            await cb.message.edit_text("📂 <b>الأقسام الرئيسية:</b>", reply_markup=b.as_markup())
        except:
            await cb.message.answer("📂 <b>الأقسام الرئيسية:</b>", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("s_sec_"))
    async def s_view_sec(cb: CallbackQuery):
        sec_id = int(cb.data.split("s_sec_")[1])
        sec    = await db_read("SELECT * FROM sub_sections WHERE id=? AND bot_token=?", (sec_id, token), one=True)
        if not sec:
            return await cb.answer("❌ غير موجود", show_alert=True)
        subs = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND parent_id=?",
            (token, sec_id)
        )
        cons = await db_read(
            "SELECT * FROM sub_content WHERE bot_token=? AND section_id=? ORDER BY pinned DESC,id DESC",
            (token, sec_id)
        )
        b = InlineKeyboardBuilder()
        for s in (subs or []):
            b.row(InlineKeyboardButton(text=f"📂 {s['name']}", callback_data=f"s_sec_{s['id']}"))
        for c in (cons or []):
            icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵"}.get(c["type"],"📝")
            pin  = "📌" if c["pinned"] else ""
            b.row(InlineKeyboardButton(
                text=f"{pin}{icon} {c['name']}",
                callback_data=f"s_con_{c['id']}"
            ))
        b.row(
            InlineKeyboardButton(text="➕ قسم فرعي", callback_data=f"s_add_sec_sub_{sec_id}"),
            InlineKeyboardButton(text="➕ محتوى",    callback_data=f"s_add_con_{sec_id}"),
        )
        b.row(InlineKeyboardButton(text="🗑 حذف القسم", callback_data=f"s_del_sec_{sec_id}"))
        back = f"s_sec_{sec['parent_id']}" if sec["parent_id"] else "s_adm_secs"
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data=back))
        try:
            await cb.message.edit_text(
                f"📂 <b>{sec['name']}</b>\n📂 فرعية: {len(subs or [])} | 📄 محتوى: {len(cons or [])}",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(f"📂 <b>{sec['name']}</b>", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("s_con_"))
    async def s_adm_con(cb: CallbackQuery):
        c_id = int(cb.data.split("s_con_")[1])
        item = await db_read(
            "SELECT * FROM sub_content WHERE id=? AND bot_token=?",
            (c_id, token), one=True
        )
        if not item:
            return await cb.answer("❌ غير موجود", show_alert=True)
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text="📌 إلغاء التثبيت" if item["pinned"] else "📌 تثبيت",
            callback_data=f"s_pin_{c_id}"
        ))
        b.row(InlineKeyboardButton(text="🗑 حذف", callback_data=f"s_del_con_{c_id}"))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data=f"s_sec_{item['section_id']}"))
        try:
            await cb.message.edit_text(
                f"📎 <b>{item['name']}</b>\nالنوع: {item['type']} | مثبت: {'نعم' if item['pinned'] else 'لا'}",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(f"📎 {item['name']}", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("s_pin_"))
    async def s_toggle_pin(cb: CallbackQuery):
        c_id = int(cb.data.split("s_pin_")[1])
        item = await db_read("SELECT pinned FROM sub_content WHERE id=? AND bot_token=?", (c_id, token), one=True)
        if not item:
            return await cb.answer("❌ غير موجود", show_alert=True)
        new_pin = 0 if item["pinned"] else 1
        await db_write("UPDATE sub_content SET pinned=? WHERE id=?", (new_pin, c_id))
        await cb.answer("📌 تم التثبيت" if new_pin else "✅ إلغاء التثبيت")
        cb.data = f"s_con_{c_id}"
        await s_adm_con(cb)

    @r.callback_query(F.data.startswith("s_add_sec_"))
    async def s_add_sec(cb: CallbackQuery, state: FSMContext):
        parts     = cb.data.split("_")
        parent_id = parts[4] if len(parts) > 4 and parts[3] == "sub" else None
        await cb.message.answer("📝 أرسل اسم القسم:")
        await state.set_state(StudySt.add_sec_name)
        await state.update_data(parent_id=parent_id)

    @r.message(StudySt.add_sec_name)
    async def s_process_sec_name(msg: Message, state: FSMContext):
        data = await state.get_data()
        name = msg.text.strip()
        if not name:
            return await msg.answer("❌ الاسم فارغ.")
        try:
            parent_id = int(data["parent_id"]) if data.get("parent_id") else None
            await db_write(
                "INSERT INTO sub_sections (bot_token,name,parent_id) VALUES (?,?,?)",
                (token, name, parent_id)
            )
            await msg.answer(f"✅ تم إضافة '<b>{name}</b>'")
        except Exception as e:
            await msg.answer(f"❌ خطأ: {e}")
        await state.clear()

    @r.callback_query(F.data.startswith("s_del_sec_"))
    async def s_del_sec(cb: CallbackQuery):
        sec_id = int(cb.data.split("s_del_sec_")[1])
        sec    = await db_read(
            "SELECT * FROM sub_sections WHERE id=? AND bot_token=?",
            (sec_id, token), one=True
        )
        if not sec:
            return await cb.answer("❌ غير موجود", show_alert=True)

        async def delete_recursive(sid):
            await db_write(
                "DELETE FROM sub_content WHERE bot_token=? AND section_id=?",
                (token, sid)
            )
            subs = await db_read(
                "SELECT id FROM sub_sections WHERE bot_token=? AND parent_id=?",
                (token, sid)
            )
            for sub in (subs or []):
                await delete_recursive(sub["id"])
            await db_write(
                "DELETE FROM sub_sections WHERE id=? AND bot_token=?",
                (sid, token)
            )

        await delete_recursive(sec_id)
        await cb.answer("✅ تم الحذف")

        if sec["parent_id"]:
            cb.data = f"s_sec_{sec['parent_id']}"
            await s_view_sec(cb)
        else:
            await s_manage_secs(cb)

    @r.callback_query(F.data.startswith("s_add_con_"))
    async def s_add_con(cb: CallbackQuery, state: FSMContext):
        sec_id = cb.data.split("s_add_con_")[1]
        await cb.message.answer("📝 أرسل اسم/عنوان المحتوى:")
        await state.set_state(StudySt.add_con_name)
        await state.update_data(sec_id=sec_id)

    @r.message(StudySt.add_con_name)
    async def s_process_con_name(msg: Message, state: FSMContext):
        await state.update_data(con_name=msg.text)
        await msg.answer("📎 أرسل المحتوى (نص، صورة، ملف، صوت، فيديو):")
        await state.set_state(StudySt.add_con_data)

    @r.message(StudySt.add_con_data)
    async def s_process_con_data(msg: Message, state: FSMContext):
        data = await state.get_data()
        c_type, c_data, f_id = "text", msg.text or "", None
        if msg.photo:
            c_type, c_data, f_id = "photo", msg.caption or "", msg.photo[-1].file_id
        elif msg.document:
            c_type, c_data, f_id = "doc",   msg.caption or "", msg.document.file_id
        elif msg.voice:
            c_type, c_data, f_id = "voice", msg.caption or "", msg.voice.file_id
        elif msg.video:
            c_type, c_data, f_id = "video", msg.caption or "", msg.video.file_id
        elif msg.audio:
            c_type, c_data, f_id = "audio", msg.caption or "", msg.audio.file_id
        elif msg.video_note:
            c_type, c_data, f_id = "video_note", "", msg.video_note.file_id
        try:
            await db_write(
                "INSERT INTO sub_content (bot_token,section_id,name,type,data,file_id,created_at) VALUES (?,?,?,?,?,?,?)",
                (token, int(data["sec_id"]), data["con_name"], c_type, c_data, f_id,
                 datetime.now().strftime("%Y-%m-%d %H:%M"))
            )
            await msg.answer(f"✅ تم الحفظ!\n📌 {data['con_name']}\n📎 {c_type}")
        except Exception as e:
            await msg.answer(f"❌ خطأ: {e}")
        await state.clear()

    @r.callback_query(F.data.startswith("s_del_con_"))
    async def s_del_con(cb: CallbackQuery):
        c_id = int(cb.data.split("s_del_con_")[1])
        item = await db_read(
            "SELECT name,section_id FROM sub_content WHERE id=? AND bot_token=?",
            (c_id, token), one=True
        )
        if not item:
            return await cb.answer("❌ غير موجود", show_alert=True)
        b = InlineKeyboardBuilder()
        b.row(
            InlineKeyboardButton(text="✅ نعم، احذف", callback_data=f"s_confirm_del_{c_id}"),
            InlineKeyboardButton(text="❌ إلغاء",     callback_data=f"s_sec_{item['section_id']}"),
        )
        try:
            await cb.message.edit_text(
                f"⚠️ حذف <b>{item['name']}</b>؟",
                reply_markup=b.as_markup()
            )
        except:
            pass

    @r.callback_query(F.data.startswith("s_confirm_del_"))
    async def s_confirm_del(cb: CallbackQuery):
        c_id = int(cb.data.split("s_confirm_del_")[1])
        item = await db_read(
            "SELECT * FROM sub_content WHERE id=? AND bot_token=?",
            (c_id, token), one=True
        )
        if not item:
            return await cb.answer("❌ محذوف مسبقاً", show_alert=True)
        await db_write(
            "DELETE FROM sub_content WHERE id=? AND bot_token=?",
            (c_id, token)
        )
        await cb.answer("✅ تم الحذف")
        cb.data = f"s_sec_{item['section_id']}"
        await s_view_sec(cb)

    # ── الإذاعة ──────────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_bc")
    async def s_bc(cb: CallbackQuery, state: FSMContext):
        if not await study_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        await cb.message.answer("📣 أرسل رسالة الإذاعة:")
        await state.set_state(StudySt.broadcast)

    @r.message(StudySt.broadcast)
    async def s_process_bc(msg: Message, state: FSMContext):
        await state.clear()
        users = await db_read(
            "SELECT user_id FROM sub_users WHERE bot_token=? AND is_active=1",
            (token,)
        )
        if not users:
            return await msg.answer("⚠️ لا يوجد مستخدمون.")
        total   = len(users)
        sm      = await msg.answer(f"📣 جاري الإذاعة لـ <b>{total}</b>...")
        success = 0
        failed  = 0
        blocked = []

        async def send_one(uid):
            nonlocal success, failed
            try:
                await msg.copy_to(uid)
                success += 1
            except TelegramForbiddenError:
                blocked.append(uid)
                failed += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    await msg.copy_to(uid)
                    success += 1
                except:
                    failed += 1
            except:
                failed += 1

        for i in range(0, total, 30):
            batch = [u["user_id"] for u in users[i: i + 30]]
            await asyncio.gather(*[send_one(uid) for uid in batch])
            await asyncio.sleep(0.05)

        for uid in blocked:
            await db_write(
                "DELETE FROM sub_users WHERE bot_token=? AND user_id=?",
                (token, uid)
            )

        try:
            await sm.edit_text(
                f"✅ الإذاعة:\n📤 نجح: <b>{success}</b>\n❌ فشل: <b>{failed}</b>\n🚫 محظور: <b>{len(blocked)}</b>"
            )
        except:
            pass

    # ── الاشتراك الإجباري ─────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_force")
    async def s_force(cb: CallbackQuery):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        chans = await db_read(
            "SELECT * FROM sub_channels WHERE bot_token=?", (token,)
        )
        b = InlineKeyboardBuilder()
        for c in (chans or []):
            b.row(InlineKeyboardButton(
                text=f"❌ {c['username'] or c['id']}",
                callback_data=f"s_del_chan_{c['id']}"
            ))
        b.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="s_add_chan"))
        b.row(InlineKeyboardButton(text="🔙 عودة",       callback_data="s_adm_back"))
        try:
            await cb.message.edit_text(
                f"📢 القنوات: <b>{len(chans or [])}</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"📢 القنوات: <b>{len(chans or [])}</b>",
                reply_markup=b.as_markup()
            )

    @r.callback_query(F.data == "s_add_chan")
    async def s_add_chan(cb: CallbackQuery, state: FSMContext):
        await cb.message.answer(
            "📢 أرسل رابط القناة أو يوزرنيمها:\n"
            "مثال: <code>@channel</code> أو <code>https://t.me/channel</code>\n\n"
            "⚠️ يجب أن يكون البوت مشرفاً في القناة!"
        )
        await state.set_state(StudySt.add_chan)

    @r.message(StudySt.add_chan)
    async def s_process_add_chan(msg: Message, state: FSMContext):
        await state.clear()
        raw = msg.text.strip()
        if "t.me/" in raw:
            username = raw.rstrip("/").split("t.me/")[-1].split("/")[0]
        elif raw.startswith("@"):
            username = raw[1:]
        else:
            username = raw

        if not username:
            return await msg.answer("❌ يوزرنيم غير صحيح.")

        wm = await msg.answer(f"⏳ جاري التحقق من @{username}...")
        chat_id_val, chat_title = await get_chat_id_direct(username, token)

        if not chat_id_val:
            try:
                await wm.delete()
            except:
                pass
            return await msg.answer(
                f"❌ فشل جلب بيانات القناة:\n<code>{chat_title}</code>"
            )

        try:
            me     = await bot_inst.get_me()
            member = await bot_inst.get_chat_member(chat_id=chat_id_val, user_id=me.id)
            if member.status not in ("administrator", "creator"):
                try:
                    await wm.delete()
                except:
                    pass
                return await msg.answer(f"❌ البوت ليس مشرفاً في @{username}.")

            await db_write(
                "INSERT OR REPLACE INTO sub_channels (bot_token,id,url,username) VALUES (?,?,?,?)",
                (token, str(chat_id_val), f"https://t.me/{username}", f"@{username}")
            )
            try:
                await wm.delete()
            except:
                pass
            await msg.answer(
                f"✅ تمت الإضافة!\n📢 {chat_title}\n🆔 <code>{chat_id_val}</code>"
            )
        except Exception as e:
            try:
                await wm.delete()
            except:
                pass
            await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")

    @r.callback_query(F.data.startswith("s_del_chan_"))
    async def s_del_chan(cb: CallbackQuery):
        c_id = cb.data[len("s_del_chan_"):]
        await db_write(
            "DELETE FROM sub_channels WHERE bot_token=? AND id=?",
            (token, c_id)
        )
        await cb.answer("✅ تم الحذف")
        await s_force(cb)

    # ── التنبيهات ─────────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_notify")
    async def s_notify(cb: CallbackQuery):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        sub = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='sub_notify'",
            (token,), one=True
        )
        ent = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='entry_notify'",
            (token,), one=True
        )
        sv = sub["value"] if sub else "OFF"
        ev = ent["value"] if ent else "OFF"
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text=f"{'🟢' if sv=='ON' else '🔴'} اشتراك: {sv}",
            callback_data="s_toggle_sub"
        ))
        b.row(InlineKeyboardButton(
            text=f"{'🟢' if ev=='ON' else '🔴'} دخول: {ev}",
            callback_data="s_toggle_entry"
        ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="s_adm_back"))
        try:
            await cb.message.edit_text("🔔 التنبيهات:", reply_markup=b.as_markup())
        except:
            await cb.message.answer("🔔 التنبيهات:", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("s_toggle_"))
    async def s_toggle(cb: CallbackQuery):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        key = "sub_notify" if "sub" in cb.data else "entry_notify"
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key=?",
            (token, key), one=True
        )
        new_val = "OFF" if res and res["value"] == "ON" else "ON"
        await db_write(
            "INSERT OR REPLACE INTO sub_settings (bot_token,key,value) VALUES (?,?,?)",
            (token, key, new_val)
        )
        await cb.answer(f"✅ {new_val}")
        await s_notify(cb)

    # ── تعديل الشرح ──────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "s_adm_help")
    async def s_adm_help(cb: CallbackQuery, state: FSMContext):
        owner = await study_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        await cb.message.answer(
            f"📝 النص الحالي:\n<i>{res['value'] if res else '—'}</i>\n\nأرسل النص الجديد:"
        )
        await state.set_state(StudySt.edit_help)

    @r.message(StudySt.edit_help)
    async def s_process_edit_help(msg: Message, state: FSMContext):
        await state.clear()
        new_text = msg.text.strip()
        if not new_text:
            return await msg.answer("❌ النص فارغ.")
        await db_write(
            "INSERT OR REPLACE INTO sub_settings (bot_token,key,value) VALUES (?,?,?)",
            (token, "help_text", new_text)
        )
        await msg.answer("✅ تم التحديث.")

    # ── عرض الأقسام للمستخدمين ────────────────────────────────────────────────────
    @r.message(F.text.regexp(r"^(🗂|💡|📝|📚|📁)\s+"))
    async def s_show_section(msg: Message):
        if not await study_check_sub(bot_inst, token, msg.from_user.id):
            return await msg.answer(
                "⚠️ اشترك أولاً:",
                reply_markup=await study_get_sub_kb(bot_inst, token)
            )
        name = ICON_RE.sub("", msg.text).strip()
        sec  = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND name=?",
            (token, name), one=True
        )
        if not sec:
            return
        await _send_user_section(msg, sec["id"])

    async def _send_user_section(msg: Message, sec_id: int):
        sec  = await db_read(
            "SELECT * FROM sub_sections WHERE id=? AND bot_token=?",
            (sec_id, token), one=True
        )
        if not sec:
            return
        subs = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND parent_id=?",
            (token, sec_id)
        )
        cons = await db_read(
            "SELECT * FROM sub_content WHERE bot_token=? AND section_id=? ORDER BY pinned DESC,id DESC",
            (token, sec_id)
        )
        b = InlineKeyboardBuilder()
        for s in (subs or []):
            display = s["name"].split(" - ")[-1] if " - " in s["name"] else s["name"]
            b.row(InlineKeyboardButton(
                text=f"📂 {display}",
                callback_data=f"su_sec_{s['id']}"
            ))
        for c in (cons or []):
            icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵","video_note":"📹"}.get(c["type"],"📝")
            pin  = "📌" if c["pinned"] else ""
            b.row(InlineKeyboardButton(
                text=f"{pin}{icon} {c['name']}",
                callback_data=f"su_view_{c['id']}"
            ))
        if not subs and not cons:
            b.row(InlineKeyboardButton(text="📭 القسم فارغ", callback_data="s_noop"))
        await msg.answer(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("su_sec_"))
    async def s_user_sec(cb: CallbackQuery):
        sec_id = int(cb.data.split("su_sec_")[1])
        sec    = await db_read(
            "SELECT * FROM sub_sections WHERE id=? AND bot_token=?",
            (sec_id, token), one=True
        )
        if not sec:
            return await cb.answer("❌ غير موجود", show_alert=True)
        subs = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND parent_id=?",
            (token, sec_id)
        )
        cons = await db_read(
            "SELECT * FROM sub_content WHERE bot_token=? AND section_id=? ORDER BY pinned DESC,id DESC",
            (token, sec_id)
        )
        b = InlineKeyboardBuilder()
        for s in (subs or []):
            display = s["name"].split(" - ")[-1] if " - " in s["name"] else s["name"]
            b.row(InlineKeyboardButton(
                text=f"📂 {display}",
                callback_data=f"su_sec_{s['id']}"
            ))
        for c in (cons or []):
            icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵","video_note":"📹"}.get(c["type"],"📝")
            pin  = "📌" if c["pinned"] else ""
            b.row(InlineKeyboardButton(
                text=f"{pin}{icon} {c['name']}",
                callback_data=f"su_view_{c['id']}"
            ))
        if not subs and not cons:
            b.row(InlineKeyboardButton(text="📭 القسم فارغ", callback_data="s_noop"))
        if sec["parent_id"]:
            b.row(InlineKeyboardButton(text="🔙 رجوع", callback_data=f"su_sec_{sec['parent_id']}"))
        try:
            await cb.message.edit_text(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())
        except:
            await cb.message.answer(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("su_view_"))
    async def s_user_view(cb: CallbackQuery):
        c_id = int(cb.data.split("su_view_")[1])
        item = await db_read(
            "SELECT * FROM sub_content WHERE id=? AND bot_token=?",
            (c_id, token), one=True
        )
        if not item:
            return await cb.answer("❌ غير موجود", show_alert=True)
        cap = item["data"] or ""
        try:
            await cb.answer()
            chat_id = cb.message.chat.id
            t = item["type"]
            if t == "text":
                await cb.message.answer(item["data"] or "لا يوجد نص")
            elif t == "photo":
                await bot_inst.send_photo(chat_id, item["file_id"], caption=cap)
            elif t == "doc":
                await bot_inst.send_document(chat_id, item["file_id"], caption=cap)
            elif t == "voice":
                await bot_inst.send_voice(chat_id, item["file_id"], caption=cap)
            elif t == "video":
                await bot_inst.send_video(chat_id, item["file_id"], caption=cap)
            elif t == "audio":
                await bot_inst.send_audio(chat_id, item["file_id"], caption=cap)
            elif t == "video_note":
                await bot_inst.send_video_note(chat_id, item["file_id"])
            else:
                await cb.message.answer("❌ نوع غير معروف")
        except TelegramBadRequest as e:
            logger.error(f"send_content: {e}")
            await cb.message.answer("❌ الملف غير متاح.")
        except Exception as e:
            logger.error(f"send_content: {e}")
            await cb.message.answer("❌ خطأ أثناء الإرسال.")

    # ── البحث ────────────────────────────────────────────────────────────────────
    @r.message(F.text == "🔍 بحث")
    @r.message(Command("search"))
    async def s_search(msg: Message, state: FSMContext):
        await msg.answer("🔍 أرسل كلمة البحث:")
        await state.set_state(StudySt.search_query)

    @r.message(StudySt.search_query)
    async def s_process_search(msg: Message, state: FSMContext):
        query = msg.text.strip()
        if len(query) < 2:
            return await msg.answer("❌ كلمة البحث قصيرة.")
        await state.clear()
        secs = await db_read(
            "SELECT * FROM sub_sections WHERE bot_token=? AND name LIKE ?",
            (token, f"%{query}%")
        )
        cons = await db_read(
            "SELECT * FROM sub_content WHERE bot_token=? AND (name LIKE ? OR data LIKE ?)",
            (token, f"%{query}%", f"%{query}%")
        )
        if not secs and not cons:
            return await msg.answer("❌ لم يتم العثور على نتائج.")
        b = InlineKeyboardBuilder()
        res_text = f"🔍 نتائج البحث عن: <b>{query}</b>\n"
        if secs:
            res_text += f"\n📂 <b>الأقسام ({len(secs)}):</b>"
            for s in secs:
                b.row(InlineKeyboardButton(
                    text=f"📁 {s['name']}",
                    callback_data=f"su_sec_{s['id']}"
                ))
        if cons:
            res_text += f"\n📄 <b>الملفات ({len(cons)}):</b>"
            for c in cons:
                icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵"}.get(c["type"],"📝")
                b.row(InlineKeyboardButton(
                    text=f"{icon} {c['name']}",
                    callback_data=f"su_view_{c['id']}"
                ))
        await msg.answer(res_text, reply_markup=b.as_markup())

    # ── طلب ملزمة ─────────────────────────────────────────────────────────────────
    @r.message(F.text == "📩 طلب ملزمة")
    @r.message(Command("request"))
    async def s_request(msg: Message, state: FSMContext):
        await msg.answer("📩 أرسل اسم الملزمة أو الملف الذي تحتاجه:")
        await state.set_state(StudySt.send_request)

    @r.message(StudySt.send_request)
    async def s_process_request(msg: Message, state: FSMContext):
        if not msg.text:
            return await msg.answer("❌ أرسل نص الطلب.")
        await state.clear()
        u = msg.from_user
        req_id = await db_write(
            "INSERT INTO sub_requests (bot_token,user_id,username,full_name,message) VALUES (?,?,?,?,?)",
            (token, u.id, u.username, u.full_name, msg.text),
            ret_id=True
        )
        await msg.answer(f"✅ تم إرسال طلبك #{req_id}. سيتم الرد قريباً.")
        owner = await study_get_owner(token)
        try:
            await bot_inst.send_message(
                owner,
                f"📩 <b>طلب جديد #{req_id}</b>\n👤 {u.full_name}\n💬 {msg.text}"
            )
        except:
            pass

    # ── شرح البوت ─────────────────────────────────────────────────────────────────
    @r.message(F.text == "ℹ️ شرح البوت")
    @r.message(Command("help"))
    async def s_help_cmd(msg: Message):
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        await msg.answer(res["value"] if res else "لا يوجد شرح.")

    @r.callback_query(F.data == "s_noop")
    async def s_noop(cb: CallbackQuery):
        await cb.answer()

    return r


# ═══════════════════════════════════════════════════════════════════════════════
# ─── بوت التواصل الفرعي ──────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

class ContactSt(StatesGroup):
    send_message   = State()
    reply_message  = State()
    broadcast      = State()
    add_chan        = State()
    edit_help       = State()
    add_sub_admin   = State()

async def contact_get_owner(token: str) -> int:
    rec = await db_read("SELECT owner_id FROM bots WHERE token=?", (token,), one=True)
    return rec["owner_id"] if rec else 0

async def contact_is_admin(token: str, uid: int) -> bool:
    owner = await contact_get_owner(token)
    if uid == owner:
        return True
    rows = await db_read("SELECT user_id FROM sub_admins WHERE bot_token=?", (token,))
    return uid in [r["user_id"] for r in (rows or [])]

async def contact_check_sub(bot_inst: Bot, token: str, uid: int) -> bool:
    owner = await contact_get_owner(token)
    if uid == owner or await contact_is_admin(token, uid):
        return True
    sub_chans     = await db_read("SELECT id FROM sub_channels WHERE bot_token=?", (token,))
    factory_chans = await db_read("SELECT id FROM factory_channels")
    all_chans     = list(sub_chans or []) + list(factory_chans or [])
    if not all_chans:
        return True
    for c in all_chans:
        try:
            m = await bot_inst.get_chat_member(chat_id=c["id"], user_id=uid)
            if m.status in ("left", "kicked"):
                return False
        except:
            pass
    return True

async def contact_get_sub_kb(bot_inst: Bot, token: str) -> InlineKeyboardMarkup:
    sub_chans     = await db_read("SELECT url FROM sub_channels WHERE bot_token=?", (token,))
    factory_chans = await db_read("SELECT url FROM factory_channels")
    all_chans     = list(sub_chans or []) + list(factory_chans or [])
    b = InlineKeyboardBuilder()
    for c in all_chans:
        b.row(InlineKeyboardButton(text="📢 انضم للقناة", url=c["url"]))
    b.row(InlineKeyboardButton(text="✅ تحققت من الاشتراك", callback_data="contact_check_sub"))
    return b.as_markup()

def make_contact_router(token: str, bot_inst: Bot) -> Router:
    """إنشاء روتر لبوت التواصل"""
    r = Router()

    # ── /start ─────────────────────────────────────────────────────────────────
    @r.message(Command("start"))
    async def c_start(msg: Message, state: FSMContext):
        await state.clear()
        u = msg.from_user
        owner = await contact_get_owner(token)

        # تسجيل المستخدم
        existing = await db_read(
            "SELECT id FROM sub_users WHERE bot_token=? AND user_id=?",
            (token, u.id), one=True
        )
        if not existing:
            await db_write(
                "INSERT OR IGNORE INTO sub_users (bot_token,user_id,username,joined_at) VALUES (?,?,?,?)",
                (token, u.id, u.username, datetime.now().isoformat())
            )

        # لوحة المدير
        if u.id == owner or await contact_is_admin(token, u.id):
            msgs_count = await db_read(
                "SELECT COUNT(*) c FROM contact_messages WHERE bot_token=? AND answered=0",
                (token,), one=True
            )
            unanswered = msgs_count["c"] if msgs_count else 0
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(
                text=f"📬 الرسائل {f'({unanswered} جديد)' if unanswered else ''}",
                callback_data="c_adm_msgs"
            ))
            b.row(
                InlineKeyboardButton(text="📊 الإحصائيات", callback_data="c_adm_stats"),
                InlineKeyboardButton(text="📣 إذاعة",       callback_data="c_adm_bc"),
            )
            b.row(
                InlineKeyboardButton(text="📢 اشتراك إجباري", callback_data="c_adm_force"),
                InlineKeyboardButton(text="🔔 التنبيهات",     callback_data="c_adm_notify"),
            )
            if u.id == owner:
                b.row(
                    InlineKeyboardButton(text="👥 المشرفين",   callback_data="c_adm_sub_admins"),
                    InlineKeyboardButton(text="📝 تعديل الشرح",callback_data="c_adm_help"),
                )
            return await msg.answer(
                f"👋 مرحباً بك مدير البوت!\n📬 الرسائل الجديدة: <b>{unanswered}</b>",
                reply_markup=b.as_markup()
            )

        # فحص الاشتراك
        if not await contact_check_sub(bot_inst, token, u.id):
            return await msg.answer(
                "⚠️ يجب الاشتراك أولاً:",
                reply_markup=await contact_get_sub_kb(bot_inst, token)
            )

        ht = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        help_text = ht["value"] if ht else "مرحباً! يمكنك إرسال رسالتك وسيتم الرد عليك."

        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="📩 إرسال رسالة", callback_data="c_send_msg"))
        await msg.answer(help_text, reply_markup=b.as_markup())

    # ── التحقق من الاشتراك ──────────────────────────────────────────────────────
    @r.callback_query(F.data == "contact_check_sub")
    async def c_check_sub(cb: CallbackQuery, state: FSMContext):
        if await contact_check_sub(bot_inst, token, cb.from_user.id):
            try:
                await cb.message.delete()
            except:
                pass
            ht = await db_read(
                "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
                (token,), one=True
            )
            help_text = ht["value"] if ht else "مرحباً! يمكنك إرسال رسالتك."
            b = InlineKeyboardBuilder()
            b.row(InlineKeyboardButton(text="📩 إرسال رسالة", callback_data="c_send_msg"))
            await cb.message.answer(help_text, reply_markup=b.as_markup())
        else:
            await cb.answer("❌ لم تشترك بعد.", show_alert=True)

    # ── إرسال رسالة ──────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_send_msg")
    async def c_send_msg(cb: CallbackQuery, state: FSMContext):
        await cb.message.answer(
            "✍️ أرسل رسالتك الآن:\n(نص، صورة، ملف، صوت - كل شيء مقبول)"
        )
        await state.set_state(ContactSt.send_message)

    @r.message(ContactSt.send_message)
    async def c_process_send_msg(msg: Message, state: FSMContext):
        await state.clear()
        u     = msg.from_user
        owner = await contact_get_owner(token)

        # حفظ الرسالة
        msg_text = msg.text or msg.caption or f"[{msg.content_type}]"
        msg_id = await db_write(
            "INSERT INTO contact_messages (bot_token,user_id,username,full_name,message) VALUES (?,?,?,?,?)",
            (token, u.id, u.username, u.full_name, msg_text),
            ret_id=True
        )

        await msg.answer(
            f"✅ تم إرسال رسالتك (#{msg_id})\n"
            f"⏳ سيتم الرد عليك في أقرب وقت."
        )

        # إرسال للمالك مع زر الرد
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text=f"↩️ رد على {u.full_name}",
            callback_data=f"c_reply_{msg_id}_{u.id}"
        ))
        notif = (
            f"📩 <b>رسالة جديدة #{msg_id}</b>\n"
            f"👤 {u.full_name}\n"
            f"🆔 <code>{u.id}</code>\n"
            f"📛 @{u.username or '—'}\n\n"
        )

        # إعادة إرسال الرسالة الأصلية
        try:
            await bot_inst.send_message(owner, notif, reply_markup=b.as_markup())
            await msg.forward(owner)
        except:
            pass

    # ── الرسائل للمدير ────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_msgs")
    async def c_adm_msgs(cb: CallbackQuery):
        if not await contact_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        rows = await db_read(
            "SELECT * FROM contact_messages WHERE bot_token=? ORDER BY id DESC LIMIT 20",
            (token,)
        )
        b = InlineKeyboardBuilder()
        if not rows:
            b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="c_adm_back"))
            try:
                await cb.message.edit_text("📭 لا توجد رسائل.", reply_markup=b.as_markup())
            except:
                await cb.message.answer("📭 لا توجد رسائل.", reply_markup=b.as_markup())
            return
        for row in rows:
            status = "✅" if row["answered"] else "🔵"
            b.row(InlineKeyboardButton(
                text=f"{status} #{row['id']} — {row['full_name']}",
                callback_data=f"c_view_msg_{row['id']}"
            ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="c_adm_back"))
        try:
            await cb.message.edit_text(
                f"📬 <b>الرسائل ({len(rows)}):</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"📬 <b>الرسائل ({len(rows)}):</b>",
                reply_markup=b.as_markup()
            )

    @r.callback_query(F.data.startswith("c_view_msg_"))
    async def c_view_msg(cb: CallbackQuery):
        msg_id = int(cb.data.split("c_view_msg_")[1])
        row = await db_read(
            "SELECT * FROM contact_messages WHERE id=? AND bot_token=?",
            (msg_id, token), one=True
        )
        if not row:
            return await cb.answer("❌ غير موجود", show_alert=True)
        text = (
            f"📩 <b>رسالة #{row['id']}</b>\n"
            f"👤 {row['full_name']}\n"
            f"🆔 <code>{row['user_id']}</code>\n"
            f"📛 @{row['username'] or '—'}\n"
            f"🕐 {row['created_at']}\n"
            f"{'✅ تم الرد' if row['answered'] else '🔵 لم يُرد بعد'}\n\n"
            f"💬 {row['message']}"
        )
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text="↩️ رد",
            callback_data=f"c_reply_{row['id']}_{row['user_id']}"
        ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="c_adm_msgs"))
        try:
            await cb.message.edit_text(text, reply_markup=b.as_markup())
        except:
            await cb.message.answer(text, reply_markup=b.as_markup())

    @r.callback_query(F.data.startswith("c_reply_"))
    async def c_reply_cb(cb: CallbackQuery, state: FSMContext):
        parts  = cb.data.split("_")
        msg_id = parts[2]
        uid    = parts[3]
        await state.update_data(reply_msg_id=msg_id, reply_user_id=uid)
        await cb.message.answer(f"↩️ أرسل ردك على الرسالة #{msg_id}:")
        await state.set_state(ContactSt.reply_message)

    @r.message(ContactSt.reply_message)
    async def c_process_reply(msg: Message, state: FSMContext):
        data    = await state.get_data()
        msg_id  = data["reply_msg_id"]
        user_id = int(data["reply_user_id"])
        await state.clear()
        try:
            await bot_inst.send_message(
                user_id,
                f"📩 <b>رد على رسالتك #{msg_id}:</b>\n\n{msg.text}"
            )
            await db_write(
                "UPDATE contact_messages SET answered=1 WHERE id=? AND bot_token=?",
                (int(msg_id), token)
            )
            await msg.answer("✅ تم إرسال الرد.")
        except Exception as e:
            await msg.answer(f"❌ فشل الإرسال: {e}")

    # ── لوحة تحكم بوت التواصل ────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_back")
    async def c_adm_back(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        msgs_count = await db_read(
            "SELECT COUNT(*) c FROM contact_messages WHERE bot_token=? AND answered=0",
            (token,), one=True
        )
        unanswered = msgs_count["c"] if msgs_count else 0
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text=f"📬 الرسائل {f'({unanswered} جديد)' if unanswered else ''}",
            callback_data="c_adm_msgs"
        ))
        b.row(
            InlineKeyboardButton(text="📊 الإحصائيات", callback_data="c_adm_stats"),
            InlineKeyboardButton(text="📣 إذاعة",       callback_data="c_adm_bc"),
        )
        b.row(
            InlineKeyboardButton(text="📢 اشتراك إجباري", callback_data="c_adm_force"),
            InlineKeyboardButton(text="🔔 التنبيهات",     callback_data="c_adm_notify"),
        )
        if cb.from_user.id == owner:
            b.row(
                InlineKeyboardButton(text="👥 المشرفين",    callback_data="c_adm_sub_admins"),
                InlineKeyboardButton(text="📝 تعديل الشرح", callback_data="c_adm_help"),
            )
        try:
            await cb.message.edit_text(
                f"👋 لوحة التحكم\n📬 جديد: <b>{unanswered}</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"👋 لوحة التحكم\n📬 جديد: <b>{unanswered}</b>",
                reply_markup=b.as_markup()
            )

    # ── إحصائيات بوت التواصل ─────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_stats")
    async def c_adm_stats(cb: CallbackQuery):
        if not await contact_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        total   = await db_read("SELECT COUNT(*) c FROM sub_users WHERE bot_token=?", (token,), one=True)
        msgs    = await db_read("SELECT COUNT(*) c FROM contact_messages WHERE bot_token=?", (token,), one=True)
        new_m   = await db_read("SELECT COUNT(*) c FROM contact_messages WHERE bot_token=? AND answered=0", (token,), one=True)
        text = (
            f"📊 <b>إحصائيات بوت التواصل:</b>\n\n"
            f"👥 المستخدمون: <b>{total['c'] if total else 0}</b>\n"
            f"💬 إجمالي الرسائل: <b>{msgs['c'] if msgs else 0}</b>\n"
            f"🔵 رسائل جديدة: <b>{new_m['c'] if new_m else 0}</b>"
        )
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="c_adm_back"))
        try:
            await cb.message.edit_text(text, reply_markup=b.as_markup())
        except:
            await cb.message.answer(text, reply_markup=b.as_markup())

    # ── إذاعة بوت التواصل ────────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_bc")
    async def c_adm_bc(cb: CallbackQuery, state: FSMContext):
        if not await contact_is_admin(token, cb.from_user.id):
            return await cb.answer("❌", show_alert=True)
        await cb.message.answer("📣 أرسل رسالة الإذاعة:")
        await state.set_state(ContactSt.broadcast)

    @r.message(ContactSt.broadcast)
    async def c_process_bc(msg: Message, state: FSMContext):
        await state.clear()
        users = await db_read(
            "SELECT user_id FROM sub_users WHERE bot_token=? AND is_active=1",
            (token,)
        )
        if not users:
            return await msg.answer("⚠️ لا يوجد مستخدمون.")
        total   = len(users)
        sm      = await msg.answer(f"📣 جاري الإذاعة لـ <b>{total}</b>...")
        success = 0
        failed  = 0
        for u in users:
            try:
                await msg.copy_to(u["user_id"])
                success += 1
            except TelegramForbiddenError:
                failed += 1
            except TelegramRetryAfter as e:
                await asyncio.sleep(e.retry_after)
                try:
                    await msg.copy_to(u["user_id"])
                    success += 1
                except:
                    failed += 1
            except:
                failed += 1
            await asyncio.sleep(0.05)
        try:
            await sm.edit_text(
                f"✅ الإذاعة:\n📤 نجح: <b>{success}</b>\n❌ فشل: <b>{failed}</b>"
            )
        except:
            pass

    # ── الاشتراك الإجباري لبوت التواصل ──────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_force")
    async def c_adm_force(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        chans = await db_read("SELECT * FROM sub_channels WHERE bot_token=?", (token,))
        b = InlineKeyboardBuilder()
        for c in (chans or []):
            b.row(InlineKeyboardButton(
                text=f"❌ {c['username'] or c['id']}",
                callback_data=f"c_del_chan_{c['id']}"
            ))
        b.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="c_add_chan"))
        b.row(InlineKeyboardButton(text="🔙 عودة",       callback_data="c_adm_back"))
        try:
            await cb.message.edit_text(
                f"📢 القنوات: <b>{len(chans or [])}</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"📢 القنوات: <b>{len(chans or [])}</b>",
                reply_markup=b.as_markup()
            )

    @r.callback_query(F.data == "c_add_chan")
    async def c_add_chan(cb: CallbackQuery, state: FSMContext):
        await cb.message.answer(
            "📢 أرسل رابط القناة أو يوزرنيمها:\n"
            "مثال: <code>@channel</code>\n\n"
            "⚠️ يجب أن يكون البوت مشرفاً في القناة!"
        )
        await state.set_state(ContactSt.add_chan)

    @r.message(ContactSt.add_chan)
    async def c_process_add_chan(msg: Message, state: FSMContext):
        await state.clear()
        raw = msg.text.strip()
        if "t.me/" in raw:
            username = raw.rstrip("/").split("t.me/")[-1].split("/")[0]
        elif raw.startswith("@"):
            username = raw[1:]
        else:
            username = raw
        if not username:
            return await msg.answer("❌ يوزرنيم غير صحيح.")
        wm = await msg.answer(f"⏳ جاري التحقق من @{username}...")
        chat_id_val, chat_title = await get_chat_id_direct(username, token)
        if not chat_id_val:
            try:
                await wm.delete()
            except:
                pass
            return await msg.answer(f"❌ فشل: <code>{chat_title}</code>")
        try:
            me     = await bot_inst.get_me()
            member = await bot_inst.get_chat_member(chat_id=chat_id_val, user_id=me.id)
            if member.status not in ("administrator", "creator"):
                try:
                    await wm.delete()
                except:
                    pass
                return await msg.answer(f"❌ البوت ليس مشرفاً في @{username}.")
            await db_write(
                "INSERT OR REPLACE INTO sub_channels (bot_token,id,url,username) VALUES (?,?,?,?)",
                (token, str(chat_id_val), f"https://t.me/{username}", f"@{username}")
            )
            try:
                await wm.delete()
            except:
                pass
            await msg.answer(f"✅ تمت الإضافة!\n📢 {chat_title}")
        except Exception as e:
            try:
                await wm.delete()
            except:
                pass
            await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")

    @r.callback_query(F.data.startswith("c_del_chan_"))
    async def c_del_chan(cb: CallbackQuery):
        c_id = cb.data[len("c_del_chan_"):]
        await db_write(
            "DELETE FROM sub_channels WHERE bot_token=? AND id=?",
            (token, c_id)
        )
        await cb.answer("✅ تم الحذف")
        await c_adm_force(cb)

    # ── التنبيهات ─────────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_notify")
    async def c_adm_notify(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        ent = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='entry_notify'",
            (token,), one=True
        )
        ev = ent["value"] if ent else "OFF"
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(
            text=f"{'🟢' if ev=='ON' else '🔴'} تنبيه الدخول: {ev}",
            callback_data="c_toggle_entry"
        ))
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="c_adm_back"))
        try:
            await cb.message.edit_text("🔔 التنبيهات:", reply_markup=b.as_markup())
        except:
            await cb.message.answer("🔔 التنبيهات:", reply_markup=b.as_markup())

    @r.callback_query(F.data == "c_toggle_entry")
    async def c_toggle_entry(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='entry_notify'",
            (token,), one=True
        )
        new_val = "OFF" if res and res["value"] == "ON" else "ON"
        await db_write(
            "INSERT OR REPLACE INTO sub_settings (bot_token,key,value) VALUES (?,?,?)",
            (token, "entry_notify", new_val)
        )
        await cb.answer(f"✅ {new_val}")
        await c_adm_notify(cb)

    # ── المشرفون الفرعيون لبوت التواصل ──────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_sub_admins")
    async def c_adm_sub_admins(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        rows   = await db_read("SELECT user_id FROM sub_admins WHERE bot_token=?", (token,))
        admins = [r["user_id"] for r in (rows or [])]
        b = InlineKeyboardBuilder()
        for sa_id in admins:
            try:
                name = (await bot_inst.get_chat(sa_id)).full_name
            except:
                name = f"ID:{sa_id}"
            b.row(InlineKeyboardButton(
                text=f"❌ {name}",
                callback_data=f"c_del_sub_{sa_id}"
            ))
        b.row(InlineKeyboardButton(text="➕ إضافة مشرف", callback_data="c_add_sub"))
        b.row(InlineKeyboardButton(text="🔙 عودة",        callback_data="c_adm_back"))
        try:
            await cb.message.edit_text(
                f"👥 المشرفون: <b>{len(admins)}</b>",
                reply_markup=b.as_markup()
            )
        except:
            await cb.message.answer(
                f"👥 المشرفون: <b>{len(admins)}</b>",
                reply_markup=b.as_markup()
            )

    @r.callback_query(F.data == "c_add_sub")
    async def c_add_sub(cb: CallbackQuery, state: FSMContext):
        await cb.message.answer("➕ أرسل ID المشرف:")
        await state.set_state(ContactSt.add_sub_admin)

    @r.message(ContactSt.add_sub_admin)
    async def c_process_add_sub(msg: Message, state: FSMContext):
        owner = await contact_get_owner(token)
        if msg.from_user.id != owner:
            return
        await state.clear()
        try:
            uid = int(msg.text.strip())
            await db_write(
                "INSERT OR IGNORE INTO sub_admins (bot_token,user_id) VALUES (?,?)",
                (token, uid)
            )
            await msg.answer(f"✅ تم إضافة <code>{uid}</code>.")
        except ValueError:
            await msg.answer("❌ أرسل رقم صحيح.")

    @r.callback_query(F.data.startswith("c_del_sub_"))
    async def c_del_sub(cb: CallbackQuery):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        sa_id = int(cb.data.split("c_del_sub_")[1])
        await db_write(
            "DELETE FROM sub_admins WHERE bot_token=? AND user_id=?",
            (token, sa_id)
        )
        await cb.answer("✅ تم الحذف")
        await c_adm_sub_admins(cb)

    # ── تعديل الشرح ──────────────────────────────────────────────────────────────
    @r.callback_query(F.data == "c_adm_help")
    async def c_adm_help(cb: CallbackQuery, state: FSMContext):
        owner = await contact_get_owner(token)
        if cb.from_user.id != owner:
            return await cb.answer("❌", show_alert=True)
        res = await db_read(
            "SELECT value FROM sub_settings WHERE bot_token=? AND key='help_text'",
            (token,), one=True
        )
        await cb.message.answer(
            f"📝 النص الحالي:\n<i>{res['value'] if res else '—'}</i>\n\nأرسل النص الجديد:"
        )
        await state.set_state(ContactSt.edit_help)

    @r.message(ContactSt.edit_help)
    async def c_process_edit_help(msg: Message, state: FSMContext):
        await state.clear()
        if not msg.text.strip():
            return await msg.answer("❌ النص فارغ.")
        await db_write(
            "INSERT OR REPLACE INTO sub_settings (bot_token,key,value) VALUES (?,?,?)",
            (token, "help_text", msg.text.strip())
        )
        await msg.answer("✅ تم التحديث.")

    return r


# ═══════════════════════════════════════════════════════════════════════════════
# ─── إدارة البوتات الفرعية ───────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════

async def start_sub_bot(token: str, bot_type: str):
    """تشغيل بوت فرعي"""
    if token in running_bots:
        return

    async def run():
        sub_bot = create_bot(token)
        sub_dp  = Dispatcher(storage=MemoryStorage())

        # إضافة الروتر المناسب
        if bot_type == "study":
            router = make_study_router(token, sub_bot)
        else:
            router = make_contact_router(token, sub_bot)

        sub_dp.include_router(router)

        # معالج الأخطاء
        @sub_dp.error()
        async def sub_error(event: ErrorEvent):
            exc = event.exception
            if isinstance(exc, (TelegramForbiddenError, TelegramConflictError, TelegramBadRequest)):
                return
            if isinstance(exc, TelegramRetryAfter):
                await asyncio.sleep(exc.retry_after)
                return
            logger.error(f"SubBot [{token[:20]}] Error: {exc}")

        try:
            await sub_bot.delete_webhook(drop_pending_updates=True)
            logger.warning(f"🚀 بوت {bot_type} [{token[:20]}...] يعمل")
            await sub_dp.start_polling(
                sub_bot,
                allowed_updates=sub_dp.resolve_used_update_types(),
                handle_signals=False,
            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"SubBot [{token[:20]}] Fatal: {e}")
        finally:
            try:
                await sub_bot.session.close()
            except:
                pass
            running_bots.pop(token, None)
            logger.warning(f"⏹ بوت [{token[:20]}...] توقف")

    task = asyncio.create_task(run())
    running_bots[token] = task

async def stop_sub_bot(token: str):
    """إيقاف بوت فرعي"""
    task = running_bots.pop(token, None)
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=5)
        except:
            pass
    logger.warning(f"⏹ أوقفنا بوت [{token[:20]}...]")

async def start_all_saved_bots():
    """تشغيل كل البوتات المحفوظة عند البدء"""
    bots = await db_read("SELECT token, bot_type FROM bots WHERE is_active=1")
    if not bots:
        return
    logger.warning(f"🔄 تشغيل {len(bots)} بوت محفوظ...")
    for bot_rec in bots:
        await asyncio.sleep(0.5)
        await start_sub_bot(bot_rec["token"], bot_rec["bot_type"])

# ─── التشغيل الرئيسي ─────────────────────────────────────────────────────────
async def setup_factory_commands():
    cmds = [
        BotCommand(command="start",  description="🏠 القائمة الرئيسية"),
        BotCommand(command="cancel", description="❌ إلغاء"),
    ]
    await factory_bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

async def main():
    # 1) تهيئة قاعدة البيانات
    init_db()

    # 2) أوامر البوت
    await setup_factory_commands()

    # 3) حذف webhook قديم
    try:
        await factory_bot.delete_webhook(drop_pending_updates=True)
        logger.warning("✅ تم حذف الـ webhook")
    except Exception as e:
        logger.warning(f"delete_webhook: {e}")

    # 4) health server
    await start_health_server()

    # 5) تشغيل كل البوتات المحفوظة
    asyncio.create_task(start_all_saved_bots())

    logger.warning("🚀 صانع البوتات يعمل...")

    # 6) تشغيل صانع البوتات
    await factory_dp.start_polling(
        factory_bot,
        allowed_updates=factory_dp.resolve_used_update_types(),
        handle_signals=True,
    )

if __name__ == "__main__":
    import sys
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("⛔ البوت أُوقف يدوياً.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)