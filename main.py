import logging
import asyncio
import time
import os
import re
import sqlite3
import threading
from datetime import datetime
from typing import Optional
import warnings
warnings.filterwarnings("ignore", message='Field "model_.*".*')

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher, F
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

# ─── إعدادات ──────────────────────────────────────────────────────────────────
TOKEN    = os.environ.get("BOT_TOKEN", "8066171928:AAHXhDfWSWLTFfgBekExFGSyveJSnIT2Dsg")
ADMIN_IDS = [8605977767, 8774463579]
PORT     = int(os.environ.get("PORT", 8080))
DB_PATH  = os.environ.get("DB_PATH", "bot_data.db")

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("bot_errors.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp  = Dispatcher(storage=MemoryStorage())

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

# ─── قاعدة البيانات SQLite (محلية) ──────────────────────────────────────────
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
            CREATE TABLE IF NOT EXISTS users (
                user_id   INTEGER PRIMARY KEY,
                username  TEXT,
                joined_at TEXT,
                is_active INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS sections (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                name      TEXT,
                parent_id INTEGER DEFAULT NULL
            );
            CREATE TABLE IF NOT EXISTS content (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                section_id INTEGER,
                name       TEXT,
                type       TEXT,
                data       TEXT,
                file_id    TEXT,
                pinned     INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime'))
            );
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS channels (
                id       TEXT PRIMARY KEY,
                url      TEXT,
                username TEXT
            );
            CREATE TABLE IF NOT EXISTS sub_admins (
                user_id INTEGER PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS requests (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                username   TEXT,
                full_name  TEXT,
                message    TEXT,
                created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M','now','localtime')),
                answered   INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_sections_parent ON sections(parent_id);
            CREATE INDEX IF NOT EXISTS idx_content_section ON content(section_id);
        """)
        # أقسام افتراضية
        cur = c.execute("SELECT COUNT(*) FROM sections WHERE parent_id IS NULL")
        if cur.fetchone()[0] == 0:
            for sec in ["الملازم", "التحفيز", "ارشادات للدراسة", "الملخصات"]:
                c.execute("INSERT INTO sections (name) VALUES (?)", (sec,))
        # إعدادات افتراضية
        for k, v in [
            ("sub_notify",  "OFF"),
            ("entry_notify","OFF"),
            ("help_text",   "أهلاً بك في بوت المساعد الدراسي 🎓"),
        ]:
            c.execute("INSERT OR IGNORE INTO settings (key,value) VALUES (?,?)", (k, v))
        conn.commit()
        conn.close()
    logger.warning("✅ SQLite جاهزة")

async def db_read(query: str, params=(), one=False):
    loop = asyncio.get_event_loop()
    def _read():
        with _db_lock:
            conn = get_conn()
            c = conn.execute(query, params)
            rows = c.fetchone() if one else c.fetchall()
            conn.close()
            if one:
                return dict(rows) if rows else None
            return [dict(r) for r in rows]
    return await loop.run_in_executor(None, _read)

async def db_write(query: str, params=(), ret_id=False):
    loop = asyncio.get_event_loop()
    def _write():
        with _db_lock:
            conn = get_conn()
            c = conn.execute(query, params)
            last_id = c.lastrowid
            conn.commit()
            conn.close()
            return last_id if ret_id else None
    return await loop.run_in_executor(None, _write)

# ─── HTTP Health Check بـ aiohttp ────────────────────────────────────────────
async def health_handler(request):
    return web.Response(text="OK")

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
            logger.warning(f"✅ HTTP health server على 0.0.0.0:{PORT}")
            return
        except OSError as e:
            logger.warning(f"⚠️ محاولة {attempt+1}/10: {e} — إعادة بعد 3 ث")
            await asyncio.sleep(3)
    logger.error("❌ فشل تشغيل health server — البوت يكمل بدونه")

# ─── مساعدات ──────────────────────────────────────────────────────────────────
async def get_sub_admins() -> list:
    cached = cache_get("sub_admins")
    if cached is not None:
        return cached
    rows = await db_read("SELECT user_id FROM sub_admins")
    result = [r["user_id"] for r in (rows or [])]
    cache_set("sub_admins", result)
    return result

async def is_admin(uid: int) -> bool:
    return uid in ADMIN_IDS or uid in await get_sub_admins()

async def check_subscription(uid: int) -> bool:
    if uid in ADMIN_IDS or await is_admin(uid):
        return True
    chans = cache_get("channels_list")
    if chans is None:
        chans = await db_read("SELECT id FROM channels")
        cache_set("channels_list", chans)
    if not chans:
        return True
    for c in chans:
        try:
            m = await bot.get_chat_member(chat_id=c["id"], user_id=uid)
            if m.status in ("left", "kicked"):
                return False
        except:
            pass
    return True

async def get_sub_kb() -> InlineKeyboardMarkup:
    chans = await db_read("SELECT id,url FROM channels")
    b = InlineKeyboardBuilder()
    for c in chans or []:
        b.row(InlineKeyboardButton(text="📢 انضم للقناة", url=c["url"]))
    b.row(InlineKeyboardButton(text="✅ تحققت من الاشتراك", callback_data="check_sub"))
    return b.as_markup()

async def get_main_kb(uid: int) -> ReplyKeyboardMarkup:
    secs = cache_get("main_secs")
    if secs is None:
        secs = await db_read("SELECT name FROM sections WHERE parent_id IS NULL")
        cache_set("main_secs", secs)
    icons = {"الملازم": "🗂", "التحفيز": "💡", "ارشادات": "📝", "الملخصات": "📚"}
    btns = []
    for s in secs or []:
        icon = next((v for k, v in icons.items() if k in s["name"]), "📁")
        btns.append(KeyboardButton(text=f"{icon} {s['name']}"))
    btns += [
        KeyboardButton(text="🔍 بحث"),
        KeyboardButton(text="📢 قناة البوت"),
        KeyboardButton(text="🔬 السادس علمي"),
        KeyboardButton(text="ℹ️ شرح البوت"),
        KeyboardButton(text="📩 طلب ملزمة"),
    ]
    b = ReplyKeyboardBuilder()
    for i in range(0, len(btns), 2):
        b.row(*btns[i: i + 2])
    if await is_admin(uid):
        b.row(KeyboardButton(text="🛠 لوحة التحكم"))
    return b.as_markup(resize_keyboard=True)

async def setup_commands():
    cmds = [
        BotCommand(command="start",   description="🏠 القائمة الرئيسية"),
        BotCommand(command="search",  description="🔍 بحث في المحتوى"),
        BotCommand(command="help",    description="ℹ️ شرح البوت"),
        BotCommand(command="cancel",  description="❌ إلغاء العملية"),
        BotCommand(command="request", description="📩 طلب ملزمة"),
    ]
    await bot.set_my_commands(cmds, scope=BotCommandScopeDefault())

# ─── الحالات ──────────────────────────────────────────────────────────────────
class St(StatesGroup):
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

# ─── معالج الأخطاء ────────────────────────────────────────────────────────────
@dp.error()
async def on_error(event: ErrorEvent):
    exc = event.exception
    if isinstance(exc, (TelegramForbiddenError, TelegramConflictError, TelegramBadRequest)):
        return
    if isinstance(exc, TelegramRetryAfter):
        await asyncio.sleep(exc.retry_after)
        return
    logger.error(f"DP Error: {exc}")

# ─── تنظيف المحظورين ──────────────────────────────────────────────────────────
async def cleanup_blocked():
    while True:
        try:
            await asyncio.sleep(86400)
            users = await db_read("SELECT user_id FROM users")
            removed = 0
            for u in users or []:
                try:
                    await bot.send_chat_action(u["user_id"], "typing")
                except TelegramForbiddenError:
                    await db_write("DELETE FROM users WHERE user_id=?", (u["user_id"],))
                    removed += 1
                except:
                    pass
            if removed:
                for aid in ADMIN_IDS:
                    try:
                        await bot.send_message(aid, f"🧹 تم حذف <b>{removed}</b> مستخدم محظور.")
                    except:
                        pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"cleanup: {e}")

# ─── /start ───────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    existing = await db_read("SELECT user_id FROM users WHERE user_id=?", (u.id,), one=True)
    if not existing:
        await db_write(
            "INSERT OR IGNORE INTO users (user_id,username,joined_at) VALUES (?,?,?)",
            (u.id, u.username, datetime.now().isoformat()),
        )
        notify = await db_read("SELECT value FROM settings WHERE key='entry_notify'", one=True)
        if notify and notify["value"] == "ON":
            all_admins = list(ADMIN_IDS) + await get_sub_admins()
            for aid in set(all_admins):
                try:
                    await bot.send_message(
                        aid,
                        f"👤 <b>مستخدم جديد:</b>\n{u.full_name}\n@{u.username or '—'}\n<code>{u.id}</code>",
                    )
                except:
                    pass
    if not await check_subscription(u.id):
        return await msg.answer("⚠️ يجب الاشتراك أولاً:", reply_markup=await get_sub_kb())
    ht = await db_read("SELECT value FROM settings WHERE key='help_text'", one=True)
    await msg.answer(ht["value"] if ht else "مرحباً 🎓", reply_markup=await get_main_kb(u.id))

@dp.message(Command("cancel"))
async def cmd_cancel(msg: Message, state: FSMContext):
    await state.clear()
    await msg.answer("❌ تم الإلغاء.", reply_markup=await get_main_kb(msg.from_user.id))

@dp.message(Command("help"))
async def cmd_help(msg: Message):
    res = await db_read("SELECT value FROM settings WHERE key='help_text'", one=True)
    await msg.answer(res["value"] if res else "لا يوجد شرح.")

# ─── التحقق من الاشتراك ───────────────────────────────────────────────────────
@dp.callback_query(F.data == "check_sub")
async def cb_check_sub(cb: CallbackQuery):
    if await check_subscription(cb.from_user.id):
        notify = await db_read("SELECT value FROM settings WHERE key='sub_notify'", one=True)
        if notify and notify["value"] == "ON":
            all_admins = list(ADMIN_IDS) + await get_sub_admins()
            for aid in set(all_admins):
                try:
                    await bot.send_message(aid, f"✅ انضم: {cb.from_user.full_name} | <code>{cb.from_user.id}</code>")
                except:
                    pass
        try:
            await cb.message.delete()
        except:
            pass
        await cb.message.answer("✅ أهلاً بك 🎓", reply_markup=await get_main_kb(cb.from_user.id))
    else:
        await cb.answer("❌ لم تشترك بعد في جميع القنوات.", show_alert=True)

# ─── لوحة التحكم ──────────────────────────────────────────────────────────────
def admin_builder(is_full: bool):
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="📂 إدارة الأقسام", callback_data="adm_secs"))
    b.row(
        InlineKeyboardButton(text="📊 الإحصائيات", callback_data="adm_stats"),
        InlineKeyboardButton(text="📣 إذاعة",       callback_data="adm_bc"),
    )
    b.row(InlineKeyboardButton(text="📩 الطلبات", callback_data="adm_requests"))
    if is_full:
        b.row(
            InlineKeyboardButton(text="👥 المشرفين",          callback_data="adm_sub_admins"),
            InlineKeyboardButton(text="📢 الاشتراك الإجباري", callback_data="adm_force"),
        )
        b.row(
            InlineKeyboardButton(text="🔔 التنبيهات",   callback_data="adm_notify"),
            InlineKeyboardButton(text="📝 تعديل الشرح", callback_data="adm_help"),
        )
    return b

@dp.message(F.text == "🛠 لوحة التحكم")
async def admin_panel(msg: Message, state: FSMContext):
    uid = msg.from_user.id
    if not await is_admin(uid):
        return
    await state.clear()
    b = admin_builder(uid in ADMIN_IDS)
    title = "🛠 <b>لوحة تحكم المسؤول:</b>" if uid in ADMIN_IDS else "🛠 <b>لوحة المشرف الفرعي:</b>"
    await msg.answer(title, reply_markup=b.as_markup())

async def send_panel(msg: Message, uid: int):
    b = admin_builder(uid in ADMIN_IDS)
    title = "🛠 <b>لوحة تحكم المسؤول:</b>" if uid in ADMIN_IDS else "🛠 <b>لوحة المشرف الفرعي:</b>"
    try:
        await msg.edit_text(title, reply_markup=b.as_markup())
    except:
        await msg.answer(title, reply_markup=b.as_markup())

@dp.callback_query(F.data == "adm_back")
async def cb_back(cb: CallbackQuery):
    await send_panel(cb.message, cb.from_user.id)

# ─── الطلبات ──────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_requests")
async def cb_adm_requests(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    rows = await db_read("SELECT * FROM requests ORDER BY id DESC LIMIT 20")
    b = InlineKeyboardBuilder()
    if not rows:
        b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
        try:
            await cb.message.edit_text("📩 لا توجد طلبات.", reply_markup=b.as_markup())
        except:
            await cb.message.answer("📩 لا توجد طلبات.", reply_markup=b.as_markup())
        return
    for r in rows:
        status = "✅" if r["answered"] else "🔵"
        b.row(InlineKeyboardButton(
            text=f"{status} #{r['id']} — {r['full_name']}",
            callback_data=f"view_req_{r['id']}"
        ))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    try:
        await cb.message.edit_text(f"📩 <b>الطلبات ({len(rows)}):</b>", reply_markup=b.as_markup())
    except:
        await cb.message.answer(f"📩 <b>الطلبات ({len(rows)}):</b>", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("view_req_"))
async def cb_view_req(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    req_id = int(cb.data.split("view_req_")[1])
    req = await db_read("SELECT * FROM requests WHERE id=?", (req_id,), one=True)
    if not req:
        return await cb.answer("❌ غير موجود", show_alert=True)
    text = (
        f"📩 <b>طلب #{req['id']}</b>\n👤 {req['full_name']}\n"
        f"🆔 <code>{req['user_id']}</code>\n📛 @{req['username'] or '—'}\n"
        f"🕐 {req['created_at']}\n{'✅ تمت الإجابة' if req['answered'] else '🔵 لم يُجب بعد'}"
        f"\n\n💬 {req['message']}"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="↩️ رد", callback_data=f"reply_req_{req['id']}_{req['user_id']}"))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_requests"))
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("reply_req_"))
async def cb_reply_req(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    parts = cb.data.split("_")
    req_id, user_id = parts[2], parts[3]
    await state.update_data(reply_req_id=req_id, reply_user_id=user_id)
    await cb.message.answer(f"↩️ أرسل ردك على الطلب #{req_id}:")
    await state.set_state(St.reply_request)

@dp.message(St.reply_request)
async def process_reply_request(msg: Message, state: FSMContext):
    data = await state.get_data()
    req_id  = data["reply_req_id"]
    user_id = int(data["reply_user_id"])
    await state.clear()
    try:
        await bot.send_message(user_id, f"📩 <b>رد على طلبك #{req_id}:</b>\n\n{msg.text}")
        await db_write("UPDATE requests SET answered=1 WHERE id=?", (int(req_id),))
        await msg.answer("✅ تم إرسال الرد.")
    except Exception as e:
        await msg.answer(f"❌ فشل الإرسال: {e}")

# ─── الإحصائيات ───────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_stats")
async def cb_stats(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    total  = await db_read("SELECT COUNT(*) c FROM users", one=True)
    active = await db_read("SELECT COUNT(*) c FROM users WHERE is_active=1", one=True)
    secs   = await db_read("SELECT COUNT(*) c FROM sections", one=True)
    cons   = await db_read("SELECT COUNT(*) c FROM content", one=True)
    pinned = await db_read("SELECT COUNT(*) c FROM content WHERE pinned=1", one=True)
    reqs   = await db_read("SELECT COUNT(*) c FROM requests", one=True)
    top_sec = await db_read(
        "SELECT s.name, COUNT(c.id) cnt FROM sections s "
        "LEFT JOIN content c ON c.section_id=s.id GROUP BY s.id ORDER BY cnt DESC LIMIT 3"
    )
    top_txt = "\n".join(f"  • {r['name']}: {r['cnt']}" for r in (top_sec or []))
    recent  = await db_read("SELECT name, created_at FROM content ORDER BY id DESC LIMIT 5")
    recent_txt = "\n".join(
        f"  • {r['name']} ({r['created_at'][:10] if r['created_at'] else '—'})" for r in (recent or [])
    )
    text = (
        f"📊 <b>إحصائيات البوت:</b>\n\n"
        f"👥 إجمالي المستخدمين: <b>{total['c'] if total else 0}</b>\n"
        f"✅ المستخدمون الفعليون: <b>{active['c'] if active else 0}</b>\n"
        f"📂 الأقسام: <b>{secs['c'] if secs else 0}</b>\n"
        f"📄 المحتويات: <b>{cons['c'] if cons else 0}</b>\n"
        f"📌 المثبتة: <b>{pinned['c'] if pinned else 0}</b>\n"
        f"📩 الطلبات: <b>{reqs['c'] if reqs else 0}</b>\n\n"
        f"🏆 <b>أكثر الأقسام محتوىً:</b>\n{top_txt or '—'}\n\n"
        f"🆕 <b>آخر المضاف:</b>\n{recent_txt or '—'}"
    )
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    try:
        await cb.message.edit_text(text, reply_markup=b.as_markup())
    except:
        await cb.message.answer(text, reply_markup=b.as_markup())

# ─── المشرفون الفرعيون ────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_sub_admins")
async def cb_sub_admins(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    sub_admins = await get_sub_admins()
    b = InlineKeyboardBuilder()
    for sa_id in sub_admins:
        try:
            name = (await bot.get_chat(sa_id)).full_name
        except:
            name = f"ID:{sa_id}"
        b.row(InlineKeyboardButton(text=f"❌ حذف {name}", callback_data=f"del_sub_{sa_id}"))
    if not sub_admins:
        b.row(InlineKeyboardButton(text="لا يوجد مشرفون فرعيون", callback_data="noop"))
    b.row(InlineKeyboardButton(text="➕ إضافة مشرف فرعي", callback_data="add_sub"))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    txt = f"👥 <b>المشرفون الفرعيون:</b> <b>{len(sub_admins)}</b>"
    try:
        await cb.message.edit_text(txt, reply_markup=b.as_markup())
    except:
        await cb.message.answer(txt, reply_markup=b.as_markup())

@dp.callback_query(F.data == "add_sub")
async def cb_add_sub(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("➕ أرسل ID المشرف الجديد:")
    await state.set_state(St.add_sub_admin)

@dp.message(St.add_sub_admin)
async def process_add_sub(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    try:
        uid = int(msg.text.strip())
        if uid in ADMIN_IDS:
            return await msg.answer("❌ مسؤول رئيسي بالفعل.")
        if uid in await get_sub_admins():
            return await msg.answer("❌ مشرف فرعي بالفعل.")
        await db_write("INSERT OR IGNORE INTO sub_admins (user_id) VALUES (?)", (uid,))
        cache_del("sub_admins")
        await msg.answer(f"✅ تم إضافة <code>{uid}</code> كمشرف فرعي.")
    except ValueError:
        await msg.answer("❌ أرسل رقم صحيح.")
    except Exception as e:
        await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")

@dp.callback_query(F.data.startswith("del_sub_"))
async def cb_del_sub(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    try:
        sa_id = int(cb.data.split("del_sub_")[1])
        await db_write("DELETE FROM sub_admins WHERE user_id=?", (sa_id,))
        cache_del("sub_admins")
        await cb.answer(f"✅ تم حذف المشرف {sa_id}")
    except Exception as e:
        logger.error(f"del_sub: {e}")
        await cb.answer("❌ خطأ أثناء الحذف", show_alert=True)
    await cb_sub_admins(cb)

# ─── إدارة الأقسام ────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_secs")
async def cb_manage_secs(cb: CallbackQuery):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    secs = await db_read("SELECT * FROM sections WHERE parent_id IS NULL")
    b = InlineKeyboardBuilder()
    for s in secs or []:
        b.row(InlineKeyboardButton(text=f"📁 {s['name']}", callback_data=f"adm_sec_{s['id']}"))
    b.row(InlineKeyboardButton(text="➕ إضافة قسم رئيسي", callback_data="add_sec_main"))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    try:
        await cb.message.edit_text("📂 <b>الأقسام الرئيسية:</b>", reply_markup=b.as_markup())
    except:
        await cb.message.answer("📂 <b>الأقسام الرئيسية:</b>", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("adm_sec_"))
async def cb_view_sec(cb: CallbackQuery):
    sec_id = int(cb.data.split("adm_sec_")[1])
    sec  = await db_read("SELECT * FROM sections WHERE id=?", (sec_id,), one=True)
    if not sec:
        return await cb.answer("القسم غير موجود", show_alert=True)
    subs = await db_read("SELECT * FROM sections WHERE parent_id=?", (sec_id,))
    cons = await db_read("SELECT * FROM content WHERE section_id=? ORDER BY pinned DESC,id DESC", (sec_id,))
    b = InlineKeyboardBuilder()
    for s in subs or []:
        b.row(InlineKeyboardButton(text=f"📂 {s['name']}", callback_data=f"adm_sec_{s['id']}"))
    for c in cons or []:
        icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵"}.get(c["type"],"📝")
        pin  = "📌" if c["pinned"] else ""
        b.row(InlineKeyboardButton(text=f"{pin}{icon} {c['name']}", callback_data=f"adm_con_{c['id']}"))
    b.row(
        InlineKeyboardButton(text="➕ قسم فرعي", callback_data=f"add_sec_sub_{sec_id}"),
        InlineKeyboardButton(text="➕ محتوى",    callback_data=f"add_con_{sec_id}"),
    )
    b.row(InlineKeyboardButton(text="🗑 حذف القسم", callback_data=f"del_sec_{sec_id}"))
    back = f"adm_sec_{sec['parent_id']}" if sec["parent_id"] else "adm_secs"
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data=back))
    try:
        await cb.message.edit_text(
            f"📂 <b>{sec['name']}</b>\n📂 فرعية: {len(subs or [])} | 📄 محتوى: {len(cons or [])}",
            reply_markup=b.as_markup(),
        )
    except:
        await cb.message.answer(f"📂 <b>{sec['name']}</b>", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("adm_con_"))
async def cb_adm_con(cb: CallbackQuery):
    c_id = int(cb.data.split("adm_con_")[1])
    item = await db_read("SELECT * FROM content WHERE id=?", (c_id,), one=True)
    if not item:
        return await cb.answer("❌ غير موجود", show_alert=True)
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text="📌 إلغاء التثبيت" if item["pinned"] else "📌 تثبيت",
        callback_data=f"toggle_pin_{c_id}"
    ))
    b.row(InlineKeyboardButton(text="🗑 حذف المحتوى", callback_data=f"del_con_{c_id}"))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data=f"adm_sec_{item['section_id']}"))
    date_str = item["created_at"][:10] if item["created_at"] else "—"
    try:
        await cb.message.edit_text(
            f"📎 <b>{item['name']}</b>\nالنوع: {item['type']} | مثبت: {'نعم' if item['pinned'] else 'لا'}\n📅 {date_str}",
            reply_markup=b.as_markup(),
        )
    except:
        await cb.message.answer(f"📎 {item['name']}", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("toggle_pin_"))
async def cb_toggle_pin(cb: CallbackQuery):
    c_id = int(cb.data.split("toggle_pin_")[1])
    item = await db_read("SELECT pinned FROM content WHERE id=?", (c_id,), one=True)
    if not item:
        return await cb.answer("❌ غير موجود", show_alert=True)
    new_pin = 0 if item["pinned"] else 1
    await db_write("UPDATE content SET pinned=? WHERE id=?", (new_pin, c_id))
    cache_clear_all()
    await cb.answer("📌 تم التثبيت" if new_pin else "✅ تم إلغاء التثبيت")
    cb.data = f"adm_con_{c_id}"
    await cb_adm_con(cb)

@dp.callback_query(F.data.startswith("add_sec_"))
async def cb_add_sec(cb: CallbackQuery, state: FSMContext):
    parts     = cb.data.split("_")
    parent_id = parts[3] if len(parts) > 3 and parts[2] == "sub" else None
    await cb.message.answer("📝 أرسل اسم القسم الجديد:")
    await state.set_state(St.add_sec_name)
    await state.update_data(parent_id=parent_id)

@dp.message(St.add_sec_name)
async def process_sec_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    name = msg.text.strip()
    if not name:
        return await msg.answer("❌ الاسم فارغ، أرسل مجدداً:")
    try:
        parent_id = int(data["parent_id"]) if data.get("parent_id") else None
        row_id = await db_write(
            "INSERT INTO sections (name,parent_id) VALUES (?,?)",
            (name, parent_id), ret_id=True,
        )
        cache_del("main_secs")
        await msg.answer(f"✅ تم إضافة '<b>{name}</b>' (ID: {row_id})")
    except Exception as e:
        await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")
    await state.clear()

async def delete_section_recursive(sec_id: int, deleted_by: int = None):
    sec_info  = await db_read("SELECT name FROM sections WHERE id=?", (sec_id,), one=True)
    contents  = await db_read("SELECT name FROM content WHERE section_id=?", (sec_id,))
    con_names = ", ".join([c["name"] for c in contents]) if contents else "لا يوجد"
    await db_write("DELETE FROM content WHERE section_id=?", (sec_id,))
    subs = await db_read("SELECT id FROM sections WHERE parent_id=?", (sec_id,))
    for sub in subs or []:
        await delete_section_recursive(sub["id"], deleted_by)
    await db_write("DELETE FROM sections WHERE id=?", (sec_id,))
    if deleted_by:
        try:
            deleter_name = (await bot.get_chat(deleted_by)).full_name
        except:
            deleter_name = f"ID:{deleted_by}"
        notif = (
            f"🗑 <b>تنبيه حذف قسم</b>\n\n👮 <b>{deleter_name}</b> (<code>{deleted_by}</code>)\n"
            f"📂 القسم: <b>{sec_info['name'] if sec_info else sec_id}</b>\n"
            f"📄 المحتويات: {con_names}\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        for aid in ADMIN_IDS:
            if aid != deleted_by:
                try:
                    await bot.send_message(aid, notif)
                except:
                    pass

@dp.callback_query(F.data.startswith("del_sec_"))
async def cb_del_sec(cb: CallbackQuery):
    sec_id = int(cb.data.split("del_sec_")[1])
    sec    = await db_read("SELECT * FROM sections WHERE id=?", (sec_id,), one=True)
    if not sec:
        return await cb.answer("القسم غير موجود", show_alert=True)
    try:
        await delete_section_recursive(sec_id, deleted_by=cb.from_user.id)
        cache_clear_all()
        await cb.answer("✅ تم حذف القسم نهائياً")
    except Exception as e:
        logger.error(f"del_sec: {e}")
        return await cb.answer("❌ خطأ أثناء الحذف", show_alert=True)
    if sec["parent_id"]:
        cb.data = f"adm_sec_{sec['parent_id']}"
        await cb_view_sec(cb)
    else:
        await cb_manage_secs(cb)

@dp.callback_query(F.data.startswith("add_con_"))
async def cb_add_con(cb: CallbackQuery, state: FSMContext):
    sec_id = cb.data.split("add_con_")[1]
    await cb.message.answer("📝 أرسل اسم/عنوان المحتوى:")
    await state.set_state(St.add_con_name)
    await state.update_data(sec_id=sec_id)

@dp.message(St.add_con_name)
async def process_con_name(msg: Message, state: FSMContext):
    await state.update_data(con_name=msg.text)
    await msg.answer("📎 أرسل المحتوى (نص، صورة، ملف، صوت، فيديو):")
    await state.set_state(St.add_con_data)

@dp.message(St.add_con_data)
async def process_con_data(msg: Message, state: FSMContext):
    data = await state.get_data()
    c_type, c_data, f_id = "text", msg.text or "", None
    if msg.photo:
        c_type, c_data, f_id = "photo",      msg.caption or "", msg.photo[-1].file_id
    elif msg.document:
        c_type, c_data, f_id = "doc",        msg.caption or "", msg.document.file_id
    elif msg.voice:
        c_type, c_data, f_id = "voice",      msg.caption or "", msg.voice.file_id
    elif msg.video:
        c_type, c_data, f_id = "video",      msg.caption or "", msg.video.file_id
    elif msg.audio:
        c_type, c_data, f_id = "audio",      msg.caption or "", msg.audio.file_id
    elif msg.video_note:
        c_type, c_data, f_id = "video_note", "",               msg.video_note.file_id
    try:
        row_id = await db_write(
            "INSERT INTO content (section_id,name,type,data,file_id,created_at) VALUES (?,?,?,?,?,?)",
            (int(data["sec_id"]), data["con_name"], c_type, c_data, f_id,
             datetime.now().strftime("%Y-%m-%d %H:%M")),
            ret_id=True,
        )
        cache_clear_all()
        await msg.answer(f"✅ <b>تم الحفظ!</b>\n📌 {data['con_name']}\n📎 {c_type}\n🆔 {row_id}")
        adder_id = msg.from_user.id
        try:
            adder_name = (await bot.get_chat(adder_id)).full_name
        except:
            adder_name = f"ID:{adder_id}"
        sec_info = await db_read("SELECT name FROM sections WHERE id=?", (int(data["sec_id"]),), one=True)
        notif = (
            f"➕ <b>تنبيه إضافة محتوى</b>\n\n👮 <b>{adder_name}</b> (<code>{adder_id}</code>)\n"
            f"📄 <b>{data['con_name']}</b> | {c_type}\n"
            f"📂 {sec_info['name'] if sec_info else '—'}\n🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )
        for aid in ADMIN_IDS:
            if aid != adder_id:
                try:
                    await bot.send_message(aid, notif)
                except:
                    pass
    except Exception as e:
        await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")
    await state.clear()

@dp.callback_query(F.data.startswith("del_con_"))
async def cb_del_con(cb: CallbackQuery):
    c_id = cb.data.split("del_con_")[1]
    item = await db_read("SELECT name,section_id FROM content WHERE id=?", (int(c_id),), one=True)
    if not item:
        return await cb.answer("❌ غير موجود", show_alert=True)
    b = InlineKeyboardBuilder()
    b.row(
        InlineKeyboardButton(text="✅ نعم، احذف", callback_data=f"confirm_del_{c_id}"),
        InlineKeyboardButton(text="❌ إلغاء",     callback_data=f"cancel_del_{item['section_id']}"),
    )
    try:
        await cb.message.edit_text(f"⚠️ حذف <b>{item['name']}</b>؟\nلا يمكن التراجع.", reply_markup=b.as_markup())
    except:
        await cb.message.answer(f"⚠️ حذف {item['name']}؟", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("confirm_del_"))
async def cb_confirm_del(cb: CallbackQuery):
    c_id = cb.data.split("confirm_del_")[1]
    item = await db_read("SELECT * FROM content WHERE id=?", (int(c_id),), one=True)
    if not item:
        return await cb.answer("❌ محذوف مسبقاً", show_alert=True)
    deleter_id = cb.from_user.id
    await db_write("DELETE FROM content WHERE id=?", (int(c_id),))
    cache_clear_all()
    await cb.answer("✅ تم الحذف نهائياً")
    try:
        deleter_name = (await bot.get_chat(deleter_id)).full_name
    except:
        deleter_name = f"ID:{deleter_id}"
    sec_info = await db_read("SELECT name FROM sections WHERE id=?", (item["section_id"],), one=True)
    notif = (
        f"🗑 <b>تنبيه حذف ملف</b>\n\n👮 <b>{deleter_name}</b> (<code>{deleter_id}</code>)\n"
        f"📄 <b>{item['name']}</b>\n📂 {sec_info['name'] if sec_info else '—'}\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    for aid in ADMIN_IDS:
        if aid != deleter_id:
            try:
                await bot.send_message(aid, notif)
            except:
                pass
    cb.data = f"adm_sec_{item['section_id']}"
    await cb_view_sec(cb)

@dp.callback_query(F.data.startswith("cancel_del_"))
async def cb_cancel_del(cb: CallbackQuery):
    sec_id = cb.data.split("cancel_del_")[1]
    await cb.answer("❌ إلغاء الحذف")
    cb.data = f"adm_sec_{sec_id}"
    await cb_view_sec(cb)

# ─── البحث ───────────────────────────────────────────────────────────────────
@dp.message(F.text == "🔍 بحث")
@dp.message(Command("search"))
async def cmd_search(msg: Message, state: FSMContext):
    await msg.answer("🔍 أرسل كلمة البحث:")
    await state.set_state(St.search_query)

@dp.message(St.search_query)
async def process_search(msg: Message, state: FSMContext):
    query = msg.text.strip()
    if len(query) < 2:
        return await msg.answer("❌ كلمة البحث قصيرة جداً.")
    await state.clear()
    secs = await db_read("SELECT * FROM sections WHERE name LIKE ?", (f"%{query}%",))
    cons = await db_read(
        "SELECT * FROM content WHERE name LIKE ? OR data LIKE ?",
        (f"%{query}%", f"%{query}%")
    )
    if not secs and not cons:
        return await msg.answer("❌ لم يتم العثور على نتائج.")
    b = InlineKeyboardBuilder()
    res_text = f"🔍 نتائج البحث عن: <b>{query}</b>\n"
    if secs:
        res_text += f"\n📂 <b>الأقسام ({len(secs)}):</b>"
        for s in secs:
            b.row(InlineKeyboardButton(text=f"📁 {s['name']}", callback_data=f"user_sec_{s['id']}"))
    if cons:
        res_text += f"\n📄 <b>الملفات ({len(cons)}):</b>"
        for c in cons:
            icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵"}.get(c["type"],"📝")
            b.row(InlineKeyboardButton(text=f"{icon} {c['name']}", callback_data=f"user_view_{c['id']}"))
    await msg.answer(res_text, reply_markup=b.as_markup())

# ─── طلب ملزمة ───────────────────────────────────────────────────────────────
@dp.message(F.text == "📩 طلب ملزمة")
@dp.message(Command("request"))
async def cmd_request(msg: Message, state: FSMContext):
    await msg.answer("📩 أرسل اسم الملزمة أو الملف الذي تحتاجه:")
    await state.set_state(St.send_request)

@dp.message(St.send_request)
async def process_send_request(msg: Message, state: FSMContext):
    if not msg.text:
        return await msg.answer("❌ يرجى إرسال نص الطلب.")
    await state.clear()
    u = msg.from_user
    req_id = await db_write(
        "INSERT INTO requests (user_id, username, full_name, message) VALUES (?,?,?,?)",
        (u.id, u.username, u.full_name, msg.text), ret_id=True,
    )
    await msg.answer(f"✅ تم إرسال طلبك (رقم #{req_id}). سيتم الرد قريباً.")
    notif = f"📩 <b>طلب جديد #{req_id}</b>\n👤 {u.full_name} (@{u.username or '—'})\n💬 {msg.text}"
    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, notif)
        except:
            pass

# ─── عرض الأقسام للمستخدمين ──────────────────────────────────────────────────
ICON_RE = re.compile(r"^(🗂|💡|📝|📚|📁)\s+")

@dp.message(F.text.regexp(r"^(🗂|💡|📝|📚|📁)\s+"))
async def show_section(msg: Message):
    if not await check_subscription(msg.from_user.id):
        return await msg.answer("⚠️ اشترك أولاً:", reply_markup=await get_sub_kb())
    name = ICON_RE.sub("", msg.text).strip()
    sec  = await db_read("SELECT * FROM sections WHERE name=?", (name,), one=True)
    if not sec:
        return
    await send_user_section(msg, sec["id"])

async def send_user_section(msg: Message, sec_id: int):
    sec  = await db_read("SELECT * FROM sections WHERE id=?", (sec_id,), one=True)
    if not sec:
        return
    subs = await db_read("SELECT * FROM sections WHERE parent_id=?", (sec_id,))
    cons = await db_read("SELECT * FROM content WHERE section_id=? ORDER BY pinned DESC,id DESC", (sec_id,))
    b = InlineKeyboardBuilder()
    for s in subs or []:
        display = s["name"].split(" - ")[-1] if " - " in s["name"] else s["name"]
        b.row(InlineKeyboardButton(text=f"📂 {display}", callback_data=f"user_sec_{s['id']}"))
    for c in cons or []:
        icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵","video_note":"📹"}.get(c["type"],"📝")
        pin  = "📌" if c["pinned"] else ""
        b.row(InlineKeyboardButton(text=f"{pin}{icon} {c['name']}", callback_data=f"user_view_{c['id']}"))
    if not subs and not cons:
        b.row(InlineKeyboardButton(text="📭 القسم فارغ", callback_data="noop"))
    await msg.answer(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("user_sec_"))
async def cb_user_sec(cb: CallbackQuery):
    sec_id = int(cb.data.split("user_sec_")[1])
    sec    = await db_read("SELECT * FROM sections WHERE id=?", (sec_id,), one=True)
    if not sec:
        return await cb.answer("القسم غير موجود", show_alert=True)
    subs = await db_read("SELECT * FROM sections WHERE parent_id=?", (sec_id,))
    cons = await db_read("SELECT * FROM content WHERE section_id=? ORDER BY pinned DESC,id DESC", (sec_id,))
    b = InlineKeyboardBuilder()
    for s in subs or []:
        display = s["name"].split(" - ")[-1] if " - " in s["name"] else s["name"]
        b.row(InlineKeyboardButton(text=f"📂 {display}", callback_data=f"user_sec_{s['id']}"))
    for c in cons or []:
        icon = {"photo":"🖼","doc":"📄","voice":"🎤","video":"🎥","audio":"🎵","video_note":"📹"}.get(c["type"],"📝")
        pin  = "📌" if c["pinned"] else ""
        b.row(InlineKeyboardButton(text=f"{pin}{icon} {c['name']}", callback_data=f"user_view_{c['id']}"))
    if not subs and not cons:
        b.row(InlineKeyboardButton(text="📭 القسم فارغ", callback_data="noop"))
    if sec["parent_id"]:
        b.row(InlineKeyboardButton(text="🔙 رجوع", callback_data=f"user_sec_{sec['parent_id']}"))
    try:
        await cb.message.edit_text(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())
    except:
        await cb.message.answer(f"📚 <b>{sec['name']}</b>:", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("user_view_"))
async def cb_user_view(cb: CallbackQuery):
    c_id = cb.data.split("user_view_")[1]
    item = await db_read("SELECT * FROM content WHERE id=?", (int(c_id),), one=True)
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
            await bot.send_photo(chat_id, item["file_id"], caption=cap)
        elif t == "doc":
            await bot.send_document(chat_id, item["file_id"], caption=cap)
        elif t == "voice":
            await bot.send_voice(chat_id, item["file_id"], caption=cap)
        elif t == "video":
            await bot.send_video(chat_id, item["file_id"], caption=cap)
        elif t == "audio":
            await bot.send_audio(chat_id, item["file_id"], caption=cap)
        elif t == "video_note":
            await bot.send_video_note(chat_id, item["file_id"])
        else:
            await cb.message.answer("❌ نوع غير معروف")
    except TelegramBadRequest as e:
        logger.error(f"send_content: {e}")
        await cb.message.answer("❌ الملف غير متاح، تواصل مع المسؤول.")
    except Exception as e:
        logger.error(f"send_content: {e}")
        await cb.message.answer("❌ خطأ أثناء الإرسال.")

@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()

# ─── الإذاعة ──────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_bc")
async def cb_bc(cb: CallbackQuery, state: FSMContext):
    if not await is_admin(cb.from_user.id):
        return await cb.answer("لا صلاحية.", show_alert=True)
    await cb.message.answer("📣 أرسل رسالة الإذاعة:")
    await state.set_state(St.broadcast)

@dp.message(St.broadcast)
async def process_broadcast(msg: Message, state: FSMContext):
    await state.clear()
    users = await db_read("SELECT user_id FROM users WHERE is_active=1")
    if not users:
        return await msg.answer("⚠️ لا يوجد مستخدمون.")
    total   = len(users)
    sm      = await msg.answer(f"📣 جاري الإذاعة لـ <b>{total}</b>...")
    success, failed, blocked = 0, 0, []

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
        await db_write("DELETE FROM users WHERE user_id=?", (uid,))

    try:
        await sm.edit_text(
            f"✅ <b>انتهت الإذاعة:</b>\n"
            f"📤 نجح: <b>{success}</b>\n❌ فشل: <b>{failed}</b>\n🚫 محظور: <b>{len(blocked)}</b>"
        )
    except:
        pass

# ─── الاشتراك الإجباري ────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_force")
async def cb_force(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    chans = await db_read("SELECT * FROM channels")
    b = InlineKeyboardBuilder()
    for c in chans or []:
        b.row(InlineKeyboardButton(text=f"❌ {c['username'] or c['id']}", callback_data=f"del_chan_{c['id']}"))
    b.row(InlineKeyboardButton(text="➕ إضافة قناة", callback_data="add_chan"))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    try:
        await cb.message.edit_text(f"📢 القنوات: <b>{len(chans or [])}</b>", reply_markup=b.as_markup())
    except:
        await cb.message.answer(f"📢 القنوات: <b>{len(chans or [])}</b>", reply_markup=b.as_markup())

@dp.callback_query(F.data == "add_chan")
async def cb_add_chan(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "📢 أرسل رابط القناة أو اليوزرنيم:\n"
        "• <code>https://t.me/channel</code>\n• <code>@channel</code>\n\n"
        "⚠️ البوت يجب أن يكون مشرفاً في القناة!"
    )
    await state.set_state(St.add_chan)

async def get_chat_id_direct(username: str):
    url = f"https://api.telegram.org/bot{TOKEN}/getChat"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data={"chat_id": f"@{username}"}) as resp:
            res = await resp.json()
            if res.get("ok"):
                return res["result"]["id"], res["result"].get("title", username)
            return None, res.get("description", "Unknown error")

@dp.message(St.add_chan)
async def process_add_chan(msg: Message, state: FSMContext):
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
        me     = await bot.get_me()
        member = await bot.get_chat_member(chat_id=chat_id_val, user_id=me.id)
        if member.status not in ("administrator", "creator"):
            try:
                await wm.delete()
            except:
                pass
            return await msg.answer(f"❌ البوت ليس مشرفاً في @{username}.")
        await db_write(
            "INSERT OR REPLACE INTO channels (id, url, username) VALUES (?,?,?)",
            (str(chat_id_val), f"https://t.me/{username}", f"@{username}"),
        )
        cache_del("channels_list")
        try:
            await wm.delete()
        except:
            pass
        await msg.answer(f"✅ تمت الإضافة!\n📢 {chat_title}\n🆔 <code>{chat_id_val}</code>")
    except Exception as e:
        try:
            await wm.delete()
        except:
            pass
        await msg.answer(f"❌ خطأ: <code>{str(e)[:200]}</code>")

@dp.callback_query(F.data.startswith("del_chan_"))
async def cb_del_chan(cb: CallbackQuery):
    c_id = cb.data[len("del_chan_"):]
    await db_write("DELETE FROM channels WHERE id=?", (c_id,))
    cache_del("channels_list")
    await cb.answer("✅ تم حذف القناة")
    await cb_force(cb)

# ─── التنبيهات ────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_notify")
async def cb_notify(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    sub = await db_read("SELECT value FROM settings WHERE key='sub_notify'",   one=True)
    ent = await db_read("SELECT value FROM settings WHERE key='entry_notify'", one=True)
    sv  = sub["value"] if sub else "OFF"
    ev  = ent["value"] if ent else "OFF"
    b = InlineKeyboardBuilder()
    b.row(InlineKeyboardButton(
        text=f"{'🟢' if sv=='ON' else '🔴'} اشتراك القنوات: {sv}",
        callback_data="toggle_sub_notify"
    ))
    b.row(InlineKeyboardButton(
        text=f"{'🟢' if ev=='ON' else '🔴'} دخول المستخدمين: {ev}",
        callback_data="toggle_entry_notify"
    ))
    b.row(InlineKeyboardButton(text="🔙 عودة", callback_data="adm_back"))
    try:
        await cb.message.edit_text("🔔 <b>إعدادات التنبيهات:</b>", reply_markup=b.as_markup())
    except:
        await cb.message.answer("🔔 <b>إعدادات التنبيهات:</b>", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("toggle_"))
async def cb_toggle(cb: CallbackQuery):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    key = "sub_notify" if "sub" in cb.data else "entry_notify"
    res = await db_read("SELECT value FROM settings WHERE key=?", (key,), one=True)
    new_val = "OFF" if res and res["value"] == "ON" else "ON"
    await db_write("UPDATE settings SET value=? WHERE key=?", (new_val, key))
    await cb.answer(f"✅ تم التغيير إلى {new_val}")
    await cb_notify(cb)

# ─── تعديل الشرح ──────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "adm_help")
async def cb_adm_help(cb: CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMIN_IDS:
        return await cb.answer("لا صلاحية.", show_alert=True)
    res = await db_read("SELECT value FROM settings WHERE key='help_text'", one=True)
    await cb.message.answer(
        f"📝 النص الحالي:\n<i>{res['value'] if res else '—'}</i>\n\nأرسل النص الجديد:"
    )
    await state.set_state(St.edit_help)

@dp.message(St.edit_help)
async def process_edit_help(msg: Message, state: FSMContext):
    await state.clear()
    new_text = msg.text.strip()
    if not new_text:
        return await msg.answer("❌ النص فارغ.")
    await db_write("UPDATE settings SET value=? WHERE key=?", (new_text, "help_text"))
    await msg.answer("✅ تم تحديث نص الشرح.")

# ─── أزرار إضافية ────────────────────────────────────────────────────────────
@dp.message(F.text == "📢 قناة البوت")
async def show_bot_channel(msg: Message):
    chans = await db_read("SELECT url, username FROM channels LIMIT 1")
    if chans:
        c = chans[0]
        b = InlineKeyboardBuilder()
        b.row(InlineKeyboardButton(text="📢 انضم للقناة", url=c["url"]))
        await msg.answer("📢 قناة البوت الرسمية:", reply_markup=b.as_markup())
    else:
        await msg.answer("لا توجد قناة مضافة حالياً.")

@dp.message(F.text == "ℹ️ شرح البوت")
async def show_help_text(msg: Message):
    res = await db_read("SELECT value FROM settings WHERE key='help_text'", one=True)
    await msg.answer(res["value"] if res else "لا يوجد شرح.")

@dp.message(F.text == "🔬 السادس علمي")
async def show_sixth_science(msg: Message):
    if not await check_subscription(msg.from_user.id):
        return await msg.answer("⚠️ اشترك أولاً:", reply_markup=await get_sub_kb())
    secs = await db_read("SELECT * FROM sections WHERE parent_id IS NULL")
    b    = InlineKeyboardBuilder()
    icons = {"الملازم":"🗂","التحفيز":"💡","ارشادات":"📝","الملخصات":"📚"}
    for s in secs or []:
        icon = next((v for k, v in icons.items() if k in s["name"]), "📁")
        b.row(InlineKeyboardButton(text=f"{icon} {s['name']}", callback_data=f"user_sec_{s['id']}"))
    await msg.answer("🔬 <b>السادس العلمي - اختر القسم:</b>", reply_markup=b.as_markup())

# ─── التشغيل الرئيسي ─────────────────────────────────────────────────────────
async def main():
    # 1) تهيئة قاعدة البيانات المحلية
    init_db()

    # 2) أوامر البوت
    await setup_commands()

    # 3) حذف webhook قديم
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.warning("✅ تم حذف الـ webhook")
    except Exception as e:
        logger.warning(f"delete_webhook: {e}")

    # 4) health server
    await start_health_server()

    # 5) تنظيف المحظورين
    asyncio.create_task(cleanup_blocked())

    logger.warning("🚀 البوت يعمل...")

    # 6) polling
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
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
