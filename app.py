# ==========================================
# app.py — Instant VC Join + Instant React/View v5
# ==========================================
import builtins as _builtins

def _no_input(prompt=""):
    raise RuntimeError(f"Pyrogram prompt blocked: {prompt!r} — BAD SESSION in DB")

_builtins.input = _no_input

import os, sys, asyncio, logging, random, uuid, time, zipfile, json, glob, shutil, sqlite3, struct, base64, wave
from datetime import datetime

# ==========================================
# 🔐 LOAD .env  (must be before any os.getenv)
# ==========================================
try:
    from dotenv import load_dotenv
    _ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_ENV_PATH):
        load_dotenv(_ENV_PATH, override=True)
    else:
        load_dotenv(override=True)
except ImportError:
    _ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_ENV_PATH):
        with open(_ENV_PATH) as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _v = _line.split("=", 1)
                _k = _k.strip()
                _v = _v.strip().strip('"').strip("'")
                if _k and _k not in os.environ:
                    os.environ[_k] = _v

import pyrogram
import pyrogram.errors
import pyrogram.utils
import pyrogram.client
from pyrogram.raw.types import InputPeerChannel, InputPeerChat, InputPeerUser

import pyrogram.raw.functions.phone as _phone_ns
_FALLBACK_RAWS = [
    "CreateConferenceCall", "JoinGroupCall", "LeaveGroupCall", "DiscardGroupCall",
    "EditGroupCallTitle", "GetGroupCall", "GetGroupCallParticipants",
    "InviteToGroupCall", "ToggleGroupCallSettings", "SendGroupCallBroadcast",
]
for _name in _FALLBACK_RAWS:
    if not hasattr(_phone_ns, _name):
        class _Dummy:
            ID = 0
            def __init__(self, *a, **k): pass
            async def resolve(self, *a, **k): return b""
            @staticmethod
            def read(b): return None
            def write(self): return b""
        _Dummy.__name__ = _name
        setattr(_phone_ns, _name, _Dummy)

class DummyGroupcallError(pyrogram.errors.BadRequest):
    pass

for _err in ['GroupcallForbidden','GroupCallForbidden','GroupcallInvalid','GroupCallInvalid','GroupcallNotFound','GroupCallNotFound']:
    if not hasattr(pyrogram.errors, _err):
        setattr(pyrogram.errors, _err, DummyGroupcallError)

try:
    import pyrogram.errors.exceptions.forbidden_403 as _f403
    for _err in ['GroupcallForbidden','GroupCallForbidden']:
        if not hasattr(_f403, _err): setattr(_f403, _err, DummyGroupcallError)
except ImportError: pass

try:
    import pyrogram.errors.exceptions.bad_request_400 as _b400
    for _err in ['GroupcallInvalid','GroupCallInvalid','GroupcallNotFound','GroupCallNotFound']:
        if not hasattr(_b400, _err): setattr(_b400, _err, DummyGroupcallError)
except ImportError: pass

_orig_gpt = pyrogram.utils.get_peer_type
def _patched_gpt(peer_id):
    try: return _orig_gpt(peer_id)
    except ValueError:
        p = str(peer_id)
        if p.startswith("-100"): return "channel"
        if p.startswith("-"):    return "chat"
        return "user"
pyrogram.utils.get_peer_type = _patched_gpt

_orig_rp = pyrogram.client.Client.resolve_peer
async def _patched_rp(self, peer_id):
    try: return await _orig_rp(self, peer_id)
    except Exception:
        p = str(peer_id)
        if p.startswith("-100"): return InputPeerChannel(channel_id=int(p[4:]), access_hash=0)
        if p.startswith("-"):    return InputPeerChat(chat_id=int(p[1:]))
        return InputPeerUser(user_id=int(peer_id), access_hash=0)
pyrogram.client.Client.resolve_peer = _patched_rp

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError: pass
try:
    import tgcrypto  # noqa
except ImportError: pass

from aiohttp import web
from pyrogram import Client, filters, idle, enums
from pyrogram.errors import (
    SessionPasswordNeeded, FloodWait, UserAlreadyParticipant,
    UsernameInvalid, UsernameOccupied, PeerIdInvalid, AuthKeyUnregistered,
    PasswordHashInvalid, UserDeactivated, UserDeactivatedBan,
    PasswordRecoveryNa
)
from pyrogram.enums import ChatMemberStatus, ChatType
from pyrogram.types import (Message, ReplyKeyboardMarkup, KeyboardButton,
                            InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery)
from pyrogram.raw import functions
from pyrogram.raw.functions.messages import GetMessagesViews, SendReaction
from pyrogram.raw.types import ReactionEmoji
from pyrogram.handlers import RawUpdateHandler
from pyrogram.raw.types import (
    UpdateGroupCall, UpdateGroupCallConnection, UpdateGroupCallParticipants,
    UpdateChannel, UpdateNewChannelMessage, UpdateNewMessage,
)
from motor.motor_asyncio import AsyncIOMotorClient

PTGCALLS_AVAILABLE = False
PyTgCalls = None
MediaStream = None
try:
    from pytgcalls import PyTgCalls  # noqa
    from pytgcalls.types import MediaStream  # noqa
    PTGCALLS_AVAILABLE = True
except Exception as _e:
    logging.getLogger(__name__).warning(f"PyTgCalls unavailable: {_e}")


# ==========================================
# ⚙️ CONFIG
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

API_ID    = int(os.getenv("API_ID", "123456").strip() or "123456")
API_HASH  = os.getenv("API_HASH", "your_api_hash").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "your_bot_token").strip()
MONGO_URI = os.getenv("MONGO_URI", os.getenv("MONGO_URL", "")).strip()
OWNER_ID  = int(os.getenv("OWNER_ID", "123456789").strip() or "123456789")

# ==========================================
# 🗄️ NEW DATABASE NAME
# ==========================================
DB_NAME = os.getenv("DB_NAME", "smm_manager").strip()

DEFAULT_CHANNELS_ENV = os.getenv("DEFAULT_CHANNELS", "").strip()
DEFAULT_REACT_EMOJI = os.getenv("DEFAULT_REACT_EMOJI", "👍").strip()

# Poll timings (fast backup poller)
CHANNEL_POST_POLL = int(os.getenv("CHANNEL_POST_POLL_SECONDS", "10"))
VC_LIVESTREAM_POLL = int(os.getenv("VC_LIVESTREAM_POLL_SECONDS", "5"))  # faster backup
VC_JOIN_DELAY_SECONDS = int(os.getenv("VC_JOIN_DELAY_SECONDS", "1"))    # near-instant joins

POSITIVE_REACTIONS = ["👍","❤️","🔥","🥰","👏","😁","🎉","🤩","🙏","💯","❤️‍🔥","😍","👌","🕊"]


# ==========================================
# 🎵 SILENT WAV
# ==========================================
SILENT_FILE = "silent.wav"
def _ensure_silent_audio():
    if os.path.exists(SILENT_FILE) and os.path.getsize(SILENT_FILE) > 1000: return
    logger.info("Generating silent.wav...")
    try:
        with wave.open(SILENT_FILE, "wb") as w:
            w.setnchannels(1); w.setsampwidth(2); w.setframerate(44100)
            w.writeframes(b"\x00\x00" * 44100 * 10)
        logger.info("✅ silent.wav created.")
    except Exception as e:
        logger.error(f"silent.wav failed: {e}")
_ensure_silent_audio()


# ==========================================
# 🗄️ DB
# ==========================================
db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000) if MONGO_URI else None
if db_client:
    db               = db_client[DB_NAME]
    accounts_col     = db["sessions"]
    admin_col        = db["admins"]
    pseudo_col       = db["pseudo_owners"]
    users_col        = db["users"]
    settings_col     = db["settings"]
    helper_col       = db["vc_helpers"]
    defaultchan_col  = db["default_channels"]
    chan_seen_col    = db["channel_seen"]
    vc_state_col     = db["vc_state"]
    logger.info(f"🗄️ Using database: {DB_NAME}")
else:
    logger.critical("❌ MONGO_URI not set!")
    sys.exit(1)

async def db_call(coro_func, *args, default=None, timeout=8, **kwargs):
    try: return await asyncio.wait_for(coro_func(*args, **kwargs), timeout=timeout)
    except asyncio.TimeoutError: return default
    except Exception as e:
        logger.error(f"DB error: {e}")
        return default

bot = Client("manager_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# ==========================================
# 🔄 GLOBAL STATE
# ==========================================
user_data = {}
DEAD_ACCOUNTS = {}
active_calls = {}
vc_queue = asyncio.Queue()
kill_event = asyncio.Event()
vc_stop_event = asyncio.Event()
worker_started = False
channel_watcher_started = False
vc_watcher_started = False
multi_vc_registry = {}
raw_listener_clients = {}   # phone -> running Client with RawUpdateHandler
raw_listener_lock = asyncio.Lock()
_watched_chat_ids = set()   # cache of default channel ids to filter updates
_reaction_locks = {}


# ==========================================
# ⚙️ HELPERS
# ==========================================
def _is_valid_session(s):
    if not s or not isinstance(s, str): return False
    s = s.strip()
    if len(s) < 300 or len(s) > 5000: return False
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_=")
    if not all(c in allowed for c in s): return False
    try:
        pad = (-len(s)) % 4
        raw = base64.urlsafe_b64decode(s + ("=" * pad))
        return len(raw) >= 200
    except Exception: return False

def make_session_client(session_str, name_prefix="x", **kwargs):
    return Client(f"{name_prefix}_{uuid.uuid4().hex[:6]}",
                  session_string=session_str.strip(),
                  api_id=API_ID, api_hash=API_HASH,
                  in_memory=True, **kwargs)

async def is_pseudo_owner(user_id):
    return bool(await db_call(pseudo_col.find_one, {"user_id": user_id}, default=None))

async def is_admin(user_id):
    if user_id == OWNER_ID: return True
    if await is_pseudo_owner(user_id): return True
    return bool(await db_call(admin_col.find_one, {"user_id": user_id}, default=None))

async def check_access(user_id):
    await db_call(users_col.update_one, {"user_id": user_id},
                  {"$setOnInsert": {"user_id": user_id, "joined_at": datetime.utcnow()}}, upsert=True)
    return True

def get_display_phone(phone, added_by, uid):
    if uid == OWNER_ID or added_by == uid: return phone
    return f"+*******{phone[-4:]}" if phone and len(phone) >= 4 else "+*******"

def main_menu(user_id, admin_status, pseudo_status=False, is_owner=False):
    buttons = [
        [KeyboardButton("📱 Add Account"), KeyboardButton("⚙️ Manage Accounts")],
        [KeyboardButton("📦 Mass Import (ZIP/File)"), KeyboardButton("📊 Stats")],
        [KeyboardButton("🏓 Ping")]
    ]
    if admin_status:
        buttons.append([KeyboardButton("🚀 Engagement Actions"), KeyboardButton("📢 Broadcast")])
        buttons.append([KeyboardButton("📺 Auto Channels"), KeyboardButton("⚙️ Bot Settings")])
    if is_owner or pseudo_status:
        buttons.append([KeyboardButton("🔑 Owner Panel")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def parse_telegram_link(link):
    link = link.strip().rstrip("/")
    if "t.me/c/" in link:
        parts = link.split("t.me/c/")[-1].split("/")
        return int(f"-100{parts[0]}"), int(parts[1].split("?")[0]) if len(parts) > 1 else None
    if "t.me/" in link:
        parts = link.split("t.me/")[-1].split("/")
        if parts[0] == "joinchat" or "+" in link: return link, None
        cid = int(parts[0]) if parts[0].lstrip("-").isdigit() else "@" + parts[0].lstrip("@")
        mid = None
        if len(parts) > 1:
            try: mid = int(parts[1].split("?")[0])
            except Exception: pass
        return cid, mid
    if link.lstrip("-").isdigit(): return int(link), None
    if not link.startswith("@") and "http" not in link and "+" not in link: return "@" + link, None
    return link, None

def extract_username_from_link(link: str):
    if not link: return link
    l = link.strip().rstrip("/")
    if "t.me/" in l:
        part = l.split("t.me/")[-1]
        if part.startswith("+") or part.startswith("joinchat/"): return l
        username = part.split("/")[0].split("?")[0]
        return "@" + username.lstrip("@")
    if l.startswith("@"): return l
    if l.lstrip("-").isdigit(): return l
    return l

def extract_string_session(db_path, api_id):
    try:
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(sessions)")
            cols = [x[1] for x in c.fetchall()]
            if "auth_key" in cols and "dc_id" in cols:
                c.execute("SELECT dc_id, auth_key FROM sessions LIMIT 1")
                row = c.fetchone()
                if row and row[0] and row[1] and len(row[1]) == 256:
                    dc, key = row
                    if int(pyrogram.__version__.split(".")[0]) >= 2:
                        packed = struct.pack(">BI?256sq?", dc, api_id, False, key, 0, False)
                    else:
                        packed = struct.pack(">B?256sI?", dc, False, key, 0, False)
                    return base64.urlsafe_b64encode(packed).decode().rstrip("=")
    except Exception as e:
        logger.error(f"SQLite: {e}")
    return None

async def get_chat_safe(client, target):
    try: return await client.get_chat(target)
    except Exception: pass
    try:
        if isinstance(target, str) and ("+" in target or "joinchat" in target or not target.lstrip("-").isdigit()):
            await client.join_chat(target)
            return await client.get_chat(target)
    except UserAlreadyParticipant:
        try: return await client.get_chat(target)
        except Exception: return None
    except Exception: return None
    return None

async def get_positive_reactions():
    cfg = await db_call(settings_col.find_one, {"id": "positive_reactions"}, default=None)
    if cfg and isinstance(cfg.get("emoji_list"), list) and cfg["emoji_list"]:
        return cfg["emoji_list"]
    return POSITIVE_REACTIONS


# ==========================================
# 📺 DEFAULT CHANNELS (multiple)
# ==========================================
async def get_default_channels() -> list:
    chans = await db_call(defaultchan_col.find({}).to_list, length=50, default=[]) or []
    links = [c.get("link") for c in chans if c.get("link")]
    if not links and DEFAULT_CHANNELS_ENV:
        for l in DEFAULT_CHANNELS_ENV.split(","):
            l = l.strip()
            if l:
                links.append(l)
                await db_call(defaultchan_col.update_one,
                    {"link": l}, {"$set": {"link": l, "added_by": OWNER_ID,
                                           "added_at": datetime.utcnow()}}, upsert=True)
    return links


async def refresh_watched_chat_ids():
    """Build a set of chat_ids for our default channels — used to filter raw updates."""
    global _watched_chat_ids
    new_set = set()
    channels = await get_default_channels()
    accs = await db_call(accounts_col.find({}).to_list, length=1, default=[]) or []
    if not accs or not _is_valid_session(accs[0].get("session")):
        return
    try:
        async with make_session_client(accs[0]["session"], "chid", no_updates=True) as app:
            for ch in channels:
                chat = await get_chat_safe(app, ch)
                if chat:
                    new_set.add(chat.id)
    except Exception as e:
        logger.debug(f"refresh_watched_chat_ids err: {e}")
    _watched_chat_ids = new_set
    logger.info(f"👁 Watched channel ids: {_watched_chat_ids}")


async def add_default_channel(link: str, uid: int):
    await db_call(defaultchan_col.update_one,
        {"link": link},
        {"$set": {"link": link, "added_by": uid, "added_at": datetime.utcnow()}},
        upsert=True)
    await refresh_watched_chat_ids()


async def remove_default_channel(link: str):
    await db_call(defaultchan_col.delete_one, {"link": link})
    await refresh_watched_chat_ids()


# ==========================================
# 🔁 REACTION / VIEW RETRY HELPERS
# ==========================================
def _reaction_lock(chat_id, msg_id):
    key = f"{chat_id}:{msg_id}"
    if key not in _reaction_locks:
        _reaction_locks[key] = asyncio.Lock()
    return _reaction_locks[key]

async def _safe_send_reaction_inner(app, peer, msg_id, emoji, max_retries=4):
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            await app.invoke(SendReaction(
                peer=peer, msg_id=msg_id,
                reaction=[ReactionEmoji(emoticon=emoji)]
            ))
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 3)
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("INTERDC_X_CALL_ERROR","INTERDC","500","TimedOut","Network","call failed","connection")):
                await asyncio.sleep(delay)
                delay *= 1.7
                continue
            return False
    return False

async def safe_send_reaction(app, peer, msg_id, emoji, max_retries=4, chat_id=None):
    lock = _reaction_lock(chat_id, msg_id) if chat_id is not None else None
    if lock:
        async with lock:
            return await _safe_send_reaction_inner(app, peer, msg_id, emoji, max_retries)
    return await _safe_send_reaction_inner(app, peer, msg_id, emoji, max_retries)

async def safe_view_message(app, peer, msg_id, max_retries=4):
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            await app.invoke(GetMessagesViews(peer=peer, id=[msg_id], increment=True))
            return True
        except FloodWait as fw:
            await asyncio.sleep(fw.value + 3)
        except Exception as e:
            err = str(e)
            if any(k in err for k in ("INTERDC_X_CALL_ERROR","INTERDC","500","TimedOut","Network","call failed","connection")):
                await asyncio.sleep(delay)
                delay *= 1.7
                continue
            return False
    return False


# ==========================================
# 🌐 WEB SERVER & AUTO-ONLINE
# ==========================================
async def dummy_web_server():
    async def h(r): return web.Response(text="Bot is running perfectly!", status=200)
    app = web.Application()
    app.router.add_get("/", h)
    app.router.add_get("/health", h)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", 7860).start()

async def auto_online_task():
    await asyncio.sleep(60)
    while True:
        try:
            accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
            for acc in accs:
                session = (acc.get("session") or "").strip()
                if not _is_valid_session(session): continue
                try:
                    async with make_session_client(session, "ka", no_updates=True) as app:
                        await app.invoke(functions.account.UpdateStatus(offline=False))
                except Exception: pass
                await asyncio.sleep(3)
        except Exception: pass
        await asyncio.sleep(518400)


# ==========================================
# 🔥 RAW UPDATE LISTENERS (INSTANT VC + POST)
# ==========================================
async def _on_raw_update(client: Client, update, users, chats):
    """
    Fires the moment Telegram delivers any raw update to a running user client.
    We filter for:
      - UpdateGroupCall / UpdateGroupCallParticipants → livestream started in a watched channel
      - UpdateNewChannelMessage / UpdateNewMessage → new post in a watched channel
    """
    try:
        # ---------- VC START EVENT ----------
        if isinstance(update, (UpdateGroupCall, UpdateGroupCallParticipants)):
            gc = getattr(update, "call", None) or getattr(update, "group_call", None)
            if not gc: return
            chat_id = None
            # UpdateGroupCall has peer; UpdateGroupCallParticipants has call + participants
            peer = getattr(update, "peer", None) or getattr(gc, "peer", None)
            if peer is not None:
                try:
                    chat_id = pyrogram.utils.get_peer_id(peer)
                except Exception:
                    chat_id = None
            if chat_id is None:
                chat_id = getattr(gc, "chat_id", None) or getattr(gc, "channel_id", None)
            if chat_id is None:
                return
            # normalize -100XXX form
            if isinstance(chat_id, int) and chat_id > 0:
                chat_id = int(f"-100{chat_id}")
            if _watched_chat_ids and chat_id not in _watched_chat_ids:
                return
            logger.info(f"🔥 RAW VC START detected on chat {chat_id}")
            # find which link this chat belongs to
            channels = await get_default_channels()
            for ch in channels:
                try:
                    async with make_session_client(
                        (await db_call(accounts_col.find({}).to_list, length=1, default=[]) or [{}])[0].get("session",""),
                        "vcmap", no_updates=True
                    ) as probe:
                        c = await get_chat_safe(probe, ch)
                        if c and c.id == chat_id:
                            asyncio.create_task(_auto_join_vc_for_channel(ch, force=True))
                            break
                except Exception:
                    continue
            return

        # ---------- NEW POST EVENT ----------
        if isinstance(update, (UpdateNewChannelMessage, UpdateNewMessage)):
            msg = getattr(update, "message", None)
            if not msg: return
            peer = getattr(msg, "peer_id", None)
            chat_id = None
            if peer is not None:
                try:
                    chat_id = pyrogram.utils.get_peer_id(peer)
                except Exception:
                    chat_id = None
            if chat_id is None:
                return
            if isinstance(chat_id, int) and chat_id > 0:
                chat_id = int(f"-100{chat_id}")
            if _watched_chat_ids and chat_id not in _watched_chat_ids:
                return
            mid = getattr(msg, "id", None)
            if not mid:
                return
            logger.info(f"🔥 RAW NEW POST detected on chat {chat_id} msg {mid}")
            # find channel link for this chat_id
            channels = await get_default_channels()
            for ch in channels:
                try:
                    accs = await db_call(accounts_col.find({}).to_list, length=1, default=[]) or []
                    if not accs: continue
                    async with make_session_client(accs[0]["session"], "pmap", no_updates=True) as probe:
                        c = await get_chat_safe(probe, ch)
                        if c and c.id == chat_id:
                            asyncio.create_task(_process_single_post(ch, chat_id, mid))
                            break
                except Exception:
                    continue
            return
    except Exception as e:
        logger.debug(f"raw update handler err: {e}")


async def _start_raw_listeners():
    """
    For every saved account, spin up a persistent client with a RawUpdateHandler
    so we get *instant* notifications of new posts and VC starts.
    """
    async with raw_listener_lock:
        accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
        accs = [a for a in accs if _is_valid_session(a.get("session"))]
        want_phones = {a.get("phone") for a in accs}

        # stop listeners no longer wanted
        for phone in list(raw_listener_clients.keys()):
            if phone not in want_phones:
                try:
                    await raw_listener_clients[phone].stop()
                except Exception: pass
                raw_listener_clients.pop(phone, None)

        # start listeners for new accounts
        for acc in accs:
            phone = acc.get("phone")
            if not phone or phone in raw_listener_clients:
                continue
            try:
                cli = Client(
                    f"listener_{phone.replace('+','')}_{uuid.uuid4().hex[:4]}",
                    api_id=API_ID, api_hash=API_HASH,
                    session_string=acc["session"],
                    in_memory=True,
                )
                cli.add_handler(RawUpdateHandler(_on_raw_update))
                await cli.start()
                raw_listener_clients[phone] = cli
                logger.info(f"👂 Raw listener started for {phone}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"listener start fail {phone}: {e}")


async def raw_listener_supervisor():
    await asyncio.sleep(30)
    while True:
        try:
            await _start_raw_listeners()
        except Exception as e:
            logger.error(f"raw listener supervisor err: {e}")
        await asyncio.sleep(300)   # rescan for new accounts every 5 min


# ==========================================
# 📺 INSTANT CHANNEL WATCHER (backup + bulk)
# ==========================================
async def _fetch_recent_ids(app, chat_id, limit=10):
    ids = []
    try:
        async for m in app.get_chat_history(chat_id, limit=limit):
            if m.id:
                ids.append(m.id)
    except Exception:
        pass
    return ids


async def _process_single_post(ch_link: str, chat_id: int, msg_id: int):
    """React + view a single post from all accounts (used by RAW handler)."""
    # skip if already seen
    seen = await db_call(chan_seen_col.find_one,
                         {"chat_id": chat_id, "msg_id": msg_id}, default=None)
    if seen:
        return
    await db_call(chan_seen_col.update_one,
        {"chat_id": chat_id, "msg_id": msg_id},
        {"$set": {"chat_id": chat_id, "msg_id": msg_id,
                  "seen_at": datetime.utcnow()}}, upsert=True)

    accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
    accs = [a for a in accs if _is_valid_session(a.get("session"))]
    if not accs: return
    reactions = await get_positive_reactions()
    ok_re = ok_vw = fail = 0

    for acc in accs:
        if kill_event.is_set(): break
        app = None
        try:
            app = make_session_client(acc["session"], "rpost", no_updates=True)
            await app.start()
            try:
                peer = await app.resolve_peer(chat_id)
            except Exception:
                try:
                    await app.join_chat(ch_link)
                except UserAlreadyParticipant: pass
                except Exception: pass
                chat = await get_chat_safe(app, ch_link)
                if not chat: continue
                peer = await app.resolve_peer(chat.id)
            emoji = random.choice(reactions)
            if await safe_send_reaction(app, peer, msg_id, emoji, chat_id=chat_id):
                ok_re += 1
            if await safe_view_message(app, peer, msg_id):
                ok_vw += 1
            await asyncio.sleep(random.uniform(0.3, 0.9))
        except Exception as e:
            logger.debug(f"single post err: {e}")
            fail += 1
        finally:
            try:
                if app: await app.stop()
            except Exception: pass
    logger.info(f"🔥 Post {chat_id}:{msg_id} → reactions={ok_re} views={ok_vw} fail={fail}")


async def _process_channel_new_posts(ch_link: str):
    """Backup poller — catches posts if raw listener missed them."""
    resolved = extract_username_from_link(ch_link)
    if not resolved: return

    accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
    accs = [a for a in accs if _is_valid_session(a.get("session"))]
    if not accs: return

    recent, cid = [], None
    try:
        async with make_session_client(accs[0]["session"], "cw", no_updates=True) as app:
            chat = await get_chat_safe(app, ch_link)
            if not chat: return
            cid = chat.id
            recent = await _fetch_recent_ids(app, cid, limit=10)
    except Exception as e:
        logger.debug(f"channel watcher fetch err {resolved}: {e}")
        return
    if not recent: return

    new_posts = []
    for mid in recent:
        seen = await db_call(chan_seen_col.find_one,
                             {"chat_id": cid, "msg_id": mid}, default=None)
        if not seen:
            new_posts.append(mid)
            await db_call(chan_seen_col.update_one,
                {"chat_id": cid, "msg_id": mid},
                {"$set": {"chat_id": cid, "msg_id": mid,
                          "seen_at": datetime.utcnow()}}, upsert=True)
    if not new_posts: return

    logger.info(f"📺 Channel {resolved}: {len(new_posts)} new post(s) → react+view")
    reactions = await get_positive_reactions()
    ok_re = ok_vw = fail = 0
    for acc in accs:
        if kill_event.is_set(): break
        app = None
        try:
            app = make_session_client(acc["session"], "cvw", no_updates=True)
            await app.start()
            try:
                peer = await app.resolve_peer(cid)
            except Exception:
                try:
                    await app.join_chat(ch_link)
                except UserAlreadyParticipant: pass
                except Exception: pass
                chat = await get_chat_safe(app, ch_link)
                if not chat: continue
                peer = await app.resolve_peer(chat.id)
            for mid in new_posts:
                emoji = random.choice(reactions)
                if await safe_send_reaction(app, peer, mid, emoji, chat_id=cid): ok_re += 1
                if await safe_view_message(app, peer, mid): ok_vw += 1
                await asyncio.sleep(random.uniform(0.4, 1.0))
        except Exception as e:
            logger.debug(f"channel watcher acct err: {e}")
            fail += 1
        finally:
            try:
                if app: await app.stop()
            except Exception: pass
    logger.info(f"📺 Channel {resolved} done: reactions={ok_re} views={ok_vw} fail={fail}")


async def channel_watcher_task():
    global channel_watcher_started
    if channel_watcher_started: return
    channel_watcher_started = True
    logger.info(f"📺 Channel backup watcher started — poll every {CHANNEL_POST_POLL}s")
    await asyncio.sleep(20)
    while True:
        try:
            await refresh_watched_chat_ids()
            channels = await get_default_channels()
            for ch in channels:
                try:
                    await _process_channel_new_posts(ch)
                except Exception as e:
                    logger.debug(f"channel watcher err {ch}: {e}")
                await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"channel watcher error: {e}")
        await asyncio.sleep(CHANNEL_POST_POLL)


async def _force_process_channel(ch_link: str):
    accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
    accs = [a for a in accs if _is_valid_session(a.get("session"))]
    if not accs: return
    try:
        async with make_session_client(accs[0]["session"], "cwf", no_updates=True) as app:
            chat = await get_chat_safe(app, ch_link)
            if not chat: return
            cid = chat.id
            latest = await _fetch_recent_ids(app, cid, limit=3)
    except Exception as e:
        logger.warning(f"force process err: {e}")
        return
    if not latest: return
    reactions = await get_positive_reactions()
    for acc in accs[:50]:
        if kill_event.is_set(): break
        app = None
        try:
            app = make_session_client(acc["session"], "cwf2", no_updates=True)
            await app.start()
            try:
                peer = await app.resolve_peer(cid)
            except Exception:
                continue
            for mid in latest:
                emoji = random.choice(reactions)
                await safe_send_reaction(app, peer, mid, emoji, chat_id=cid)
                await safe_view_message(app, peer, mid)
                await asyncio.sleep(random.uniform(0.8, 1.5))
        except Exception:
            pass
        finally:
            try:
                if app: await app.stop()
            except Exception: pass


# ==========================================
# 🎤 MULTI-VC JOIN (muted)
# ==========================================
async def _play_muted(call_py, chat_id, media_path):
    stream = MediaStream(media_path)
    played = False
    try:
        await call_py.play(chat_id, stream, muted=True)
        played = True
    except TypeError:
        pass
    except Exception as e:
        logger.debug(f"play(muted) failed: {e}")
    if not played:
        try:
            await call_py.play(chat_id, stream)
            try: await call_py.mute(chat_id)
            except Exception: pass
            played = True
        except Exception as e:
            logger.debug(f"play fallback failed: {e}")
    return played


async def multi_vc_join_all(uid: int, target_link: str, helpers: list,
                            media_path: str, silent: bool = True,
                            state_key: str = None):
    if not PTGCALLS_AVAILABLE:
        logger.warning("PyTgCalls unavailable, cannot join VC")
        return

    reg_key = f"state::{state_key}" if state_key else f"uid::{uid}"
    reg = multi_vc_registry.setdefault(reg_key, {"active": {}, "stop": asyncio.Event()})
    reg["stop"].clear()

    resolved = extract_username_from_link(target_link) if target_link else None

    sessions = []
    for ph in helpers:
        acc = await db_call(accounts_col.find_one, {"phone": ph}, default=None)
        if acc and _is_valid_session(acc.get("session")):
            sessions.append({"phone": ph, "session": acc["session"],
                             "name": acc.get("name", "?")})
    if not sessions:
        logger.warning("multi_vc_join_all: no valid helper sessions")
        return

    ok = fail = total_joins = 0
    joined_phones = []

    for i, item in enumerate(sessions):
        if reg["stop"].is_set(): break
        if any(k.startswith(f"{item['phone']}:") for k in reg["active"]):
            ok += 1
            joined_phones.append(item["phone"])
            continue

        app = None
        try:
            app = Client(
                f"mvc_{uuid.uuid4().hex[:6]}",
                api_id=API_ID, api_hash=API_HASH,
                session_string=item["session"],
                in_memory=True, no_updates=True,
            )
            await app.start()
            chat = None
            try:
                chat = await app.get_chat(resolved or target_link)
            except Exception:
                try:
                    await app.join_chat(target_link)
                    chat = await app.get_chat(resolved or target_link)
                except UserAlreadyParticipant:
                    chat = await app.get_chat(resolved or target_link)
                except Exception as e:
                    logger.warning(f"MVC target fail {item['phone']}: {e}")
                    chat = None
            if not chat:
                raise RuntimeError("no target chat")

            call_py = PyTgCalls(app)
            await call_py.start()
            played = await _play_muted(call_py, chat.id, media_path)
            key = f"{item['phone']}:{chat.id}"
            reg["active"][key] = {
                "client": app, "call": call_py, "chat_id": chat.id,
                "phone": item["phone"],
                "chat_title": getattr(chat, "title", str(chat.id)),
            }
            total_joins += 1
            ok += 1
            joined_phones.append(item["phone"])
            if not played:
                logger.debug(f"join-only (play failed) for {chat.id}")
            await asyncio.sleep(0.6)
        except Exception as e:
            logger.error(f"MVC join err ({item['phone']}): {e}")
            fail += 1
            try:
                if app: await app.stop()
            except Exception: pass
        if i < len(sessions) - 1:
            waited = 0
            while waited < VC_JOIN_DELAY_SECONDS:
                if reg["stop"].is_set(): break
                await asyncio.sleep(1); waited += 1
            if reg["stop"].is_set(): break

    if not reg["active"]:
        logger.info("MVC: no helpers joined")
        return

    if state_key:
        await db_call(vc_state_col.update_one, {"key": state_key},
            {"$set": {"key": state_key, "joined_phones": joined_phones,
                      "updated_at": datetime.utcnow()}}, upsert=True)

    logger.info(f"🎤 MVC running on {resolved}: joined={ok} fail={fail}")
    await reg["stop"].wait()

    for key, blk in list(reg["active"].items()):
        try: await blk["call"].leave_call(blk["chat_id"])
        except Exception: pass
        try: await blk["client"].stop()
        except Exception: pass
    reg["active"].clear()
    reg["stop"].clear()

    if state_key:
        await db_call(vc_state_col.update_one, {"key": state_key},
            {"$set": {"key": state_key, "joined_phones": [],
                      "updated_at": datetime.utcnow()}}, upsert=True)
    logger.info(f"🎤 MVC stopped on {resolved}")


# ==========================================
# 🎤 AUTO VC WATCHER
# ==========================================
async def _check_vc_active(app, chat) -> bool:
    try:
        cid = chat.id
        peer = await app.resolve_peer(cid)
        try:
            full = await app.invoke(functions.channels.GetFullChannel(channel=peer))
            if getattr(full.full_chat, "call", None):
                return True
        except Exception: pass
        try:
            full = await app.invoke(functions.messages.GetFullChat(chat_id=cid))
            if getattr(full.full_chat, "call", None):
                return True
        except Exception: pass
    except Exception as e:
        logger.debug(f"VC check err: {e}")
    return False


async def _auto_join_vc_for_channel(ch_link: str, force: bool = False):
    resolved = extract_username_from_link(ch_link)
    if not resolved: return

    accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
    accs = [a for a in accs if _is_valid_session(a.get("session"))]
    if not accs: return

    has_vc = False
    checked = False
    cid = None
    try:
        async with make_session_client(accs[0]["session"], "vcchk", no_updates=True) as app:
            chat = await get_chat_safe(app, ch_link)
            if not chat: return
            cid = chat.id
            has_vc = await _check_vc_active(app, chat)
            checked = True
    except Exception as e:
        logger.debug(f"VC check err {resolved}: {e}")
        return
    if not checked or cid is None: return

    state_key = f"autochan_{cid}"
    state = await db_call(vc_state_col.find_one, {"key": state_key}, default=None) or {}
    currently_joined = state.get("joined_phones", [])

    if has_vc and not currently_joined:
        logger.info(f"🎤 VC LIVE in {resolved} → joining ALL accounts (force={force})")
        asyncio.create_task(multi_vc_join_all(
            uid=OWNER_ID,
            target_link=ch_link,
            helpers=[a["phone"] for a in accs],
            media_path=SILENT_FILE,
            silent=True,
            state_key=state_key,
        ))
    elif not has_vc and currently_joined:
        logger.info(f"🎤 VC ended in {resolved} → releasing")
        reg = multi_vc_registry.get(f"state::{state_key}")
        if reg:
            reg["stop"].set()
            await db_call(vc_state_col.update_one, {"key": state_key},
                {"$set": {"key": state_key, "joined_phones": [],
                          "updated_at": datetime.utcnow()}}, upsert=True)


async def vc_watcher_task():
    global vc_watcher_started
    if vc_watcher_started: return
    vc_watcher_started = True
    logger.info(f"🎤 VC backup watcher started — poll every {VC_LIVESTREAM_POLL}s")
    await asyncio.sleep(15)
    while True:
        try:
            channels = await get_default_channels()
            for ch in channels:
                try:
                    await _auto_join_vc_for_channel(ch)
                except Exception as e:
                    logger.debug(f"vc watcher err {ch}: {e}")
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"vc watcher error: {e}")
        await asyncio.sleep(VC_LIVESTREAM_POLL)


# ==========================================
# 📩 PRIMARY MESSAGE HANDLER
# ==========================================
@bot.on_message(filters.private)
async def unified_private_handler(client, message: Message):
    uid = message.from_user.id
    text = (message.text or "").strip()
    try:
        if uid in user_data and user_data[uid].get("step") == "broadcast":
            user_data.pop(uid, None)
            s = await message.reply("📢 Broadcasting...")
            users_ = await db_call(users_col.find({}).to_list, length=None, default=[]) or []
            ok = fail = 0
            for u in users_:
                try:
                    await message.copy(u["user_id"]); ok += 1
                except Exception: fail += 1
                await asyncio.sleep(0.1)
            return await s.edit_text(f"✅ Broadcast complete.\nSent: `{ok}` | Failed: `{fail}`")

        if text.startswith("/start"):   return await cmd_start(client, message)
        if text.startswith("/ping"):    return await cmd_ping(client, message)
        if text.startswith("/stopvc"):  return await cmd_stopvc(client, message)
        if text.startswith("/leavevc"): return await cmd_leavevc(client, message)
        if text.startswith("/cleanrevoked"): return await cmd_cleanrevoked(client, message)
        if text.startswith("/reloadlisteners"): return await cmd_reload_listeners(client, message)

        await check_access(uid)
        admin_status = await is_admin(uid)
        pseudo_status = await is_pseudo_owner(uid)
        is_owner = (uid == OWNER_ID)

        if message.photo:    return await handle_photo_internal(client, message)
        if message.document: return await handle_document_internal(client, message)
        if message.audio or message.video or message.voice: return await handle_media_internal(client, message)
        if not text: return

        if text == "🏓 Ping": return await cmd_ping(client, message)

        if text == "📊 Stats":
            if admin_status:
                ac = await db_call(accounts_col.count_documents, {}, default=0)
                ch = await db_call(defaultchan_col.count_documents, {}, default=0)
                return await message.reply(
                    f"📊 **System Stats**\n📱 Accounts: `{ac}`\n📺 Default Channels: `{ch}`\n"
                    f"🗄️ DB: `{DB_NAME}`\n👂 Listeners: `{len(raw_listener_clients)}`"
                )
            ac = await db_call(accounts_col.count_documents, {"added_by": uid}, default=0)
            return await message.reply(f"📊 **Your Stats**\n📱 Your Accounts: `{ac}`")

        if text == "🚀 Engagement Actions":
            if not admin_status: return await message.reply("🚫 Admins only.")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⚡ Fast Mass Join", callback_data="eng_fastjoin"),
                 InlineKeyboardButton("🐢 Slow Mass Join", callback_data="eng_slowjoin")],
                [InlineKeyboardButton("👀 Add Views", callback_data="eng_views"),
                 InlineKeyboardButton("❤️ Add Reactions", callback_data="eng_reacts")],
                [InlineKeyboardButton("💬 Mass Comment", callback_data="eng_comment"),
                 InlineKeyboardButton("🚪 Mass Leave", callback_data="eng_leave")],
                [InlineKeyboardButton("🎤 Join Voice Chat", callback_data="eng_vcmenu")]
            ])
            return await message.reply("🚀 **Engagement Panel**", reply_markup=kb)

        if text == "📺 Auto Channels":
            if not admin_status: return await message.reply("🚫 Admins only.")
            return await show_auto_channels_menu(message, uid)

        if text == "🔑 Owner Panel" and (is_owner or pseudo_status):
            return await show_owner_panel(message, uid, is_owner, pseudo_status)

        if text == "⚙️ Bot Settings" and admin_status:
            return await message.reply("⚙️ **Bot Settings**",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("👋 Edit Welcome", callback_data="bot_edit_welcome")]]))

        if text == "📱 Add Account":
            user_data[uid] = {"step": "phone"}
            return await message.reply("📱 Send Phone Number (+919876543210):")

        if text == "📦 Mass Import (ZIP/File)":
            user_data[uid] = {"step": "import_zip"}
            return await message.reply("📦 Send a `.zip`, `.session`, `.json`, or `.txt` file.")

        if text == "⚙️ Manage Accounts":
            accs = await db_call(accounts_col.find({} if admin_status else {"added_by": uid}).to_list, length=50, default=[]) or []
            if not accs: return await message.reply("❌ No accounts saved.")
            kb = [[InlineKeyboardButton(
                f"{a.get('name', 'User')} | {get_display_phone(a.get('phone', ''), a.get('added_by'), uid)}",
                callback_data=f"manage_{a.get('phone', '')}")] for a in accs]
            return await message.reply("⚙️ **Select account:**", reply_markup=InlineKeyboardMarkup(kb))

        if text == "📢 Broadcast" and admin_status:
            user_data[uid] = {"step": "broadcast"}
            return await message.reply("📢 Send the message to broadcast:")

        if uid in user_data:
            return await handle_state_machine(client, message, uid, text)

    except Exception as e:
        logger.exception(f"unified_private_handler error: {e}")
        try: await message.reply(f"❌ Error: {e}")
        except Exception: pass


# ==========================================
# 🎯 STATE MACHINE
# ==========================================
async def handle_state_machine(client, message: Message, uid: int, text: str):
    state = user_data.get(uid, {})
    step = state.get("step")

    if step == "phone":
        try:
            temp = Client(f"temp_{uuid.uuid4().hex[:6]}", api_id=API_ID, api_hash=API_HASH,
                          in_memory=True, no_updates=True)
            await temp.connect()
            code_obj = await temp.send_code(text)
            user_data[uid] = {"step": "code", "phone": text,
                              "hash": code_obj.phone_code_hash, "client": temp}
            return await message.reply("📩 Code sent! Enter OTP:")
        except Exception as e:
            return await message.reply(f"❌ Error: {e}")

    if step == "code":
        try:
            await user_data[uid]["client"].sign_in(user_data[uid]["phone"],
                                                  user_data[uid]["hash"], text)
            return await finalize_login(message, uid)
        except SessionPasswordNeeded:
            user_data[uid]["step"] = "password"
            return await message.reply("🔐 2FA enabled. Enter password:")
        except Exception as e:
            try: await user_data[uid]["client"].disconnect()
            except Exception: pass
            user_data.pop(uid, None)
            return await message.reply(f"❌ Error: {e}")

    if step == "password":
        try:
            await user_data[uid]["client"].check_password(text)
            return await finalize_login(message, uid)
        except Exception as e:
            try: await user_data[uid]["client"].disconnect()
            except Exception: pass
            user_data.pop(uid, None)
            return await message.reply(f"❌ Error: {e}")

    if step == "set_bot_welcome_txt":
        await db_call(settings_col.update_one, {"id": "bot_start_msg"},
                      {"$set": {"text": text, "photo_id": state.get("bot_pic")}}, upsert=True)
        user_data.pop(uid, None)
        return await message.reply("✅ Welcome updated!")

    if step == "own_addadmin":
        try:
            await db_call(admin_col.update_one, {"user_id": int(text)},
                          {"$set": {"user_id": int(text)}}, upsert=True)
            user_data.pop(uid, None)
            return await message.reply(f"✅ `{text}` is now Admin.")
        except ValueError: return await message.reply("❌ Invalid User ID.")

    if step == "own_rmadmin":
        try:
            await db_call(admin_col.delete_one, {"user_id": int(text)})
            user_data.pop(uid, None)
            return await message.reply(f"✅ Admin `{text}` removed.")
        except ValueError: return await message.reply("❌ Invalid User ID.")

    if step == "own_addpseudo":
        try:
            await db_call(pseudo_col.update_one, {"user_id": int(text)},
                          {"$set": {"user_id": int(text)}}, upsert=True)
            user_data.pop(uid, None)
            return await message.reply(f"✅ `{text}` is now **Pseudo-Owner**.")
        except ValueError: return await message.reply("❌ Invalid User ID.")

    if step == "own_rmpseudo":
        try:
            await db_call(pseudo_col.delete_one, {"user_id": int(text)})
            user_data.pop(uid, None)
            return await message.reply(f"✅ Pseudo-Owner `{text}` removed.")
        except ValueError: return await message.reply("❌ Invalid User ID.")

    if step == "own_add_channel":
        link = text.strip()
        if "t.me/" not in link and not link.startswith("@"):
            return await message.reply("❌ Send a valid t.me link or @username.")
        await add_default_channel(link, uid)
        user_data.pop(uid, None)
        return await message.reply(f"✅ Default channel added: `{link}`")

    if step == "own_remove_channel":
        link = text.strip()
        await remove_default_channel(link)
        user_data.pop(uid, None)
        return await message.reply(f"✅ Default channel removed: `{link}`")

    if step == "own_default_reactions":
        parts = [x.strip() for x in text.replace(",", " ").split() if x.strip()]
        if not parts:
            return await message.reply("❌ Send at least one emoji.")
        await db_call(settings_col.update_one, {"id": "positive_reactions"},
                      {"$set": {"id": "positive_reactions", "emoji_list": parts,
                                "updated_by": uid, "updated_at": datetime.utcnow()}},
                      upsert=True)
        user_data.pop(uid, None)
        return await message.reply(f"✅ Reactions updated: `{' '.join(parts)}`")

    if step == "eng_target":
        user_data[uid]["target_link"] = text
        action = user_data[uid]["action"]
        if action in ("views", "reacts"):
            user_data[uid]["step"] = "eng_count"
            return await message.reply("🔢 How many accounts? (number):")
        elif action == "comment":
            user_data[uid]["step"] = "eng_comment_text"
            return await message.reply("💬 Send comment text:")
        elif action == "vc":
            user_data[uid]["step"] = "eng_vc_music"
            return await message.reply("🎵 Send stream URL / upload audio, or type `none` for silent:")
        else:
            data_copy = dict(user_data[uid]); user_data.pop(uid, None)
            await vc_queue.put({"message": message, "uid": uid, "data": data_copy})
            return await message.reply(f"✅ Task queued! Position: {vc_queue.qsize()}")

    if step == "eng_comment_text":
        user_data[uid]["comment_text"] = text
        user_data[uid]["step"] = "eng_count"
        return await message.reply("🔢 How many accounts? (number):")

    if step == "eng_vc_music":
        user_data[uid]["vc_media_url"] = None if text.lower() == "none" else text
        user_data[uid]["is_local_file"] = False
        user_data[uid]["step"] = "eng_vc_duration"
        return await message.reply("⏱ Send VC duration (minutes):")

    if step in ["eng_count", "eng_vc_duration"]:
        try:
            val = int(text)
            if step == "eng_vc_duration": user_data[uid]["duration"] = val
            else: user_data[uid]["count"] = val
            if step == "eng_vc_duration" and user_data[uid].get("vc_phone", "ALL") == "ALL":
                user_data[uid]["step"] = "eng_count"
                return await message.reply("🔢 How many accounts join VC? (number):")
            data_copy = dict(user_data[uid]); user_data.pop(uid, None)
            await vc_queue.put({"message": message, "uid": uid, "data": data_copy})
            return await message.reply(f"✅ Task queued! Position: {vc_queue.qsize()}")
        except ValueError:
            return await message.reply("❌ Send a number.")

    if step == "chan_name":
        user_data[uid]["chan_name"] = text
        user_data[uid]["step"] = "chan_dp"
        return await message.reply("🖼 Step 2: Send Photo (or Skip):",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_desc")]]))
    if step == "chan_desc":
        user_data[uid]["chan_desc"] = text
        user_data[uid]["step"] = "chan_user"
        return await message.reply("🔗 Step 4: Send Username (without @):")
    if step == "chan_user":
        user_data[uid]["chan_user"] = text
        return await finalize_channel_creation(message, uid)

    if step == "edit_chan_id":
        user_data[uid]["target_chan"] = text
        user_data[uid]["step"] = "edit_chan_val"
        return await message.reply("🖼 Send new photo:" if user_data[uid]["edit_action"] == "dp"
                                   else "📝 Send new description:")
    if step == "edit_chan_val": return await apply_channel_edit(message, uid)

    if step == "acc_set_bio":  return await apply_account_bio(message, uid, text)
    if step == "set_acc_name": return await execute_account_name_change(message, uid, text)
    if step == "set_2fa_pass": return await execute_2fa_action(message, uid, "set", new_pass=text)
    if step == "rm_2fa_pass":  return await execute_2fa_action(message, uid, "remove", old_pass=text)
    if step == "chg_2fa_old":
        user_data[uid]["old_pass"] = text
        user_data[uid]["step"] = "chg_2fa_new"
        return await message.reply("🔐 Send NEW 2FA Password:")
    if step == "chg_2fa_new":
        return await execute_2fa_action(message, uid, "change",
                                        old_pass=user_data[uid]["old_pass"], new_pass=text)
    if step == "chg_email_pass":
        user_data[uid]["current_pass"] = text
        user_data[uid]["step"] = "chg_email_new"
        return await message.reply("📧 Send NEW Recovery Email:")
    if step == "chg_email_new": return await execute_email_change(message, uid, text)


# ==========================================
# 🖼️ FILE / MEDIA HANDLERS
# ==========================================
async def handle_photo_internal(client, message: Message):
    uid = message.from_user.id
    if uid not in user_data: return
    step = user_data[uid].get("step")

    if step == "set_bot_welcome_pic":
        user_data[uid]["bot_pic"] = message.photo.file_id
        user_data[uid]["step"] = "set_bot_welcome_txt"
        return await message.reply("📝 Now send Text:")

    if step in ["acc_set_dp", "chan_dp", "edit_chan_val"]:
        s = await message.reply("⏳ Downloading photo...")
        target_path = f"dp_{uuid.uuid4().hex}.jpg"
        pp = await bot.download_media(message.photo.file_id, file_name=target_path)
        if not pp or not os.path.exists(pp) or os.path.getsize(pp) == 0:
            return await s.edit_text("❌ Empty image.")
        try:
            if step == "acc_set_dp":
                acc = await db_call(accounts_col.find_one,
                                    {"phone": user_data[uid]["target_phone"]}, default=None)
                async with make_session_client(acc["session"], "adp", no_updates=True) as app:
                    await app.set_profile_photo(photo=pp)
                await s.edit_text("✅ Account DP Updated.")
            elif step == "chan_dp":
                user_data[uid]["chan_dp_path"] = pp
                user_data[uid]["step"] = "chan_desc"
                return await s.edit_text("📝 Step 3: Send Description:")
            elif step == "edit_chan_val":
                acc = await db_call(accounts_col.find_one,
                                    {"phone": user_data[uid]["target_phone"]}, default=None)
                target = user_data[uid]["target_chan"]
                target = int(target) if str(target).lstrip("-").isdigit() else target
                async with make_session_client(acc["session"], "cdp", no_updates=True) as app:
                    await app.set_chat_photo(target, photo=pp)
                await s.edit_text("✅ Channel DP Updated.")
        except Exception as e:
            await s.edit_text(f"❌ Error: {e}")
        finally:
            if step != "chan_dp":
                try: os.remove(pp)
                except Exception: pass
                user_data.pop(uid, None)


async def handle_document_internal(client, message: Message):
    uid = message.from_user.id
    if uid not in user_data: return
    target_col = accounts_col
    fname = message.document.file_name or ""
    if not any(fname.lower().endswith(e) for e in (".zip", ".session", ".json", ".txt")): return

    s = await message.reply("⏳ Downloading & extracting...")
    fp = await message.download()
    edir = f"tmp_{uid}_{int(time.time())}"
    os.makedirs(edir, exist_ok=True)
    ok = fail = 0
    try:
        if fname.lower().endswith(".zip"):
            with zipfile.ZipFile(fp, "r") as z: z.extractall(edir)
        else:
            shutil.copy2(fp, os.path.join(edir, fname))

        for jf in glob.glob(f"{edir}/**/*.json", recursive=True):
            try:
                with open(jf) as f: d = json.load(f)
                ss = d.get("session_file", d.get("session_string", ""))
                cid_ = d.get("app_id", d.get("api_id", API_ID))
                chash = d.get("app_hash", d.get("api_hash", API_HASH))
                base = os.path.splitext(jf)[0]; sp = base + ".session"
                if not ss and os.path.exists(sp): ss = extract_string_session(sp, cid_)
                if _is_valid_session(ss):
                    async with Client(f"j_{uuid.uuid4().hex[:8]}", session_string=ss,
                                      api_id=cid_, api_hash=chash,
                                      in_memory=True, no_updates=True) as app:
                        me = await app.get_me()
                        await db_call(target_col.update_one, {"phone": me.phone_number},
                            {"$set": {"session": ss, "name": me.first_name,
                                      "id": me.id, "added_by": uid}}, upsert=True)
                        ok += 1
            except Exception as e:
                logger.error(f"JSON extract failed: {e}"); fail += 1

        for sf in glob.glob(f"{edir}/**/*.session", recursive=True):
            if f"{os.path.splitext(sf)[0]}.json" in glob.glob(f"{edir}/**/*.json", recursive=True): continue
            try:
                ss = extract_string_session(sf, API_ID)
                if _is_valid_session(ss):
                    async with make_session_client(ss, "s", no_updates=True) as app:
                        me = await app.get_me()
                        await db_call(target_col.update_one, {"phone": me.phone_number},
                            {"$set": {"session": ss, "name": me.first_name,
                                      "id": me.id, "added_by": uid}}, upsert=True)
                        ok += 1
            except Exception: fail += 1

        for tf in glob.glob(f"{edir}/**/*.txt", recursive=True):
            try:
                with open(tf) as f: ss = f.read().strip()
                if _is_valid_session(ss):
                    async with make_session_client(ss, "t", no_updates=True) as app:
                        me = await app.get_me()
                        await db_call(target_col.update_one, {"phone": me.phone_number},
                            {"$set": {"session": ss, "name": me.first_name,
                                      "id": me.id, "added_by": uid}}, upsert=True)
                        ok += 1
            except Exception: fail += 1

        await s.edit_text(f"🏁 **Import Complete**\n✅ `{ok}` | ❌ `{fail}`")
        # start raw listeners for new accounts
        asyncio.create_task(_start_raw_listeners())
    finally:
        try: os.remove(fp)
        except Exception: pass
        try: shutil.rmtree(edir)
        except Exception: pass
        user_data.pop(uid, None)


async def handle_media_internal(client, message: Message):
    uid = message.from_user.id
    if uid not in user_data: return
    step = user_data[uid].get("step")
    if step == "eng_vc_music":
        s = await message.reply("⏳ Downloading VC media...")
        fp = await message.download(file_name=f"vc_{uuid.uuid4().hex}.mp3")
        if fp and os.path.getsize(fp) > 0:
            user_data[uid]["vc_media_url"] = fp
            user_data[uid]["is_local_file"] = True
            user_data[uid]["step"] = "eng_vc_duration"
            await s.edit_text("✅ Saved!\n\n⏱ Send duration (minutes):")
        else:
            await s.edit_text("❌ Telegram rejected the media.")


# ==========================================
# 🚀 ENGAGEMENT RUNNER
# ==========================================
async def execute_vc_join(message: Message, uid: int, data: dict):
    if not PTGCALLS_AVAILABLE:
        return await message.reply("❌ PyTgCalls not available.")

    target_link = data.get("target_link", "")
    duration = data.get("duration", 1) * 60
    vc_phone = data.get("vc_phone", "ALL")
    media_url = data.get("vc_media_url")
    is_local_file = data.get("is_local_file", False)

    chat_target, _ = parse_telegram_link(target_link)
    all_accs = await db_call(accounts_col.find({}).to_list, length=3000, default=[]) or []
    all_accs = [a for a in all_accs if _is_valid_session(a.get("session"))]
    if vc_phone != "ALL":
        all_accs = [a for a in all_accs if a.get("phone") == vc_phone]
    if not all_accs: return await message.reply("❌ No valid accounts.")
    media_path = media_url if media_url else SILENT_FILE
    vc_stop_event.clear()

    stop_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🛑 STOP VC", callback_data=f"vcstop_{uid}")]])
    status = await message.reply(
        f"🎤 **VC Join**\nTarget: `{chat_target}`\nAccounts: `{len(all_accs)}`\n"
        f"Delay: `{VC_JOIN_DELAY_SECONDS}s` | Duration: `{data.get('duration', 1)}` min",
        reply_markup=stop_kb
    )
    local_clients = []; ok = fail = 0; vc_chat_id = chat_target; aborted = False
    for i, acc in enumerate(all_accs):
        if vc_stop_event.is_set() or kill_event.is_set():
            aborted = True; break
        sess = (acc.get("session") or "").strip()
        if not _is_valid_session(sess):
            fail += 1; continue
        app = None
        try:
            app = Client(f"vc_{uuid.uuid4().hex[:6]}", api_id=API_ID, api_hash=API_HASH,
                         session_string=sess, in_memory=True, no_updates=True)
            await app.start()
            chat = None
            try: chat = await app.get_chat(chat_target)
            except Exception:
                try:
                    await app.join_chat(chat_target)
                    chat = await app.get_chat(chat_target)
                except UserAlreadyParticipant:
                    chat = await app.get_chat(chat_target)
            if not chat: raise RuntimeError("no chat")
            call_py = PyTgCalls(app)
            await call_py.start()
            await _play_muted(call_py, chat.id, media_path)
            local_clients.append({"client": app, "call": call_py, "chat_id": chat.id})
            vc_chat_id = chat.id
            ok += 1
            if ok % 5 == 0 or i == len(all_accs) - 1:
                try:
                    await status.edit_text(
                        f"🎤 **Joining...** ✅ `{ok}` | ❌ `{fail}`\n`{i+1}/{len(all_accs)}`",
                        reply_markup=stop_kb)
                except Exception: pass
        except Exception as e:
            logger.error(f"VC err: {e}"); fail += 1
            try:
                if app: await app.stop()
            except Exception: pass
            await asyncio.sleep(min(VC_JOIN_DELAY_SECONDS, 3)); continue
        if i < len(all_accs) - 1:
            waited = 0
            while waited < VC_JOIN_DELAY_SECONDS:
                if vc_stop_event.is_set() or kill_event.is_set():
                    aborted = True; break
                await asyncio.sleep(1); waited += 1
            if aborted: break

    if not local_clients:
        return await status.edit_text("❌ No VC joined.")
    active_calls[str(vc_chat_id)] = local_clients
    await status.edit_text(
        f"{'🛑 Aborted' if aborted else '🎤 VC Joined'}\n✅ `{ok}` | ❌ `{fail}`\n"
        f"Staying `{data.get('duration', 1)}` min...", reply_markup=stop_kb)
    remaining = duration
    while remaining > 0:
        if vc_stop_event.is_set() or kill_event.is_set(): break
        await asyncio.sleep(min(remaining, 2)); remaining -= 2
    for blk in local_clients:
        try: await blk["call"].leave_call(blk["chat_id"])
        except Exception: pass
        try: await blk["client"].stop()
        except Exception: pass
    active_calls.pop(str(vc_chat_id), None)
    if is_local_file and media_url:
        try: os.remove(media_url)
        except Exception: pass
    vc_stop_event.clear()
    return await status.edit_text(f"🏁 **VC Done.** ✅ `{ok}` | ❌ `{fail}`")


async def execute_engagement(message: Message, uid: int, data: dict):
    action = data.get("action")
    if action == "vc":
        return await execute_vc_join(message, uid, data)
    link = data.get("target_link", "")
    req_count = data.get("count", 0)
    comment_text = data.get("comment_text")
    chat_target, msg_id = parse_telegram_link(link)
    all_accs = await db_call(accounts_col.find({}).to_list, length=2000, default=[]) or []
    all_accs = [a for a in all_accs if _is_valid_session(a.get("session"))]
    if not all_accs: return await message.reply("❌ No accounts.")
    accs = all_accs[:req_count] if req_count > 0 else all_accs

    if action == "slowjoin":
        status = await message.reply("🐢 **Slow Join**")
        async def slow_w():
            for acc in accs:
                try:
                    await asyncio.sleep(random.randint(180, 540))
                    async with make_session_client(acc["session"], "sj", no_updates=True) as app:
                        chat = await get_chat_safe(app, chat_target)
                        if not chat: await app.join_chat(chat_target)
                except Exception: pass
        asyncio.create_task(slow_w()); return

    status = await message.reply(f"⚙️ **{action.upper()}** with `{len(accs)}` accounts...")
    ok = fail = 0
    reactions = await get_positive_reactions()
    for i, acc in enumerate(accs):
        if kill_event.is_set(): break
        app = None
        try:
            app = make_session_client(acc["session"], "eng", no_updates=True)
            await app.start()
            chat = await get_chat_safe(app, chat_target)
            if not chat: raise Exception("no chat")
            if action == "views" and msg_id:
                peer = await app.resolve_peer(chat.id)
                if await safe_view_message(app, peer, msg_id): ok += 1
                else: fail += 1
            elif action == "reacts" and msg_id:
                emoji = random.choice(reactions)
                sent = False
                try:
                    await app.send_reaction(chat.id, msg_id, emoji=emoji); sent = True
                except Exception: pass
                if not sent:
                    peer = await app.resolve_peer(chat.id)
                    sent = await safe_send_reaction(app, peer, msg_id, emoji, chat_id=chat.id)
                if sent: ok += 1
                else: fail += 1
            elif action == "fastjoin":
                try: await app.join_chat(chat_target)
                except UserAlreadyParticipant: pass
                ok += 1
            elif action == "leave":
                await app.leave_chat(chat.id); ok += 1
            elif action == "comment" and msg_id:
                try:
                    disc = await app.get_discussion_message(chat.id, msg_id)
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    await disc.reply_text(comment_text)
                except Exception:
                    try:
                        await app.send_message(chat.id, comment_text, reply_to_message_id=msg_id)
                    except Exception: raise
                ok += 1
        except Exception as e:
            logger.error(f"Task {action} failed: {e}"); fail += 1
        finally:
            try:
                if app: await app.stop()
            except Exception: pass
        if i % 5 == 0 or i == len(accs) - 1:
            try:
                await status.edit_text(f"⚙️ **{action.upper()}** ✅ `{ok}` | ❌ `{fail}` | `{i+1}/{len(accs)}`")
            except Exception: pass
    await status.edit_text(f"🏁 **{action.upper()} Done** ✅ `{ok}` | ❌ `{fail}`")


async def process_queue():
    while True:
        task = await vc_queue.get()
        kill_event.clear()
        try:
            await execute_engagement(task["message"], task["uid"], task["data"])
        except Exception as e:
            logger.error(f"Queue Error: {e}")
        finally:
            vc_queue.task_done()

async def ensure_worker_started():
    global worker_started
    if not worker_started:
        worker_started = True
        asyncio.create_task(process_queue())


# ==========================================
# 👑 OWNER / PSEUDO PANEL
# ==========================================
async def show_owner_panel(message: Message, uid: int, is_owner: bool, is_pseudo: bool):
    chans = await get_default_channels()
    reactions = await get_positive_reactions()
    chan_txt = "\n".join([f"• `{c}`" for c in chans]) if chans else "_none_"

    if is_owner:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 List Users/Admins", callback_data="own_list")],
            [InlineKeyboardButton("➕ Add Admin", callback_data="own_addadmin"),
             InlineKeyboardButton("➖ Rm Admin", callback_data="own_rmadmin")],
            [InlineKeyboardButton("👑 Add Pseudo-Owner", callback_data="own_addpseudo"),
             InlineKeyboardButton("🚫 Rm Pseudo-Owner", callback_data="own_rmpseudo")],
            [InlineKeyboardButton("➕ Add Default Channel", callback_data="own_add_channel"),
             InlineKeyboardButton("➖ Remove Default Channel", callback_data="own_rm_channel")],
            [InlineKeyboardButton("❤️ Set Default Reactions", callback_data="own_set_reactions")],
            [InlineKeyboardButton("🩺 Health Check", callback_data="own_health"),
             InlineKeyboardButton("🧹 Clean Revoked", callback_data="own_clean_revoked")],
            [InlineKeyboardButton("🤖 Mass Spam", callback_data="own_mass_spam")],
            [InlineKeyboardButton("💾 Backup Accounts", callback_data="own_backup"),
             InlineKeyboardButton("⏰ Force Wake", callback_data="own_wake")]
        ])
        txt = (f"👑 **Owner Panel**\n\n"
               f"🗄️ DB: `{DB_NAME}`\n"
               f"👂 Listeners: `{len(raw_listener_clients)}`\n\n"
               f"📢 Default Channels ({len(chans)}):\n{chan_txt}\n\n"
               f"❤️ Default reactions: `{' '.join(reactions[:6])}...`")
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("👥 List Users/Admins", callback_data="own_list")],
            [InlineKeyboardButton("➕ Add Admin", callback_data="own_addadmin"),
             InlineKeyboardButton("➖ Rm Admin", callback_data="own_rmadmin")],
            [InlineKeyboardButton("➕ Add Default Channel", callback_data="own_add_channel"),
             InlineKeyboardButton("➖ Remove Default Channel", callback_data="own_rm_channel")],
            [InlineKeyboardButton("❤️ Set Default Reactions", callback_data="own_set_reactions")],
            [InlineKeyboardButton("🩺 Health Check", callback_data="own_health"),
             InlineKeyboardButton("🧹 Clean Revoked", callback_data="own_clean_revoked")],
            [InlineKeyboardButton("🤖 Mass Spam", callback_data="own_mass_spam")],
            [InlineKeyboardButton("⏰ Force Wake", callback_data="own_wake")]
        ])
        txt = (f"👑 **Pseudo-Owner Panel**\n_(no backup / manage-accounts)_\n\n"
               f"🗄️ DB: `{DB_NAME}`\n"
               f"📢 Default Channels ({len(chans)}):\n{chan_txt}\n\n"
               f"❤️ Default reactions: `{' '.join(reactions[:6])}...`")

    return await message.reply(txt, reply_markup=kb)


# ==========================================
# 📺 AUTO CHANNELS MENU
# ==========================================
async def show_auto_channels_menu(message: Message, uid: int):
    channels = await get_default_channels()
    txt = f"📺 **Default Channels** (`{len(channels)}`)\n\n"
    if not channels:
        txt += "_No channels yet._\n\n"
    else:
        for c in channels:
            txt += f"• `{c}`\n"
    txt += (f"\n🗄️ DB: `{DB_NAME}`\n"
            f"👂 Listeners: `{len(raw_listener_clients)}`\n\n"
            "Each channel gets:\n"
            "• ⚡ **Instant** positive reaction via RAW event\n"
            "• ⚡ **Instant** view from all accounts\n"
            "• 🎤 **Instant** muted VC join the moment a livestream starts\n")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Add Channel", callback_data="autochan_add")],
        [InlineKeyboardButton("🗑 Remove Channel", callback_data="autochan_remove")],
        [InlineKeyboardButton("🧪 Test Reaction Now", callback_data="autochan_test"),
         InlineKeyboardButton("🎤 Test VC Join", callback_data="autochan_testvc")],
        [InlineKeyboardButton("🗑 Reset Seen Posts", callback_data="autochan_reset_seen")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="autochan_refresh")],
    ])
    return await message.reply(txt, reply_markup=kb)


# ==========================================
# 🔘 CALLBACK HANDLER
# ==========================================
@bot.on_callback_query()
async def callback_handler(client, query: CallbackQuery):
    uid = query.from_user.id
    data = query.data
    try:
        admin_status = await is_admin(uid)
        pseudo_status = await is_pseudo_owner(uid)
        is_owner = (uid == OWNER_ID)

        if "_" in data and data.split("_")[0] not in (
            "engvc","vcpick","own","bot","skip","vcstop","autochan"
        ):
            parts = data.split("_")
            if len(parts) > 1 and parts[1].replace("+","").isdigit():
                acc = await db_call(accounts_col.find_one, {"phone": parts[1]}, default=None)
                if acc and not admin_status and acc.get("added_by") != uid:
                    return await query.answer("⚠️ Access denied.", show_alert=True)

        if data.startswith("eng") and not admin_status:
            return await query.answer("Admins only.", show_alert=True)
        if data.startswith("autochan") and not admin_status:
            return await query.answer("Admins only.", show_alert=True)
        if data.startswith("own_") and not (is_owner or pseudo_status):
            return await query.answer("Owner only.", show_alert=True)

        if data == "autochan_add":
            user_data[uid] = {"step": "own_add_channel"}
            return await query.message.reply("📺 Send the channel link (@username or https://t.me/...):")
        if data == "autochan_remove":
            user_data[uid] = {"step": "own_remove_channel"}
            return await query.message.reply("🗑 Send the channel link to remove:")
        if data == "autochan_refresh":
            return await show_auto_channels_menu(query.message, uid)
        if data == "autochan_reset_seen":
            r = await db_call(chan_seen_col.delete_many, {})
            dc = getattr(r, "deleted_count", 0) if r else 0
            return await query.message.reply(f"✅ Cleared `{dc}` seen records.")
        if data == "autochan_test":
            channels = await get_default_channels()
            if not channels:
                return await query.answer("No default channels added.", show_alert=True)
            await query.message.reply(f"🧪 Forcing reaction+view on `{len(channels)}` channel(s)...")
            for ch in channels:
                try: await _force_process_channel(ch)
                except Exception as e: logger.error(f"test err: {e}")
            await query.message.reply("✅ Force test done.")
            return
        if data == "autochan_testvc":
            channels = await get_default_channels()
            if not channels:
                return await query.answer("No default channels.", show_alert=True)
            await query.message.reply("🎤 Checking VCs now (joining if live)...")
            for ch in channels:
                try: await _auto_join_vc_for_channel(ch, force=True)
                except Exception as e: logger.error(f"vc test err: {e}")
            await query.message.reply("✅ VC check done.")
            return

        if data == "own_list":
            u_ = await db_call(users_col.find({}).to_list, length=None, default=[]) or []
            a_ = await db_call(admin_col.find({}).to_list, length=None, default=[]) or []
            p_ = await db_call(pseudo_col.find({}).to_list, length=None, default=[]) or []
            txt = f"👑 Owner: `{OWNER_ID}`\n🗄️ DB: `{DB_NAME}`\n\n👑 Pseudo-Owners:\n"
            for p in p_: txt += f"`{p['user_id']}`\n"
            txt += "\n👮 Admins:\n"
            for a in a_: txt += f"`{a['user_id']}`\n"
            txt += "\n👤 Users (top 100):\n"
            for u in u_[:100]: txt += f"`{u['user_id']}`\n"
            return await query.message.reply(txt)

        if data == "own_addpseudo":
            if not is_owner: return await query.answer("Owner only.", show_alert=True)
            user_data[uid] = {"step": "own_addpseudo"}
            return await query.message.reply("👑 Send User ID to make **Pseudo-Owner**:")
        if data == "own_rmpseudo":
            if not is_owner: return await query.answer("Owner only.", show_alert=True)
            user_data[uid] = {"step": "own_rmpseudo"}
            return await query.message.reply("🚫 Send User ID to remove:")
        if data == "own_addadmin":
            user_data[uid] = {"step": "own_addadmin"}
            return await query.message.reply("🆔 Send User ID to make Admin:")
        if data == "own_rmadmin":
            user_data[uid] = {"step": "own_rmadmin"}
            return await query.message.reply("🆔 Send User ID to remove:")
        if data == "own_add_channel":
            user_data[uid] = {"step": "own_add_channel"}
            return await query.message.reply("📺 Send the channel link:")
        if data == "own_rm_channel":
            user_data[uid] = {"step": "own_remove_channel"}
            return await query.message.reply("🗑 Send the channel link to remove:")
        if data == "own_set_reactions":
            user_data[uid] = {"step": "own_default_reactions"}
            return await query.message.reply(
                "❤️ Send the positive reactions separated by spaces or commas:\n"
                "Example: `👍 ❤️ 🔥 🥰 👏 😁 🎉`")
        if data == "own_health":
            asyncio.create_task(run_health_check(query.message))
            return await query.answer("Started.", show_alert=True)
        if data == "own_clean_revoked":
            asyncio.create_task(clean_revoked_task(query.message))
            return await query.answer("Scanning...", show_alert=True)
        if data == "own_rm_dead_confirm":
            dead = DEAD_ACCOUNTS.pop(query.message.chat.id, [])
            if not dead: return await query.answer("None.", show_alert=True)
            for ph in dead: await db_call(accounts_col.delete_one, {"phone": ph})
            return await query.message.edit_text(f"✅ Removed {len(dead)} dead accounts.")
        if data == "own_mass_spam":
            asyncio.create_task(run_mass_spam_check(query.message))
            return await query.answer("Started.", show_alert=True)
        if data == "own_backup":
            if not is_owner: return await query.answer("Owner only.", show_alert=True)
            asyncio.create_task(cb_backup(client, query)); return
        if data == "own_wake":
            asyncio.create_task(manual_wake_task(query.message))
            return await query.answer("Waking...", show_alert=True)

        if data.startswith("vcstop_"):
            vc_stop_event.set()
            return await query.answer("🛑 Stop signal sent.", show_alert=True)

        if data == "eng_vcmenu":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎤 All Accounts", callback_data="engvc_all")],
                [InlineKeyboardButton("👤 One Account", callback_data="engvc_one")],
                [InlineKeyboardButton("🛑 Stop All VC", callback_data="vcstop_0")]
            ])
            return await query.message.reply("🎤 **VC Selection:**", reply_markup=kb)
        if data == "engvc_all":
            user_data[uid] = {"step": "eng_target", "action": "vc", "vc_phone": "ALL"}
            return await query.message.reply("🎯 Send Target Group/Chat Link or ID:")
        if data == "engvc_one":
            accs = await db_call(accounts_col.find({} if admin_status else {"added_by": uid}).to_list, length=50, default=[]) or []
            if not accs: return await query.message.reply("❌ No accounts.")
            kb = [[InlineKeyboardButton(
                f"{a.get('name', 'User')} | {get_display_phone(a.get('phone', ''), a.get('added_by'), uid)}",
                callback_data=f"vcpick_{a.get('phone', '')}")] for a in accs]
            return await query.message.reply("⚙️ Select account:", reply_markup=InlineKeyboardMarkup(kb))
        if data.startswith("vcpick_"):
            phone = data.split("_", 1)[1]
            user_data[uid] = {"step": "eng_target", "action": "vc", "vc_phone": phone}
            return await query.message.reply("🎯 Send Target Group/Chat Link or ID:")

        if data.startswith("eng_"):
            action = data.split("_", 1)[1]
            user_data[uid] = {"step": "eng_target", "action": action}
            return await query.message.reply("🎯 Send Target Chat/Message Link:")

        if data.startswith("manage_"):
            phone = data.split("_", 1)[1]
            acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
            if not acc: return await query.answer("Not found.")
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("ℹ️ Info", callback_data=f"accinfo_{phone}")],
                [InlineKeyboardButton("📥 OTP", callback_data=f"getotp_{phone}"),
                 InlineKeyboardButton("🤖 Spam", callback_data=f"spamck_{phone}")],
                [InlineKeyboardButton("👤 Name", callback_data=f"chgname_{phone}"),
                 InlineKeyboardButton("🖼 DP", callback_data=f"accdp_{phone}")],
                [InlineKeyboardButton("📝 Bio", callback_data=f"accbio_{phone}"),
                 InlineKeyboardButton("💻 Devices", callback_data=f"devices_{phone}")],
                [InlineKeyboardButton("🧹 Clean", callback_data=f"cleanacc_{phone}")],
                [InlineKeyboardButton("📩 Mark Read", callback_data=f"markread_{phone}")],
                [InlineKeyboardButton("🔐 2FA", callback_data=f"2faopts_{phone}"),
                 InlineKeyboardButton("📧 Email", callback_data=f"emailopts_{phone}")],
                [InlineKeyboardButton("📢 Create Channel", callback_data=f"mkchan_{phone}")],
                [InlineKeyboardButton("⚙️ Edit Channel", callback_data=f"editchan_{phone}")]
            ])
            return await query.message.edit_text(
                f"⚙️ **Managing {get_display_phone(phone, acc.get('added_by'), uid)}**",
                reply_markup=kb)

        if data.startswith("accinfo_"):   asyncio.create_task(fetch_account_info(query.message, data.split("_", 1)[1], uid))
        elif data.startswith("cleanacc_"):asyncio.create_task(execute_clean_account(query.message, data.split("_", 1)[1]))
        elif data.startswith("markread_"):asyncio.create_task(execute_mark_read(query.message, data.split("_", 1)[1]))
        elif data.startswith("spamck_"):  asyncio.create_task(check_spam(query.message, data.split("_", 1)[1], uid))
        elif data.startswith("devices_"): asyncio.create_task(fetch_devices(query.message, data.split("_", 1)[1]))
        elif data.startswith("rmdev_"):
            p = data.split("_"); asyncio.create_task(remove_device(query.message, p[1], int(p[2])))
        elif data.startswith("emailopts_"):
            phone = data.split("_", 1)[1]
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔍 Status", callback_data=f"checkemail_{phone}")],
                                       [InlineKeyboardButton("🔄 Change", callback_data=f"chgemail_{phone}")]])
            await query.message.edit_text("📧 **Email Options**", reply_markup=kb)
        elif data.startswith("checkemail_"): asyncio.create_task(check_email_status(query.message, data.split("_", 1)[1]))
        elif data.startswith("chgemail_"):
            user_data[uid] = {"step": "chg_email_pass", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("🔐 Send CURRENT 2FA PASSWORD:")
        elif data.startswith("2faopts_"):
            phone = data.split("_", 1)[1]
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔐 Set", callback_data=f"set2fa_{phone}"),
                 InlineKeyboardButton("🔓 Remove", callback_data=f"rm2fa_{phone}")],
                [InlineKeyboardButton("🔄 Change", callback_data=f"chg2fa_{phone}"),
                 InlineKeyboardButton("📩 Reset", callback_data=f"reset2fa_{phone}")]
            ])
            await query.message.edit_text("🔐 **2FA Options**", reply_markup=kb)
        elif data.startswith("reset2fa_"): asyncio.create_task(execute_2fa_reset(query.message, data.split("_", 1)[1]))
        elif data.startswith("set2fa_"):
            user_data[uid] = {"step": "set_2fa_pass", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("🔐 Send NEW password:")
        elif data.startswith("rm2fa_"):
            user_data[uid] = {"step": "rm_2fa_pass", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("🔓 Send CURRENT password:")
        elif data.startswith("chg2fa_"):
            user_data[uid] = {"step": "chg_2fa_old", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("🔄 Send OLD password:")
        elif data.startswith("getotp_"): asyncio.create_task(fetch_otp(query.message, data.split("_", 1)[1], uid))
        elif data.startswith("chgname_"):
            user_data[uid] = {"step": "set_acc_name", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("👤 Send new name:")
        elif data.startswith("accdp_"):
            user_data[uid] = {"step": "acc_set_dp", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("🖼 Send new account photo:")
        elif data.startswith("accbio_"):
            user_data[uid] = {"step": "acc_set_bio", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("📝 Send new Bio text:")
        elif data.startswith("mkchan_"):
            user_data[uid] = {"step": "chan_name", "target_phone": data.split("_", 1)[1]}
            await query.message.reply("📢 Step 1: Channel Name:")
        elif data.startswith("editchan_"):
            phone = data.split("_", 1)[1]
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🖼 DP", callback_data=f"chanact_dp_{phone}"),
                                       InlineKeyboardButton("📝 Bio", callback_data=f"chanact_bio_{phone}")]])
            await query.message.edit_text("Select what to edit:", reply_markup=kb)
        elif data.startswith("chanact_"):
            p = data.split("_")
            user_data[uid] = {"step": "edit_chan_id", "target_phone": p[2],
                              "edit_action": p[1], "action": "editchan"}
            await query.message.reply("Send target channel ID/username:")

        if data == "bot_edit_welcome":
            user_data[uid] = {"step": "set_bot_welcome_pic"}
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip Photo", callback_data="skip_bot_pic")]])
            return await query.message.reply("🖼 Send photo (or Skip):", reply_markup=kb)
        if data == "skip_bot_pic":
            user_data.setdefault(uid, {})["bot_pic"] = None
            user_data[uid]["step"] = "set_bot_welcome_txt"
            return await query.message.edit_text("📝 Send welcome text:")

    except Exception as e:
        logger.error(f"Callback error: {e}")
    finally:
        try: await query.answer()
        except Exception: pass


async def cb_backup(client, query):
    await query.answer("Backup started...")
    s = await query.message.reply("⏳ Backing up...")
    accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
    edir = f"bk_{int(time.time())}"
    os.makedirs(edir, exist_ok=True)
    added = 0
    for acc in accs:
        sess = acc.get("session")
        if _is_valid_session(sess):
            ph = acc.get("phone", "unknown")
            with open(os.path.join(edir, f"{ph}.txt"), "w") as f: f.write(sess)
            added += 1
    if added > 0:
        shutil.make_archive(edir, "zip", edir)
        if os.path.exists(f"{edir}.zip") and os.path.getsize(f"{edir}.zip") > 50:
            await query.message.reply_document(f"{edir}.zip", caption=f"💾 {added} accounts")
        else:
            await query.message.reply("✅ Backup done (empty).")
    else:
        await query.message.reply("❌ No valid accounts.")
    try: os.remove(f"{edir}.zip")
    except Exception: pass
    try: shutil.rmtree(edir)
    except Exception: pass
    try: await s.delete()
    except Exception: pass


# ==========================================
# ⚙️ ACCOUNT UTILITIES
# ==========================================
async def finalize_login(message, uid):
    temp = user_data[uid]["client"]
    phone = user_data[uid]["phone"]
    ss = await temp.export_session_string()
    me = await temp.get_me()
    await db_call(accounts_col.update_one, {"phone": phone},
        {"$set": {"session": ss, "name": me.first_name, "id": me.id, "added_by": uid}}, upsert=True)
    await message.reply(f"✅ Account {me.first_name} saved!")
    try: await temp.disconnect()
    except Exception: pass
    user_data.pop(uid, None)
    # start raw listener for new account immediately
    asyncio.create_task(_start_raw_listeners())


async def fetch_account_info(message, phone, uid):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Fetching...")
    try:
        async with make_session_client(acc["session"], "i", no_updates=True) as app:
            me = await app.get_me()
            t = me.id
            if t < 1e9: age = "Very Old (Pre 2019)"
            elif t < 2e9: age = "Old (2019-2021)"
            elif t < 6e9: age = "Recent (2022-2023)"
            elif t < 7e9: age = "New (2023-2024)"
            else: age = "Very New (2024+)"
            try: dc = (await app.invoke(functions.help.GetNearestDc())).this_dc
            except Exception: dc = "?"
            await s.edit_text(
                f"ℹ️ **Info**\n👤 `{me.first_name}`\n📱 `{get_display_phone(str(me.phone_number), acc.get('added_by'), uid)}`\n"
                f"🆔 `{me.id}`\n🌍 DC `{dc}`\n⏳ `{age}`")
    except (AuthKeyUnregistered, UserDeactivatedBan):
        await db_call(accounts_col.delete_one, {"phone": phone})
        await s.edit_text(f"💀 **Revoked/deleted.** Removed from DB.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def check_email_status(message, phone):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Checking...")
    try:
        async with make_session_client(acc["session"], "e", no_updates=True) as app:
            pw = await app.invoke(functions.account.GetPassword())
            if not pw.has_password: return await s.edit_text("❌ No 2FA Password.")
            if pw.has_recovery: return await s.edit_text("✅ Recovery Email SET.")
            if pw.email_unconfirmed_pattern:
                return await s.edit_text(f"⏳ Pending `{pw.email_unconfirmed_pattern}`")
            await s.edit_text("❌ No Recovery Email.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def execute_email_change(message, uid, new_email):
    d = user_data.pop(uid, {})
    phone = d["target_phone"]; pw = d["current_pass"]
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Updating...")
    try:
        async with make_session_client(acc["session"], "ec", no_updates=True) as app:
            p = await app.invoke(functions.account.GetPassword())
            if not p.has_password: return await s.edit_text("❌ Need 2FA password first.")
            await app.change_cloud_password(current_password=pw, new_password=pw, recovery_email=new_email)
            await s.edit_text("✅ Email Update Sent!")
    except PasswordHashInvalid: await s.edit_text("❌ Wrong 2FA password.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def execute_clean_account(message, phone):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    s = await message.reply("🧹 Cleaning...")
    try:
        async with make_session_client(acc["session"], "c", no_updates=True) as app:
            left = clr = 0
            async for d in app.get_dialogs():
                try:
                    if d.chat.type in (ChatType.GROUP, ChatType.SUPERGROUP, ChatType.CHANNEL):
                        await app.leave_chat(d.chat.id); left += 1
                    elif d.chat.type == ChatType.PRIVATE:
                        await app.delete_user_history(d.chat.id); clr += 1
                except Exception: pass
            await s.edit_text(f"✅ Cleaned! 🚪 `{left}` 🗑 `{clr}`")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def execute_mark_read(message, phone):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    s = await message.reply("📩 Marking read...")
    try:
        async with make_session_client(acc["session"], "mr", no_updates=True) as app:
            n = 0
            async for d in app.get_dialogs():
                if d.unread_messages_count > 0:
                    try: await app.read_chat_history(d.chat.id); n += 1
                    except Exception: pass
            await s.edit_text(f"✅ Marked `{n}` chats read.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def check_spam(message, phone, uid):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    disp = get_display_phone(phone, acc.get("added_by"), uid)
    s = await message.reply("⏳ Checking @SpamBot...")
    try:
        async with make_session_client(acc["session"], "sp", no_updates=True) as app:
            await app.send_message("SpamBot", "/start")
            await asyncio.sleep(2)
            async for m in app.get_chat_history("SpamBot", limit=1):
                if "Good news" in (m.text or "") or "free from" in (m.text or ""):
                    await s.edit_text(f"✅ **{disp} FREE.**")
                else:
                    await s.edit_text(f"⚠️ **{disp} LIMITED.** `{m.text}`")
                return
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def execute_2fa_reset(message, phone):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Requesting recovery...")
    try:
        async with make_session_client(acc["session"], "2f", no_updates=True) as app:
            await app.invoke(functions.auth.RequestPasswordRecovery())
            await s.edit_text("📩 Recovery Email Sent!")
    except PasswordRecoveryNa: await s.edit_text("❌ No recovery email.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def fetch_devices(message, phone):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    try:
        async with make_session_client(acc["session"], "d", no_updates=True) as app:
            a = await app.invoke(functions.account.GetAuthorizations())
            kb = [[InlineKeyboardButton(f"❌ {x.device_model} ({x.app_name})",
                                        callback_data=f"rmdev_{phone}_{x.hash}")]
                  for x in a.authorizations if not getattr(x, "current", False)]
            if not kb: await message.reply("✅ No other devices.")
            else: await message.reply("💻 **Active Devices:**", reply_markup=InlineKeyboardMarkup(kb))
    except Exception as e: await message.reply(f"❌ Failed: {e}")


async def remove_device(message, phone, h):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc: return
    try:
        async with make_session_client(acc["session"], "rd", no_updates=True) as app:
            await app.invoke(functions.account.ResetAuthorization(hash=h))
        await message.edit_text("✅ Device logged out!")
    except Exception as e: await message.edit_text(f"❌ Failed: {e}")


async def execute_2fa_action(message, uid, action, old_pass="", new_pass=""):
    d = user_data.pop(uid, {})
    phone = d.get("target_phone")
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Processing 2FA...")
    try:
        async with make_session_client(acc["session"], "2f", no_updates=True) as app:
            if action == "set":
                await app.enable_cloud_password(new_pass)
                await s.edit_text(f"✅ 2FA set: `{new_pass}`")
            elif action == "remove":
                await app.remove_cloud_password(old_pass)
                await s.edit_text("🔓 2FA removed.")
            elif action == "change":
                await app.change_cloud_password(old_pass, new_pass)
                await s.edit_text(f"🔄 2FA changed: `{new_pass}`")
    except PasswordHashInvalid: await s.edit_text("❌ Wrong password.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def fetch_otp(message, phone, uid):
    acc = await db_call(accounts_col.find_one, {"phone": phone}, default=None)
    if not acc or not _is_valid_session(acc.get("session")): return await message.reply("❌ Invalid.")
    try:
        async with make_session_client(acc["session"], "o", no_updates=True) as app:
            async for m in app.get_chat_history(777000, limit=1):
                return await message.reply(
                    f"📥 **OTP** for `{get_display_phone(phone, acc.get('added_by'), uid)}`:\n\n`{m.text}`")
        await message.reply("❌ No OTP found.")
    except Exception as e: await message.reply(f"❌ Failed: {e}")


async def execute_account_name_change(message, uid, new_name):
    d = user_data.pop(uid, {})
    acc = await db_call(accounts_col.find_one, {"phone": d.get("target_phone")}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Changing name...")
    try:
        async with make_session_client(acc["session"], "n", no_updates=True) as app:
            await app.update_profile(first_name=new_name, last_name="")
        await db_call(accounts_col.update_one, {"phone": d["target_phone"]},
                      {"$set": {"name": new_name}})
        await s.edit_text(f"✅ Name changed to **{new_name}**.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


async def apply_account_bio(message, uid, text):
    d = user_data.pop(uid, {})
    acc = await db_call(accounts_col.find_one, {"phone": d.get("target_phone")}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Updating Bio...")
    try:
        async with make_session_client(acc["session"], "b", no_updates=True) as app:
            await app.update_profile(bio=text)
        await s.edit_text("✅ Bio updated.")
    except Exception as e: await s.edit_text(f"❌ Error: {e}")


async def finalize_channel_creation(message, uid):
    d = user_data.pop(uid, {})
    acc = await db_call(accounts_col.find_one, {"phone": d.get("target_phone")}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    s = await message.reply("⏳ Creating Channel...")
    try:
        async with make_session_client(acc["session"], "ch", no_updates=True) as app:
            ch = await app.create_channel(d["chan_name"], d.get("chan_desc", ""))
            if d.get("chan_dp_path"):
                await app.set_chat_photo(ch.id, photo=d["chan_dp_path"])
                try: os.remove(d["chan_dp_path"])
                except Exception: pass
            try:
                await app.set_chat_username(ch.id, d["chan_user"])
                await s.edit_text(f"✅ **Channel Created!** https://t.me/{d['chan_user']}")
            except (UsernameInvalid, UsernameOccupied) as e:
                await s.edit_text(f"⚠️ **Username Error:** `{e}`")
    except Exception as e: await s.edit_text(f"❌ Creation Failed: {e}")


async def apply_channel_edit(message, uid):
    d = user_data.pop(uid, {})
    acc = await db_call(accounts_col.find_one, {"phone": d.get("target_phone")}, default=None)
    if not acc: return await message.reply("❌ Invalid.")
    target = d.get("target_chan")
    if isinstance(target, str) and target.lstrip("-").isdigit(): target = int(target)
    s = await message.reply("⏳ Applying...")
    try:
        async with make_session_client(acc["session"], "ed", no_updates=True) as app:
            await app.set_chat_description(target, message.text)
            await s.edit_text("✅ Bio updated.")
    except Exception as e: await s.edit_text(f"❌ Failed: {e}")


# ==========================================
# 🩺 HEALTH / SESSION CHECKS
# ==========================================
async def run_health_check(message):
    s = await message.reply("🩺 Health Check...")
    accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
    alive, dead, revoked = 0, [], []
    for i, acc in enumerate(accs):
        sess = acc.get("session")
        if not _is_valid_session(sess): continue
        try:
            async with make_session_client(sess, "h", no_updates=True) as app:
                await app.get_me(); alive += 1
        except (AuthKeyUnregistered, UserDeactivatedBan):
            revoked.append(acc.get("phone"))
        except (UserDeactivated,):
            dead.append(acc.get("phone"))
        except Exception:
            alive += 1
        if i % 10 == 0 or i == len(accs) - 1:
            try:
                await s.edit_text(f"🩺 ✅{alive} | 💀{len(dead)} | 🚫{len(revoked)}")
            except Exception: pass
    DEAD_ACCOUNTS[message.chat.id] = dead + revoked
    kb = None
    if dead or revoked:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"🧹 Clean {len(dead)+len(revoked)} Revoked/Dead",
                                  callback_data="own_rm_dead_confirm")]
        ])
    await s.edit_text(
        f"🏁 **Health Done**\n✅ Alive: `{alive}`\n💀 Dead: `{len(dead)}`\n🚫 Revoked: `{len(revoked)}`",
        reply_markup=kb)


async def clean_revoked_task(message):
    s = await message.reply("🧹 Scanning for revoked sessions...")
    accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
    removed, checked = 0, 0
    for i, acc in enumerate(accs):
        sess = acc.get("session")
        if not _is_valid_session(sess):
            await db_call(accounts_col.delete_one, {"phone": acc.get("phone")})
            removed += 1
            continue
        checked += 1
        try:
            async with make_session_client(sess, "cr", no_updates=True) as app:
                await app.get_me()
        except (AuthKeyUnregistered, UserDeactivatedBan, UserDeactivated):
            await db_call(accounts_col.delete_one, {"phone": acc.get("phone")})
            removed += 1
        except Exception:
            pass
        if i % 10 == 0 or i == len(accs) - 1:
            try:
                await s.edit_text(f"🧹 Scanned `{i+1}/{len(accs)}` — removed `{removed}`")
            except Exception: pass
    await s.edit_text(f"🏁 **Cleanup Complete**\n✅ Checked: `{checked}`\n🗑 Removed: `{removed}`")


async def run_mass_spam_check(message):
    s = await message.reply("🤖 Mass Spam Check...")
    accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
    f = l = x = 0
    for i, acc in enumerate(accs):
        sess = acc.get("session")
        if not _is_valid_session(sess): continue
        try:
            async with make_session_client(sess, "ms", no_updates=True) as app:
                await app.send_message("SpamBot", "/start")
                await asyncio.sleep(2)
                async for m in app.get_chat_history("SpamBot", limit=1):
                    if "Good news" in (m.text or "") or "free from" in (m.text or ""): f += 1
                    else: l += 1
                    break
        except Exception: x += 1
        if i % 5 == 0 or i == len(accs) - 1:
            try: await s.edit_text(f"🤖 `{i+1}/{len(accs)}` ✅{f} ⚠️{l} ❌{x}")
            except Exception: pass
    await s.edit_text(f"🏁 **Done** ✅ `{f}` ⚠️ `{l}` ❌ `{x}`")


async def manual_wake_task(message):
    s = await message.reply("⏳ Waking...")
    accs = await db_call(accounts_col.find({}).to_list, length=None, default=[]) or []
    ok = 0
    for i, acc in enumerate(accs):
        sess = acc.get("session")
        if not _is_valid_session(sess): continue
        try:
            async with make_session_client(sess, "w", no_updates=True) as app:
                await app.invoke(functions.account.UpdateStatus(offline=False))
                ok += 1
        except Exception: pass
        if i % 5 == 0 or i == len(accs) - 1:
            try: await s.edit_text(f"⏳ `{ok}/{len(accs)}`")
            except Exception: pass
    await s.edit_text(f"✅ **Wake Done.** Online: `{ok}/{len(accs)}`")


# ==========================================
# 🛑 COMMANDS
# ==========================================
async def cmd_start(client, message: Message):
    uid = message.from_user.id
    await check_access(uid)
    bs = await db_call(settings_col.find_one, {"id": "bot_start_msg"}, default=None)
    admin_status = await is_admin(uid)
    is_owner = (uid == OWNER_ID)
    pseudo = await is_pseudo_owner(uid)
    kb = main_menu(uid, admin_status, pseudo, is_owner)
    if bs:
        t = bs.get("text", f"👋 Welcome {message.from_user.first_name}!").replace("{name}", message.from_user.first_name)
        if bs.get("photo_id"):
            try: await message.reply_photo(bs["photo_id"], caption=t, reply_markup=kb)
            except Exception: await message.reply(t, reply_markup=kb)
        else: await message.reply(t, reply_markup=kb)
    else:
        await message.reply("👋 **Welcome!**\nAdvanced Manager Bot active.", reply_markup=kb)


async def cmd_ping(client, message: Message):
    t0 = time.time()
    msg = await message.reply("🏓 Pinging...")
    t1 = time.time()
    try:
        me = await bot.get_me()
        bot_name = me.first_name
    except Exception:
        bot_name = "?"
    text = (
        f"🏓 **Pong!**\n"
        f"⏱ Latency: `{(t1-t0)*1000:.0f} ms`\n"
        f"🤖 Bot: `{bot_name}`\n"
        f"🗄️ DB: `{DB_NAME}`\n"
        f"👂 Listeners: `{len(raw_listener_clients)}`\n"
        f"📅 Time: `{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC`"
    )
    try: await msg.edit_text(text)
    except Exception: await msg.edit(text)


async def cmd_stopvc(client, message: Message):
    if not await is_admin(message.from_user.id): return await message.reply("Admins only.")
    vc_stop_event.set(); kill_event.set()
    await message.reply("🛑 Stop signal sent.")


async def cmd_leavevc(client, message: Message):
    if not await is_admin(message.from_user.id): return await message.reply("Admins only.")
    kill_event.set(); vc_stop_event.set()
    n = 0
    for cid, clients in list(active_calls.items()):
        for d in clients:
            try: await d["call"].leave_call(d["chat_id"])
            except Exception: pass
            try: await d["client"].stop()
            except Exception: pass
            n += 1
    active_calls.clear()
    for reg_key, reg in list(multi_vc_registry.items()):
        try: reg["stop"].set()
        except Exception: pass
        for key, blk in list(reg.get("active", {}).items()):
            try: await blk["call"].leave_call(blk["chat_id"])
            except Exception: pass
            try: await blk["client"].stop()
            except Exception: pass
            n += 1
        reg["active"] = {}
    await message.reply(f"✅ Cleared {n} sessions.")


async def cmd_cleanrevoked(client, message: Message):
    if not await is_admin(message.from_user.id): return await message.reply("Admins only.")
    asyncio.create_task(clean_revoked_task(message))


async def cmd_reload_listeners(client, message: Message):
    if not await is_admin(message.from_user.id): return await message.reply("Admins only.")
    await message.reply("🔄 Reloading raw listeners...")
    asyncio.create_task(_start_raw_listeners())
    await refresh_watched_chat_ids()


# ==========================================
# ▶️ BOOT
# ==========================================
async def _run_bot():
    logger.info("Initializing background services...")
    await refresh_watched_chat_ids()
    await ensure_worker_started()
    asyncio.create_task(dummy_web_server())
    asyncio.create_task(auto_online_task())
    asyncio.create_task(channel_watcher_task())
    asyncio.create_task(vc_watcher_task())
    asyncio.create_task(raw_listener_supervisor())
    # start listeners immediately in background too
    asyncio.create_task(_start_raw_listeners())
    logger.info(f"Bot fully operational. DB={DB_NAME}")
    await idle()


async def main():
    try:
        logger.info(f"Pyrogram: {pyrogram.__version__}")
        try:
            import pytgcalls as _p
            logger.info(f"PyTgCalls: {getattr(_p, '__version__', 'unknown')}")
            logger.info(f"MediaStream: {MediaStream is not None}")
        except Exception as e:
            logger.warning(f"PyTgCalls warn: {e}")
        await bot.start()
        await _run_bot()
        await bot.stop()
    except Exception as e:
        logger.critical(f"Start failure: {e}")


if __name__ == "__main__":
    loop = bot.loop
    loop.run_until_complete(main())