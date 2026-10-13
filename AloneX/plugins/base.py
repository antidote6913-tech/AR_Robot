import base64
import asyncio
import config
import logging
import random
import time
from datetime import datetime
from collections import defaultdict, deque
from pyrogram import enums, types, Client, filters
from pyrogram.enums import ButtonStyle
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait, UserIsBlocked, InputUserDeactivated, BadRequest, RPCError
from AloneX import MODULE, pbot, SUPPORT_CHAT, LOGS_CHANNEL, UPDATE_CHANNEL, BOT_USERNAME, prefix_cmds, font
from AloneX.db.users import add_user, get_user_join_source, activate_user
from AloneX.db.chats import check_chat_exists
from AloneX.db.autofilter import get_file_by_index
from AloneX.helpers.misc import get_help_button
from AloneX.helpers.utils import autofilter_send_file, decode_to_base64, auto_delete, time_formatter
from AloneX.helpers.pyro_utils import check_membership, no_channel
from AloneX.db.rules import get_rules
from AloneX.plugins.rules import DEFAULT_MESSAGE
from AloneX.plugins.adminpanel import handle_admin_deeplink
from AloneX.plugins.settings import handle_settings_deeplink

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

_user_cache = {}
_chat_cache = {}
_rules_cache = {}
_file_cache = {}
_chat_name_cache = {}
_membership_cache = {}
_user_request_times = {}
_request_queue = deque(maxlen=500)
_db_write_queue = []
_db_write_lock = asyncio.Lock()
_log_queue = []
_log_lock = asyncio.Lock()
_pending_tasks = {}
MAX_REQ_PER_SEC = 1000
USER_COOLDOWN = 0.00001
BATCH_INTERVAL = 0.01
CACHE_TTL = 3600
PRELOAD_ENABLED = True
START_EFFECTS = {'👍': 5107584321108051014, '👎': 5104858069142078462, '❤': 5159385139981059251, '🔥': 5104841245755180586, '🎉': 5046509860389126442, '💩': 5046589136895476101}
EFFECT_VALUES = list(START_EFFECTS.values())
BOT_UN = BOT_USERNAME.lstrip('@')
SE = (5107584321108051014, 5104858069142078462, 5159385139981059251, 5104841245755180586, 5046509860389126442, 5046589136895476101)
_SB = _bi = _tr = None

_user_queue = asyncio.Queue()
_processing_users = set()
_last_process_time = defaultdict(float)
_semaphore = asyncio.Semaphore(15000)
_queue_processor_started = False
_cmd_cache = defaultdict(dict)
_cache_expiry = 3600

def _msb():
    global _SB
    if _SB: return _SB
    sc = SUPPORT_CHAT.lstrip('@') if not SUPPORT_CHAT.startswith("http") else SUPPORT_CHAT
    uc = UPDATE_CHANNEL.lstrip('@') if not UPDATE_CHANNEL.startswith("http") else UPDATE_CHANNEL
    su = sc if sc.startswith("http") else f"https://t.me/{sc}"
    uu = uc if uc.startswith("http") else f"https://t.me/{uc}"
    _SB = InlineKeyboardMarkup([[InlineKeyboardButton(font('Support'), url=su, style=ButtonStyle.PRIMARY)],[InlineKeyboardButton(font('Updates'), url=uu, style=ButtonStyle.SUCCESS)]])
    return _SB

SB = _msb()

def irc(cid): pass
def imc(cid, uid): pass

async def _st():
    global _tr, _bi
    if _tr: return
    _tr = True
    if not _bi:
        try: _bi = await pbot.get_me()
        except Exception as e: print(f"[ST ERROR] {e}")

def _gbi(): return _bi

def _gsb(uid):
    su = SUPPORT_CHAT if SUPPORT_CHAT.startswith("http") else f"https://t.me/{SUPPORT_CHAT.lstrip('@')}"
    uu = UPDATE_CHANNEL if UPDATE_CHANNEL.startswith("http") else f"https://t.me/{UPDATE_CHANNEL.lstrip('@')}"
    au = f"https://t.me/{BOT_UN}?startgroup=true"

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                font("＋ 𝐈ηᴛᴇɢʀᴀᴛᴇ 𝐈η 𝐘ᴏᴜʀ 𝐂ʜᴀᴛ ＋"),
                url=au,
                style=ButtonStyle.SUCCESS
            )
        ],
        [
            InlineKeyboardButton(
                font("🎧 𝐌ᴜsɪᴄ"),
                url="https://t.me/AR_MUSIC_69BOT?start=help",
                style=ButtonStyle.PRIMARY
            ),
            InlineKeyboardButton(
                font("⚙ 𝐇ᴇʟᴘ ⚙"),
                callback_data=f"help_{uid}",
                style=ButtonStyle.DANGER
            )
        ],
        [
            InlineKeyboardButton(
                font("𝐔ᴘᴅᴀᴛᴇs ⎘"),
                url=uu,
                style=ButtonStyle.PRIMARY
            ),
            InlineKeyboardButton(
                font("𝐒ᴜᴘᴘᴏʀᴛ ☏︎"),
                url=su,
                style=ButtonStyle.PRIMARY
            )
        ],
        [
            InlineKeyboardButton(
                font("σᴡηᴇʀ ✧ ᴀʀ"),
                user_id=config.ALONE_OWNER_ID,
                style=ButtonStyle.SUCCESS
            )
        ]
    ])
    
async def _sp(cid, p, c=None, rm=None, eid=None, rt=None):
    try:
        if isinstance(p, (list, tuple)): p = p[0] if p else None
        if isinstance(c, (list, tuple)): c = c[0] if c else None
        if not p or not isinstance(p, str):
            if c:
                try: return await pbot.send_message(chat_id=cid, text=c, reply_markup=rm, reply_to_message_id=rt, effect_id=eid)
                except Exception as e: 
                    print(f"[SP MSG ERROR] {e}")
                    return await pbot.send_message(chat_id=cid, text=c)
            return None
        kw = {'chat_id': cid, 'photo': p, 'caption': c, 'reply_markup': rm}
        if rt: kw['reply_to_message_id'] = rt
        try:
            if eid: kw['message_effect_id'] = eid
            return await pbot.send_photo(**kw)
        except TypeError:
            try:
                kw.pop('message_effect_id', None)
                kw['effect_id'] = eid
                return await pbot.send_photo(**kw)
            except: raise
    except FloodWait as e:
        print(f"[SP FLOODWAIT] {e}")
        await asyncio.sleep(min(10, getattr(e, "value", 1)))
        return await _sp(cid, p, c, rm, eid, rt)
    except Exception as e:
        print(f"[SP ERROR] {e}")
        try: return await pbot.send_message(chat_id=cid, text=c or "❌ Photo failed to send.", reply_markup=rm)
        except Exception as e2: 
            print(f"[SP FALLBACK ERROR] {e2}")
            return None

async def _sm(cid, t, rm=None, eid=None, rt=None):
    try:
        if isinstance(t, (list, tuple)): t = str(t[0]) if t else ""
        if not isinstance(t, str): t = str(t)
        if not t: return None
        kw = {'chat_id': cid, 'text': t, 'reply_markup': rm}
        if rt: kw['reply_to_message_id'] = rt
        if eid:
            try:
                kw['effect_id'] = eid
                return await pbot.send_message(**kw)
            except TypeError:
                kw.pop('effect_id', None)
                kw['message_effect_id'] = eid
                return await pbot.send_message(**kw)
        return await pbot.send_message(**kw)
    except FloodWait as e:
        print(f"[SM FLOODWAIT] {e}")
        await asyncio.sleep(min(10, getattr(e, "value", 1)))
        return await _sm(cid, t, rm, eid, rt)
    except Exception as e:
        print(f"[SM ERROR] {e}")
        try: return await pbot.send_message(chat_id=cid, text=t)
        except Exception as e2:
            print(f"[SM FALLBACK ERROR] {e2}")
            return None

async def _gcn(cid):
    try:
        ch = await pbot.get_chat(cid)
        return getattr(ch, "title", "this chat")
    except Exception as e:
        print(f"[GCN ERROR] {e}")
        return "this chat"

async def _cm(cid, uid):
    try: return await check_membership(cid, uid)
    except Exception as e:
        print(f"[CM ERROR] {e}")
        return False

def _db64(p):
    try: return base64.b64decode(p.encode()).decode()
    except Exception as e:
        print(f"[DB64 ERROR] {e}")
        return None

async def _haf(uid, t, p):
    try:
        d = decode_to_base64(p.encode())
        au, _, idx = d.partition("&")
        idx = int(idx) if idx.isdigit() else None
        m = await _cm(config.AF_SUB_CHAT, uid)
        if m:
            if not idx:
                await _sm(uid, "❌ Invalid file reference.")
                return True
            f = await get_file_by_index(idx)
            await autofilter_send_file(pbot, t, uid, f)
            return True
        cu = f"https://t.me/{config.AF_SUB_CHAT.lstrip('@')}"
        bs = f"t.me/{BOT_UN}?start={t}"
        b = InlineKeyboardMarkup([[InlineKeyboardButton(font('📢 My ( Channel / Group )'), url=cu, style=ButtonStyle.PRIMARY)], [InlineKeyboardButton(font('📁 Get File'), url=bs, style=ButtonStyle.SUCCESS)]])
        await _sp(uid, config.FORCE_JOIN_IMG, config.AF_SUB_TEXT, b, random.choice(SE))
        return True
    except Exception as e:
        print(f"[HAF ERROR] {e}")
        return False

async def _hgm(uid, p):
    try:
        d = _db64(p)
        if not d:
            await _sm(uid, "❌ Invalid media payload.")
            return True
        mt, _, m = d.partition("&")
        mm = {"photo": pbot.send_photo, "video": pbot.send_video, "animation": pbot.send_animation, "audio": pbot.send_audio}
        me = mm.get(mt)
        if me: await me(uid, m)
        else: await _sm(uid, "❌ Unsupported media type.")
        return True
    except Exception as e:
        print(f"[HGM ERROR] {e}")
        return False

async def _hgf(uid, t, p):
    try:
        d = _db64(p)
        if not d:
            await _sm(uid, "❌ Invalid file payload.")
            return True
        ms, _, uq = d.partition("&")
        if not ms.isdigit():
            await _sm(uid, "**This file is removed or an invalid link!** 👻")
            return True
        mid = int(ms)
        fm = await pbot.get_messages(config.FILE_DB_CHANNEL, mid)
        if not fm:
            await _sm(uid, "**This file is removed or an invalid link!** 👻")
            return True
        fuid = None
        for at in ("document", "video", "sticker", "photo", "animation", "audio"):
            it = getattr(fm, at, None)
            if it:
                fuid = getattr(it[-1] if at == "photo" and isinstance(it, list) else it, "file_unique_id", None)
                if fuid: break
        if uq != fuid:
            await _sm(uid, "**This is not a valid link!** 🤨")
            return True
        m = await _cm(config.UPDATE_CHANNEL, uid)
        if not m:
            b = InlineKeyboardMarkup([[InlineKeyboardButton(font("⚡ Channel"), url=f"t.me/{config.UPDATE_CHANNEL.lstrip('@')}", style=ButtonStyle.PRIMARY)], [InlineKeyboardButton(font("⚡ Try again"), url=f"t.me/{BOT_UN}?start={t}", style=ButtonStyle.SUCCESS)]])
            await _sm(uid, "**In order to access file please Join my channel!**", b)
            return True
        fw = await fm.forward(uid)
        aft = getattr(config, "AF_FILE_DEL_TIME", 60)
        await fw.reply_text(f"✨ **Thank you for using me!**\n```\nThe file will be deleted after {time_formatter(aft)}, so please save it somewhere else like Saved Messages!```")
        asyncio.create_task(auto_delete(fw, aft))
        return True
    except Exception as e:
        print(f"[HGF ERROR] {e}")
        return False

async def _hr(uid, cid):
    try:
        ci = int(cid)
        rt = await get_rules(ci) or DEFAULT_MESSAGE
        cn = await _gcn(ci)
        await _sm(uid, f"📜 **Rules for {cn}:**\n\n{rt}", None, random.choice(SE))
        return True
    except Exception as e:
        print(f"[HR ERROR] {e}")
        await _sm(uid, "❌ Invalid chat rules link.")
        return False

async def _hh(uid, msg, u):
    try:
        b = await get_help_button(msg, u)
        if not b:
            await _sm(uid, "Help menu not available.")
            return False
        hc = f"━━━━━━━━━━━━━━━━━━━\n<blockquote><b>{font('Hii')} {u.mention}!\n\n{font('Need help or want to support us?')}\n\n{font('Main available commands:')}\n- /support : {font('Connect with our support.')}\n- /alive : {font('Check uptime')}\n- /donate : {font('For information about donations!')}\n- /privacy : {font('Learn how we protect your privacy.')}\n- {font('In a group: Get your group settings.')}</b></blockquote>\n━━━━━━━━━━━━━━━━━━━"
        await _sp(uid, config.HELP_CMD_IMG, hc, b, random.choice(SE))
        return True
    except Exception as e:
        print(f"[HH ERROR] {e}")
        await _sm(uid, "Error loading help menu.")
        return False

async def _dl(msg):
    a = msg.text.split(None, 1)
    if len(a) < 2: return False
    t = a[1]
    uid = msg.from_user.id
    try:
        if t.startswith('manage_'): return await handle_admin_deeplink(msg, t)
        if t.startswith('settings_'): return await handle_settings_deeplink(msg, t)
        if t.startswith('afFile'):
            _, _, p = t.partition('-')
            return await _haf(uid, t, p) if p else False
        if t.startswith(('music', 'sud', 'inf')): return True
        if t.startswith("getmedia"):
            _, _, p = t.partition('-')
            return await _hgm(uid, p) if p else False
        if t.startswith("getfile"):
            _, _, p = t.partition('-')
            return await _hgf(uid, t, p) if p else False
        if t.startswith("rules_"):
            _, _, c = t.partition("_")
            return await _hr(uid, c)
        if t.startswith('help'): return await _hh(uid, msg, msg.from_user)
        await _sm(uid, "**No deep message detected for this link 🤷**")
        return False
    except Exception as e:
        print(f"[DL ERROR] {e}")
        await _sm(uid, "❌ Error processing request")
        return False

async def _bst(uid, u, cmd):
    try:
        us = await get_user_join_source(uid)
        w = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        un = f"@{u.username}" if u.username else "No Username"
        if us == 'new':
            lt = f"⚡ **New User Started Bot**\n\n**👤 User:** {u.mention}\n**🆔 ID:** `{uid}`\n**🔗 Username:** {un}\n**📍 Source:** Direct DM\n**🔧 Command:** `/{cmd}`\n**📅 Date:** `{w}`"
        elif us == 'group_inactive':
            lt = f"⚡ **New Active User**\n\n**👤 User:** {u.mention}\n**🆔 ID:** `{uid}`\n**🔗 Username:** {un}\n**📍 Source:** Group → DM\n**🔧 Command:** `/{cmd}`\n**📅 Date:** `{w}`"
        else:
            lt = f"⚡ **User Started Bot**\n\n**👤 User:** {u.mention}\n**🆔 ID:** `{uid}`\n**🔗 Username:** {un}\n**📍 Source:** Returning User\n**🔧 Command:** `/{cmd}`\n**📅 Date:** `{w}`"
        if LOGS_CHANNEL:
            asyncio.create_task(pbot.send_message(LOGS_CHANNEL, lt))
    except Exception as e:
        print(f"[BST ERROR] {e}")

async def _bst_group(chat, u, cmd):
    try:
        w = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        un = f"@{u.username}" if u.username else "No Username"
        lt = f"⚡ **Bot Started in Group**\n\n**👥 Group:** {chat.title}\n**🆔 Chat ID:** `{chat.id}`\n**👤 Started By:** {u.mention}\n**🆔 User ID:** `{u.id}`\n**🔗 Username:** {un}\n**🔧 Command:** `/{cmd}`\n**📅 Date:** `{w}`"
        if LOGS_CHANNEL:
            asyncio.create_task(pbot.send_message(LOGS_CHANNEL, lt))
    except Exception as e:
        print(f"[BST_GROUP ERROR] {e}")

async def _process_start_queue():
    while True:
        try:
            msg = await _user_queue.get()
            if msg is None:
                break
            
            uid = msg.from_user.id
            
            if uid in _processing_users:
                _user_queue.task_done()
                continue
            
            ct = time.time()
            lpt = _last_process_time.get(uid, 0)
            if ct - lpt < 2:
                await asyncio.sleep(2 - (ct - lpt))
            
            _processing_users.add(uid)
            
            async with _semaphore:
                try:
                    await _handle_start_private(msg)
                except Exception as e:
                    print(f"[QUEUE PROCESS ERROR] {e}")
                finally:
                    _last_process_time[uid] = time.time()
                    await asyncio.sleep(0.1)
            
            _processing_users.discard(uid)
            _user_queue.task_done()
            
        except Exception as e:
            print(f"[QUEUE ERROR] {e}")
            await asyncio.sleep(1)

async def _ensure_queue_processor():
    global _queue_processor_started
    if not _queue_processor_started:
        _queue_processor_started = True
        asyncio.create_task(_process_start_queue())

async def _handle_start_private(message: Message):
    try:
        u = message.from_user
        if not u: return
        
        if message.text and len(message.text.split()) > 1:
            if await _dl(message): return
        
        uid = u.id
        ct = time.time()
        
        is_cached = 'start' in _cmd_cache[uid] and (ct - _cmd_cache[uid]['start']) < _cache_expiry
        
        if not is_cached:
            _cmd_cache[uid]['start'] = ct
            
            join_source = await get_user_join_source(uid)
            
            asyncio.create_task(_bst(uid, u, 'start'))
            asyncio.create_task(add_user({'id': uid, 'first_name': u.first_name, 'last_name': u.last_name, 'username': u.username, 'is_bot': u.is_bot}))
            
            if join_source in ['new', 'group_inactive']:
                asyncio.create_task(activate_user(uid))
        
        bi = _gbi()
        bm = f"[{bi.first_name}](tg://user?id={bi.id})" if bi else "I"
        b = _gsb(uid)
        tx = f"<blockquote><b>⍣ 𝖧𝖾𝗒𝖺 {u.mention} {bm} 𝖨'𝗆 𝖠𝗇 𝖠𝖽𝗏𝖺𝗇𝖼𝖾 𝖠𝖨 𝖨𝗇𝗍𝖾𝗀𝗋𝖺𝗍𝖾𝖽 𝖱𝗈𝖻𝗈𝗍, 𝖨'𝗅𝗅 𝖬𝖺𝗇𝖺𝗀𝖾 𝖸𝗈𝗎𝗋 𝖦𝗋𝗈𝗎𝗉 𝖤𝖺𝗌𝗂𝗅𝗒.</b></blockquote>\n──────────────────────\n<blockquote><b>➛ 70+ 𝖬𝗎𝗅𝗍𝗂𝗉𝗅𝖾 𝖥𝖾𝖺𝗍𝗎𝗋𝖾𝗌 𝖶𝗂𝗍𝗁 𝖠𝗂\n➛ E𝖺𝗌𝗒 𝖳𝗈 𝖴𝗌𝖾, 𝖠𝗅𝗅 𝖨𝗇 𝖮𝗇𝖾 𝖡𝗈𝗍\n➛ 𝖲𝖺𝖿𝖾𝗌𝗍 𝖦𝗋𝗈𝗎𝗉 𝖬𝖺𝗇𝖺𝗀𝖾𝗆𝖾𝗇𝗍 𝖡𝗈𝗍</b></blockquote>\n──────────────────────\n<blockquote><b>⍣ 𝖧𝗂𝗍 𝖳𝗁𝖾 /help 𝖡𝗎𝗍𝗍𝗈𝗇 𝖳𝗈 𝖪𝗇𝗈𝗐 𝖬𝗒 𝖠𝖻𝗂𝗅𝗂𝗍𝗂𝖾𝗌</b></blockquote>"
        await _sp(cid=message.chat.id, p=getattr(config, "PM_START_IMG", None), c=tx, rm=b, eid=random.choice(SE))
    except Exception as e:
        print(f"[HANDLE_START_PRIVATE ERROR] {e}")
        try: await _sm(message.chat.id, "Hello — something went wrong while showing start. Check logs.")
        except Exception as e2: print(f"[HANDLE_START_PRIVATE FALLBACK ERROR] {e2}")

@pbot.on_message(filters.command('start', prefixes=prefix_cmds) & filters.private, group=11)
async def start_private(client: Client, message: Message):
    try:
        asyncio.create_task(_st())
        await _ensure_queue_processor()
        await _user_queue.put(message)
    except Exception as e:
        print(f"[START_PRIVATE ERROR] {e}")
        try: await _sm(message.chat.id, "Please try again in a moment.")
        except Exception as e2: print(f"[START_PRIVATE FALLBACK ERROR] {e2}")

@pbot.on_message(filters.command('start', prefixes=prefix_cmds) & filters.group, group=12)
async def start_group(client: Client, message: Message):
    try:
        asyncio.create_task(_st())
        asyncio.create_task(check_chat_exists(message.chat.id))
        asyncio.create_task(_bst_group(message.chat, message.from_user, 'start'))
        await _sp(cid=message.chat.id, p=getattr(config, "START_IMG", None), c="👋 Hello everyone! Toji here.\nI'm ready to manage and protect this group. Type /help to see what I can do!")
    except Exception as e:
        print(f"[START_GROUP ERROR] {e}")

@pbot.on_message(filters.command('help', prefixes=prefix_cmds) & filters.private, group=13)
async def help_private(client: Client, message: Message):
    try:
        u = message.from_user
        if not u: return
        
        uid = u.id
        ct = time.time()
        
        t = message.text or ""
        p = t.split(maxsplit=1)
        if len(p) == 2:
            hm = p[1].strip().lower()
            if hm in MODULE:
                r = message.reply_to_message
                await _sp(cid=message.chat.id, p=getattr(config, "HELP_MODULE_IMG", None), c=MODULE[hm], rm=None, eid=random.choice(SE), rt=r.id if r else message.id)
                return
        
        is_cached = 'help' in _cmd_cache[uid] and (ct - _cmd_cache[uid]['help']) < _cache_expiry
        
        if not is_cached:
            _cmd_cache[uid]['help'] = ct
            
            join_source = await get_user_join_source(uid)
            
            asyncio.create_task(_bst(uid, u, 'help'))
            asyncio.create_task(add_user({'id': uid, 'first_name': u.first_name, 'last_name': u.last_name, 'username': u.username, 'is_bot': u.is_bot}))
            
            if join_source in ['new', 'group_inactive']:
                asyncio.create_task(activate_user(uid))
        
        b = await get_help_button(message, u)
        if not b:
            await _sm(message.chat.id, "Help buttons not available.")
            return
        ht = f"━━━━━━━━━━━━━━━━━━━\n<blockquote><b>{font('Hii')} {u.mention}!\n\n{font('Need help or want to support us?')}\n\n{font('Main available commands:')}\n- /support : {font('Connect with our support.')}\n- /alive : {font('Check uptime')}\n- /donate : {font('For information about donations!')}\n- /privacy : {font('Learn how we protect your privacy.')}\n- {font('In a group: Get your group settings.')}</b></blockquote>\n━━━━━━━━━━━━━━━━━━━"
        await _sp(cid=message.chat.id, p=getattr(config, "HELP_CMD_IMG", None), c=ht, rm=b, eid=random.choice(SE))
    except Exception as e:
        print(f"[HELP_PRIVATE ERROR] {e}")
        await _sm(message.chat.id, "Error loading help menu.")

@pbot.on_message(filters.command('help', prefixes=prefix_cmds) & filters.group, group=14)
@no_channel
async def help_group(client: Client, message: Message):
    try:
        if message.from_user and message.from_user.is_bot: return
        await message.reply_text(font("🧏 Check my all commands in private chat!"), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(font("🆘 Commands"), url=f"t.me/{BOT_UN}?start=help", style=ButtonStyle.SUCCESS)]]))
    except Exception as e:
        print(f"[HELP_GROUP ERROR] {e}")

PT = "🗨️ **Privacy Policy:**\n\nWe care about your privacy.\n\n📥 **Data Collection:**\nWe only collect your unique Telegram User ID, necessary for our bot to function properly.\n\n🔎 **Data Use:**\nWe don't share your User ID with third-party apps or services. Data is only used to support bot features such as preferences, command usage, and permission checks.\n\n🧾 **Logs & Storage:**\nWe may store minimal logs (timestamps and user IDs) for moderation and abuse prevention. Files you upload are not shared and can be deleted on request.\n\n🙋 **Your Rights:**\nYou have the right to request access, correction, or deletion of your data. To request this, contact the support chat below or use bot commands if available.\n\n🔒 **Security:**\nWe take reasonable measures to protect data, but no system is 100% secure. Please avoid sending sensitive personal information.\n\n🤷 **Changes to this Policy:**\nWe may update this policy. By using our bot, you agree to this policy. If major changes are made, we will notify users in the update channel."
DT = f"🙏 **Thank you for considering donating to help keep this Bot alive and functioning!**\n\n⚫ You can also donate telegram stars using /pay command.\n\n🙋 Please reach out at {SUPPORT_CHAT}."
ST = f"👮 **Click below buttons to reach out to the official Support for** {BOT_USERNAME}**.**"

@pbot.on_message(filters.command('support', prefixes=prefix_cmds) & filters.private, group=-310)
@no_channel
async def support_cmd(client: Client, message: Message):
    try:
        u = message.from_user
        if not u: return
        
        uid = u.id
        ct = time.time()
        
        is_cached = 'support' in _cmd_cache[uid] and (ct - _cmd_cache[uid]['support']) < _cache_expiry
        
        if not is_cached:
            _cmd_cache[uid]['support'] = ct
            
            join_source = await get_user_join_source(uid)
            
            asyncio.create_task(_bst(uid, u, 'support'))
            asyncio.create_task(add_user({'id': uid, 'first_name': u.first_name, 'last_name': u.last_name, 'username': u.username, 'is_bot': u.is_bot}))
            
            if join_source in ['new', 'group_inactive']:
                asyncio.create_task(activate_user(uid))
        
        await _sm(message.chat.id, ST, SB, random.choice(SE))
    except Exception as e: print(f"[SUPPORT_CMD ERROR] {e}")

@pbot.on_message(filters.command('donate', prefixes=prefix_cmds) & filters.private, group=15)
@no_channel
async def donate_cmd(client: Client, message: Message):
    try:
        u = message.from_user
        if not u: return
        
        uid = u.id
        ct = time.time()
        
        is_cached = 'donate' in _cmd_cache[uid] and (ct - _cmd_cache[uid]['donate']) < _cache_expiry
        
        if not is_cached:
            _cmd_cache[uid]['donate'] = ct
            
            join_source = await get_user_join_source(uid)
            
            asyncio.create_task(_bst(uid, u, 'donate'))
            asyncio.create_task(add_user({'id': uid, 'first_name': u.first_name, 'last_name': u.last_name, 'username': u.username, 'is_bot': u.is_bot}))
            
            if join_source in ['new', 'group_inactive']:
                asyncio.create_task(activate_user(uid))
        
        await _sm(message.chat.id, DT, None, random.choice(SE))
    except Exception as e: print(f"[DONATE_CMD ERROR] {e}")

@pbot.on_message(filters.command('privacy', prefixes=prefix_cmds) & filters.private, group=16)
@no_channel
async def privacy_cmd(client: Client, message: Message):
    try:
        u = message.from_user
        if not u: return
        
        uid = u.id
        ct = time.time()
        
        is_cached = 'privacy' in _cmd_cache[uid] and (ct - _cmd_cache[uid]['privacy']) < _cache_expiry
        
        if not is_cached:
            _cmd_cache[uid]['privacy'] = ct
            
            join_source = await get_user_join_source(uid)
            
            asyncio.create_task(_bst(uid, u, 'privacy'))
            asyncio.create_task(add_user({'id': uid, 'first_name': u.first_name, 'last_name': u.last_name, 'username': u.username, 'is_bot': u.is_bot}))
            
            if join_source in ['new', 'group_inactive']:
                asyncio.create_task(activate_user(uid))
        
        await _sm(message.chat.id, PT, None, random.choice(SE))
    except Exception as e: print(f"[PRIVACY_CMD ERROR] {e}")
