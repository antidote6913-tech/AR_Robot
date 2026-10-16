"""
AloneX/db/chatbot_profiles.py

Long-term memory for the chatbot (MongoDB, async / Motor style).

* `chatbot_profiles` collection: one document per person (and scope, see chatbot.py):
  who they are (name, facts), a running summary of earlier chats, where the last
  conversation stopped (`recent`, `last_topic`, `last_seen_at`), things they still
  have to report back on (`open_loops`, `left_note`) and a few flags.
* Raw message history is NOT stored here: it lives in `chatbot_memory`
  (AloneX/db/chatbot.py: save_message / get_history). This module only adds
  pruning / wiping helpers for that collection.

Uses the same connection as AloneX/db/chatbot.py (`AloneX.database2`).
"""
import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from AloneX import database2 as database
from AloneX.db.chatbot import memory_collection  # raw message log (db/chatbot.py)

log = logging.getLogger("AloneX.chatbot.profiles")

profile_collection = database["chatbot_profiles"]

_ALLOWED_FLAGS = ("gender", "flirt_off", "minor_hint", "left_note", "left_at")
_indexes_ready = False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clip(value, limit: int) -> str:
    return str(value).strip()[:limit]


async def initialize_profile_db() -> bool:
    """Create indexes (idempotent, called lazily)."""
    global _indexes_ready
    if _indexes_ready:
        return True
    try:
        await profile_collection.create_index("user_id")
        _indexes_ready = True
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Profile index creation failed: %s", type(e).__name__)
        return False


# ---------------- PROFILES ----------------

async def get_profile(key: str) -> Optional[dict]:
    try:
        return await profile_collection.find_one({"_id": str(key)})
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error loading profile: %s", type(e).__name__)
        return None


async def record_exchange(
    key: str,
    user_id: int,
    name: Optional[str] = None,
    username: Optional[str] = None,
    chat_id: Optional[int] = None,
    chat_type: Optional[str] = None,
    chat_title: Optional[str] = None,
    recent: Optional[Sequence[str]] = None,
) -> bool:
    """Upsert the profile after a finished exchange: last_seen, message count, where the chat stopped."""
    now = _now()
    set_fields: Dict[str, object] = {"last_seen_at": now}
    if name:
        set_fields["name"] = _clip(name, 64)
    if username:
        set_fields["username"] = _clip(username, 64)
    if chat_id is not None:
        set_fields["last_chat_id"] = int(chat_id)
    if chat_type:
        set_fields["last_chat_type"] = _clip(chat_type, 32)
    if chat_title:
        set_fields["last_chat_title"] = _clip(chat_title, 64)
    if recent is not None:
        set_fields["recent"] = [_clip(x, 200) for x in list(recent)[-8:]]
    update = {
        "$set": set_fields,
        "$inc": {"msg_count": 1},
        "$setOnInsert": {"user_id": int(user_id), "first_seen_at": now, "facts": [], "summary": ""},
    }
    await initialize_profile_db()
    try:
        await profile_collection.update_one({"_id": str(key)}, update, upsert=True)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error recording exchange: %s", type(e).__name__)
        return False


async def save_memory(
    key: str,
    summary: str,
    facts: Sequence[str],
    last_topic: str = "",
    open_loops: Optional[Sequence[str]] = None,
) -> bool:
    """Store the refreshed long-term notes (summary, facts, last topic, things the person still has to report back on)."""
    fields = {
        "summary": _clip(summary or "", 900),
        "facts": [_clip(f, 140) for f in list(facts)[:30] if str(f).strip()],
        "last_topic": _clip(last_topic or "", 140),
        "memory_updated_at": _now(),
    }
    if open_loops is not None:
        fields["open_loops"] = [_clip(x, 140) for x in list(open_loops)[:10] if str(x).strip()]
    try:
        await profile_collection.update_one({"_id": str(key)}, {"$set": fields}, upsert=False)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error saving memory notes: %s", type(e).__name__)
        return False


async def set_flags(key: str, user_id: int, **flags) -> bool:
    """Persist gender / flirt_off / minor_hint / left_note / left_at (only these fields are accepted; None clears)."""
    fields = {k: v for k, v in flags.items() if k in _ALLOWED_FLAGS}
    if not fields:
        return False
    update = {
        "$set": fields,
        "$setOnInsert": {"user_id": int(user_id), "first_seen_at": _now(), "facts": [], "summary": ""},
    }
    await initialize_profile_db()
    try:
        await profile_collection.update_one({"_id": str(key)}, update, upsert=True)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error saving profile flags: %s", type(e).__name__)
        return False


async def delete_user_profiles(user_id: int) -> int:
    """Delete every profile (all scopes) of one user. Used by /forgetme."""
    try:
        res = await profile_collection.delete_many({"user_id": int(user_id)})
        return res.deleted_count
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error deleting profiles: %s", type(e).__name__)
        return 0


async def count_profiles() -> int:
    try:
        return await profile_collection.count_documents({})
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error counting profiles: %s", type(e).__name__)
        return 0


# ---------------- RAW MESSAGE LOG HELPERS (collection owned by db/chatbot.py) ----------------

async def clear_chat_history(chat_id: int) -> int:
    """Delete the stored message log of a whole chat (used by /chatbot reset)."""
    try:
        res = await memory_collection.delete_many({"chat_id": int(chat_id)})
        return res.deleted_count
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error clearing chat history: %s", type(e).__name__)
        return 0


async def prune_history(chat_id: int, user_id: int, keep: int = 400) -> int:
    """Keep only the newest `keep` stored messages of one user in one chat."""
    query = {"chat_id": int(chat_id), "user_id": int(user_id)}
    try:
        docs: List[dict] = await (
            memory_collection.find(query, {"_id": 1}).sort("_id", -1).skip(int(keep)).limit(1).to_list(length=1)
        )
        if not docs:
            return 0
        res = await memory_collection.delete_many({**query, "_id": {"$lte": docs[0]["_id"]}})
        return res.deleted_count
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error pruning history: %s", type(e).__name__)
        return 0


async def search_history(chat_id: int, user_id: int, words: Sequence[str], limit: int = 4) -> List[dict]:
    """
    Older messages this person sent that mention any of `words` (newest first).
    Lets the bot remember things from long ago, not only from the last few messages.
    Returns [{"content": str, "timestamp": datetime or None}, ...].
    """
    words = [w for w in (str(x).strip() for x in words) if len(w) >= 3][:4]
    if not words:
        return []
    query = {
        "chat_id": int(chat_id),
        "user_id": int(user_id),
        "role": "user",
        "content": {"$regex": "|".join(re.escape(w) for w in words), "$options": "i"},
    }
    try:
        n = int(limit) + 6  # a few extra: the caller drops messages that are still in the recent context
        docs = await memory_collection.find(query, {"content": 1, "timestamp": 1}).sort("_id", -1).limit(n).to_list(length=n)
        return [{"content": str(d.get("content") or ""), "timestamp": d.get("timestamp")} for d in docs]
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error searching history: %s", type(e).__name__)
        return []
