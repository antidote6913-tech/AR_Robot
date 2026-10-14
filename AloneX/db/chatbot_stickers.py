"""
AloneX/db/chatbot_stickers.py

Persistent sticker storage for the chatbot (MongoDB, async / Motor style).

* One document per unique sticker, deduplicated by `file_unique_id`
  (unique index + atomic upsert, so concurrent saves can't create duplicates).
* Every sticker carries `moods` tags (happy, sad, excited, ...). Tags are
  inferred from the sticker emoji and can be extended by the owner manually.
* Only Telegram `file_id`s are stored. Nothing is downloaded, and no secrets
  are ever written to the documents.

Uses the same connection as AloneX/db/chatbot.py (`AloneX.database2`).
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from pymongo.errors import DuplicateKeyError

from AloneX import database2 as database

log = logging.getLogger("AloneX.chatbot.stickers")

# Collection: one document per unique sticker
sticker_collection = database["chatbot_stickers"]

# Results returned by save_sticker()
SAVED = "saved"      # a brand-new sticker was stored
EXISTS = "exists"    # the sticker was already in the collection
INVALID = "invalid"  # missing file_id / file_unique_id
ERROR = "error"      # database failure

MOODS = (
    "happy", "sad", "excited", "angry", "confused",
    "playful", "affectionate", "supportive", "neutral",
)

# emoji -> mood tags (a sticker emoji may match several moods)
_MOOD_EMOJIS: Dict[str, str] = {
    "happy": "😀😃😄😁😊🙂😌😇🌞👍☺🌸",
    "excited": "🤩🥳🎉🎊🔥🙌💃🕺🤯⚡🚀😆🏆",
    "sad": "😢😭😞😔🥺😿💔😥😓😪☹🙁😩😫😟😰",
    "angry": "😡😠🤬👿💢😤",
    "confused": "😕🤔😵🤨❓❔🤷😳🫤🧐😶😧",
    "playful": "😂🤣😜😝😛😏😎😈🤪😹🙃😋😉😅👀🤭😬",
    "affectionate": "❤💕💖💗💘💝😍🥰😘😚😻🤗💋💞💓😙💜🧡💛",
    "supportive": "🤗🫂💪🙏👏🤝💙💚🌈🫶👌",
    "neutral": "😐😑👋🫡🙄😴🥱",
}


def _strip_vs(text: str) -> str:
    return text.replace("\ufe0f", "")


_EMOJI_PAIRS: List[tuple] = []
for _mood, _chars in _MOOD_EMOJIS.items():
    for _ch in _strip_vs(_chars):
        if _ch.strip():
            _EMOJI_PAIRS.append((_ch, _mood))


def moods_from_emoji(text: Optional[str]) -> List[str]:
    """Moods suggested by the emoji found in `text` (ordered, no duplicates)."""
    if not text:
        return []
    cleaned = _strip_vs(str(text))
    found: List[str] = []
    for ch, mood in _EMOJI_PAIRS:
        if ch in cleaned and mood not in found:
            found.append(mood)
    return found


def normalize_moods(moods: Optional[Iterable[str]]) -> List[str]:
    """Lower-case, keep only known moods, drop duplicates (order preserved)."""
    out: List[str] = []
    for m in moods or []:
        m = str(m).strip().lower()
        if m in MOODS and m not in out:
            out.append(m)
    return out


def _clean_text(value, limit: int) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value[:limit] if value else None


# ---------------- INDEXES ----------------

_indexes_ready = False


async def initialize_sticker_db() -> bool:
    """Create indexes (idempotent). Called lazily, so calling it at startup is optional."""
    global _indexes_ready
    if _indexes_ready:
        return True
    try:
        await sticker_collection.create_index("file_unique_id", unique=True)
        await sticker_collection.create_index("moods")
        _indexes_ready = True
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Sticker index creation failed: %s", type(e).__name__)
        return False


# ---------------- WRITE ----------------

async def save_sticker(
    file_id: str,
    file_unique_id: str,
    emoji: Optional[str] = None,
    set_name: Optional[str] = None,
    chat_id: Optional[int] = None,
    user_id: Optional[int] = None,
    moods: Optional[Sequence[str]] = None,
    owner_saved: bool = False,
    is_animated: bool = False,
    is_video: bool = False,
) -> str:
    """
    Save a sticker with an atomic upsert keyed on `file_unique_id`.

    Returns SAVED (new), EXISTS (already stored), INVALID or ERROR.
    For existing stickers the latest file_id / last_seen_at are refreshed and any
    new mood tags are added; source chat/user stay those of the first sighting.
    """
    fid = _clean_text(file_id, 512)
    fuid = _clean_text(file_unique_id, 128)
    if not fid or not fuid:
        return INVALID

    emoji = _clean_text(emoji, 32)
    set_name = _clean_text(set_name, 128)
    all_moods = normalize_moods(moods_from_emoji(emoji) + list(normalize_moods(moods)))
    now = datetime.now(timezone.utc)

    set_fields: Dict[str, object] = {"file_id": fid, "last_seen_at": now}
    if emoji:
        set_fields["emoji"] = emoji
    if set_name:
        set_fields["set_name"] = set_name
    if owner_saved:
        set_fields["owner_saved"] = True

    insert_fields: Dict[str, object] = {
        "created_at": now,
        "is_animated": bool(is_animated),
        "is_video": bool(is_video),
    }
    try:
        if chat_id is not None:
            insert_fields["source_chat_id"] = int(chat_id)
        if user_id is not None:
            insert_fields["source_user_id"] = int(user_id)
    except (TypeError, ValueError):
        pass

    update: Dict[str, object] = {"$set": set_fields, "$inc": {"seen_count": 1}}
    if all_moods:
        update["$addToSet"] = {"moods": {"$each": all_moods}}
    else:
        insert_fields["moods"] = ["neutral"]  # unknown emoji -> neutral, only on first insert
    update["$setOnInsert"] = insert_fields

    await initialize_sticker_db()
    for _ in range(2):
        try:
            res = await sticker_collection.update_one({"file_unique_id": fuid}, update, upsert=True)
            return SAVED if res.upserted_id is not None else EXISTS
        except DuplicateKeyError:
            continue  # lost an upsert race: the second pass updates the winner's document
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("Error saving sticker: %s", type(e).__name__)
            return ERROR
    return EXISTS


async def remove_sticker(file_unique_id: str) -> bool:
    """Delete a sticker (used when Telegram reports its file_id as invalid)."""
    fuid = _clean_text(file_unique_id, 128)
    if not fuid:
        return False
    try:
        res = await sticker_collection.delete_one({"file_unique_id": fuid})
        return res.deleted_count > 0
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error removing sticker: %s", type(e).__name__)
        return False


# ---------------- READ ----------------

_PROJECTION = {"_id": 0, "file_id": 1, "file_unique_id": 1, "emoji": 1, "set_name": 1, "moods": 1}


async def _sample_one(query: dict) -> Optional[dict]:
    pipeline = [{"$match": query}, {"$sample": {"size": 1}}, {"$project": _PROJECTION}]
    docs = await sticker_collection.aggregate(pipeline).to_list(length=1)
    return docs[0] if docs else None


async def get_random_sticker(
    mood: Optional[str] = None,
    exclude: Optional[Iterable[str]] = None,
) -> Optional[dict]:
    """
    One random sticker, optionally of a given mood, skipping the `file_unique_id`s in `exclude`.
    Returns {"file_id", "file_unique_id", "emoji", "set_name", "moods"} or None.
    """
    query: Dict[str, object] = {}
    if mood:
        query["moods"] = str(mood).strip().lower()
    ex = [x for x in (exclude or []) if x]
    if ex:
        query["file_unique_id"] = {"$nin": ex}
    try:
        return await _sample_one(query)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error fetching random sticker: %s", type(e).__name__)
        return None


async def get_sticker_for_moods(
    moods: Sequence[str],
    exclude: Optional[Iterable[str]] = None,
) -> Optional[dict]:
    """Try the moods in order (best match first) and return the first sticker found, else None."""
    for mood in moods:
        doc = await get_random_sticker(mood, exclude)
        if doc:
            return doc
    return None


async def count_stickers(mood: Optional[str] = None) -> int:
    query: Dict[str, object] = {}
    if mood:
        query["moods"] = str(mood).strip().lower()
    try:
        return await sticker_collection.count_documents(query)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error counting stickers: %s", type(e).__name__)
        return 0


async def count_by_mood() -> Dict[str, int]:
    """{mood: number of stickers tagged with it} (a sticker may count under several moods)."""
    pipeline = [{"$unwind": "$moods"}, {"$group": {"_id": "$moods", "count": {"$sum": 1}}}]
    try:
        docs = await sticker_collection.aggregate(pipeline).to_list(length=None)
        return {d["_id"]: int(d["count"]) for d in docs}
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.error("Error counting stickers by mood: %s", type(e).__name__)
        return {}
