from AloneX import database2 as database
from datetime import datetime, timezone

# Existing collection: enabled chatbot chats
collection = database["chatbot"]

# New collection: AI conversation memory
memory_collection = database["chatbot_memory"]

CHAT_IDS = []


# ---------------- CHAT MANAGEMENT ----------------

async def count_chats() -> int:
    return await collection.count_documents({})


async def add_chat(chat_id):
    try:
        chat_id = int(chat_id)

        if not await collection.find_one({"chat_id": chat_id}):
            await collection.insert_one({"chat_id": chat_id})

        if chat_id not in CHAT_IDS:
            CHAT_IDS.append(chat_id)

    except Exception as e:
        print(f"Error adding chatbot chat: {e}")


async def remove_chat(chat_id):
    try:
        chat_id = int(chat_id)

        await collection.delete_one({"chat_id": chat_id})

        if chat_id in CHAT_IDS:
            CHAT_IDS.remove(chat_id)

    except Exception as e:
        print(f"Error removing chatbot chat: {e}")


async def get_all_chats():
    try:
        chats = await collection.find().to_list(length=None)
        return [chat["chat_id"] for chat in chats]

    except Exception as e:
        print(f"Error getting all chatbot chats: {e}")
        return []


async def initialize_db_chats():
    chats = await get_all_chats()
    CHAT_IDS.clear()
    CHAT_IDS.extend(chats)


async def reset_chat_chatbot(chat_id: int):
    chat_id = int(chat_id)

    await collection.delete_one({"chat_id": chat_id})

    if chat_id in CHAT_IDS:
        CHAT_IDS.remove(chat_id)


# ---------------- AI CHAT MEMORY ----------------

async def save_message(
    chat_id: int,
    user_id: int,
    role: str,
    content: str,
    user_name: str = None,
):
    try:
        if role not in ("user", "assistant"):
            return False

        content = str(content).strip()
        if not content:
            return False

        await memory_collection.insert_one({
            "chat_id": int(chat_id),
            "user_id": int(user_id),
            "role": role,
            "content": content,
            "user_name": user_name,
            "timestamp": datetime.now(timezone.utc),
        })

        return True

    except Exception as e:
        print(f"Error saving chatbot memory: {e}")
        return False


async def get_history(
    chat_id: int,
    user_id: int,
    limit: int = 20,
):
    try:
        limit = max(1, min(int(limit), 100))

        cursor = (
            memory_collection
            .find({
                "chat_id": int(chat_id),
                "user_id": int(user_id),
            })
            .sort("_id", -1)
            .limit(limit)
        )

        messages = await cursor.to_list(length=limit)
        messages.reverse()

        return [
            {
                "role": item["role"],
                "content": item["content"],
            }
            for item in messages
        ]

    except Exception as e:
        print(f"Error loading chatbot memory: {e}")
        return []


async def get_user_memory(user_id: int, limit: int = 30):
    try:
        limit = max(1, min(int(limit), 100))

        cursor = (
            memory_collection
            .find({"user_id": int(user_id)})
            .sort("_id", -1)
            .limit(limit)
        )

        messages = await cursor.to_list(length=limit)
        messages.reverse()
        return messages

    except Exception as e:
        print(f"Error getting user memory: {e}")
        return []


async def clear_history(chat_id: int, user_id: int):
    try:
        result = await memory_collection.delete_many({
            "chat_id": int(chat_id),
            "user_id": int(user_id),
        })
        return result.deleted_count

    except Exception as e:
        print(f"Error clearing chatbot history: {e}")
        return 0


async def clear_all_user_memory(user_id: int):
    try:
        result = await memory_collection.delete_many({
            "user_id": int(user_id),
        })
        return result.deleted_count

    except Exception as e:
        print(f"Error clearing user memory: {e}")
        return 0


async def count_messages(chat_id: int = None):
    query = {}

    if chat_id is not None:
        query["chat_id"] = int(chat_id)

    return await memory_collection.count_documents(query)


async def initialize_memory_db():
    try:
        await memory_collection.create_index([
            ("chat_id", 1),
            ("user_id", 1),
            ("_id", -1),
        ])

        await memory_collection.create_index("user_id")

    except Exception as e:
        print(f"Error initializing chatbot memory: {e}")
