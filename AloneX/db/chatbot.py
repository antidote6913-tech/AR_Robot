
from AloneX import database2 as database
from datetime import datetime, timezone

# Separate collection for AI chatbot memories
memory_collection = database["chatbot_memory"]


async def save_message(
    chat_id: int,
    user_id: int,
    role: str,
    content: str,
    user_name: str = None,
):
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


async def get_history(
    chat_id: int,
    user_id: int,
    limit: int = 20,
):
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


async def get_user_memory(user_id: int, limit: int = 30):
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


async def clear_history(chat_id: int, user_id: int):
    result = await memory_collection.delete_many({
        "chat_id": int(chat_id),
        "user_id": int(user_id),
    })
    return result.deleted_count


async def clear_all_user_memory(user_id: int):
    result = await memory_collection.delete_many({
        "user_id": int(user_id),
    })
    return result.deleted_count


async def count_messages(chat_id: int = None):
    query = {}

    if chat_id is not None:
        query["chat_id"] = int(chat_id)

    return await memory_collection.count_documents(query)


async def initialize_memory_db():
    await memory_collection.create_index([
        ("chat_id", 1),
        ("user_id", 1),
        ("_id", -1),
    ])
    await memory_collection.create_index("user_id")
