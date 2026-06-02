import sys
import traceback
import os
import logging
import asyncio
import time
import bcrypt
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Set, Optional

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse

# Глобальный перехват ошибок
def global_exception_handler(exc_type, exc_value, exc_traceback):
    sys.stderr.write("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
    sys.stderr.flush()

sys.excepthook = global_exception_handler

logging.basicConfig(level=logging.INFO, stream=sys.stderr)
logger = logging.getLogger("burmalda")

logger.info("Starting Burmalda server...")

DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "BurmaldaOwner")

if not DATABASE_URL:
    logger.error("DATABASE_URL not set. Waiting 30 seconds...")
    for _ in range(30):
        time.sleep(1)
        DATABASE_URL = os.getenv("DATABASE_URL")
        if DATABASE_URL:
            break
    if not DATABASE_URL:
        logger.critical("DATABASE_URL still missing. Exiting.")
        sys.exit(1)

# In-memory state
active_connections: Dict[str, dict] = {}
rooms: Dict[str, dict] = {}
voice_rooms: Dict[str, Set[str]] = {}

rooms["general"] = {
    "name": "Общий чат",
    "messages": [],
    "counter": 1,
    "typing": set(),
    "creator": None
}
voice_rooms["general"] = set()


# ========== Вспомогательные функции ==========

def get_dm_other(rid: str, current_user: str):
    """Определяет второго участника DM. Поддерживает username с '_'."""
    if not rid.startswith("dm_"):
        return None
    rest = rid[3:]  # всё после "dm_"
    if rest == current_user:
        return None
    prefix = current_user + "_"
    if rest.startswith(prefix):
        return rest[len(prefix):]
    suffix = "_" + current_user
    if rest.endswith(suffix):
        return rest[:-len(suffix)]
    # На случай если ни один паттерн не подошёл — ищем первый '_'
    idx = rest.find("_")
    if idx != -1:
        a, b = rest[:idx], rest[idx+1:]
        return b if a == current_user else a
    return None


def get_online_count(rid: str) -> int:
    return sum(1 for u in active_connections.values() if u.get("room") == rid)


def get_online_list() -> List[str]:
    return list(active_connections.keys())


async def broadcast_to_room(room_id: str, msg: dict, exclude: str = None):
    data = json.dumps(msg, ensure_ascii=False)
    dead = []
    for u, info in list(active_connections.items()):
        if info.get("room") == room_id and u != exclude:
            try:
                await info["ws"].send_text(data)
            except:
                dead.append(u)
    for u in dead:
        active_connections.pop(u, None)


async def send_to_user(username: str, msg: dict):
    if username in active_connections:
        try:
            await active_connections[username]["ws"].send_text(json.dumps(msg, ensure_ascii=False))
        except:
            active_connections.pop(username, None)


async def broadcast_to_all_users(msg: dict):
    data = json.dumps(msg, ensure_ascii=False)
    dead = []
    for u, info in list(active_connections.items()):
        try:
            await info["ws"].send_text(data)
        except:
            dead.append(u)
    for u in dead:
        active_connections.pop(u, None)


async def log_action(pool, username: str, action: str):
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO user_logs (username, action) VALUES ($1, $2)", username, action)
    except:
        pass


async def get_all_profiles(pool) -> dict:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT username, color, emoji, bio, status, display_name, bg_id, owner_badge FROM profiles"
        )
    return {row["username"]: {
        "color": row["color"],
        "emoji": row["emoji"],
        "bio": row["bio"],
        "status": row["status"],
        "displayName": row["display_name"],
        "bgId": row["bg_id"],
        "ownerBadge": row["owner_badge"]
    } for row in rows}


# ========== Lifespan ==========

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Lifespan starting...")
    await asyncio.sleep(3)
    pool = None
    for attempt in range(5):
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            logger.info("Database pool created")
            break
        except Exception as e:
            logger.warning(f"DB attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2)
    if pool is None:
        raise RuntimeError("Could not connect to database")
    app.state.pool = pool

    async with pool.acquire() as conn:
        # Таблицы
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "  username TEXT PRIMARY KEY,"
            "  password_hash TEXT NOT NULL,"
            "  created_at TIMESTAMP DEFAULT NOW()"
            ")"
        )
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS profiles ("
            "  username TEXT PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,"
            "  color TEXT, emoji TEXT, bio TEXT, status TEXT DEFAULT 'online',"
            "  display_name TEXT, bg_id TEXT DEFAULT 'none',"
            "  owner_badge TEXT, updated_at TIMESTAMP DEFAULT NOW()"
            ")"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS messages ("
            "  id SERIAL PRIMARY KEY, room_id TEXT NOT NULL,"
            "  sender TEXT NOT NULL, text TEXT NOT NULL,"
            "  created_at TIMESTAMP DEFAULT NOW()"
            ")"
        )
        await conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to INTEGER")
        await conn.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited BOOLEAN DEFAULT FALSE")
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS user_logs ("
            "  id SERIAL PRIMARY KEY, username TEXT NOT NULL,"
            "  action TEXT NOT NULL, timestamp TIMESTAMP DEFAULT NOW()"
            ")"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks ("
            "  blocker TEXT NOT NULL, blocked TEXT NOT NULL,"
            "  PRIMARY KEY (blocker, blocked)"
            ")"
        )
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS rooms ("
            "  id TEXT PRIMARY KEY, name TEXT NOT NULL,"
            "  creator TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW()"
            ")"
        )

        # Загрузка комнат из БД
        saved_rooms = await conn.fetch("SELECT id, name, creator FROM rooms")
        for r in saved_rooms:
            rid = r["id"]
            if rid not in rooms:
                rooms[rid] = {
                    "name": r["name"],
                    "messages": [],
                    "counter": 1,
                    "typing": set(),
                    "creator": r["creator"]
                }
                voice_rooms[rid] = set()

        # Загрузка сообщений из БД
        for rid in list(rooms.keys()):
            rows = await conn.fetch(
                "SELECT id, sender, text, reply_to, edited, created_at "
                "FROM messages WHERE room_id = $1 ORDER BY created_at",
                rid
            )
            msgs = []
            max_id = 0
            for row in rows:
                msgs.append({
                    "id": row["id"],
                    "room_id": rid,
                    "sender": row["sender"],
                    "text": row["text"],
                    "reply_to": row["reply_to"],
                    "edited": row["edited"],
                    "timestamp": row["created_at"].isoformat(),
                    "reactions": {}
                })
                if row["id"] > max_id:
                    max_id = row["id"]
            rooms[rid]["messages"] = msgs
            rooms[rid]["counter"] = max_id + 1

    yield
    await app.state.pool.close()
    logger.info("Lifespan shutdown")


app = FastAPI(lifespan=lifespan)


# ========== Auth endpoints ==========

@app.post("/register")
async def register(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if not username or not password:
        return {"error": "Заполните все поля"}
    if len(username) < 2 or len(username) > 32:
        return {"error": "Ник: от 2 до 32 символов"}
    if len(password) < 8:
        return {"error": "Пароль минимум 8 символов"}
    async with app.state.pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", username)
        if exists:
            return {"error": "Имя уже занято"}
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        role = "owner" if username == OWNER_USERNAME else "user"
        await conn.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES ($1, $2, $3, NOW())",
            username, hashed, role
        )
        await conn.execute(
            "INSERT INTO profiles (username, color, emoji, status, bg_id) VALUES ($1, $2, $3, $4, $5)",
            username, None, None, "online", "none"
        )
    await log_action(app.state.pool, username, "register")
    return {"ok": True}


@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if not username or not password:
        return {"error": "Заполните все поля"}
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT password_hash, role FROM users WHERE username = $1", username)
    if not row or not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return {"error": "Неверный логин или пароль"}
    await log_action(app.state.pool, username, "login")
    return {"ok": True, "username": username, "role": row["role"]}


@app.get("/users")
async def get_users():
    online = get_online_list()
    return {"online": online, "count": len(online)}


@app.get("/rooms")
async def get_rooms():
    result = []
    for rid, rdata in rooms.items():
        if not rid.startswith("dm_"):
            result.append({
                "id": rid,
                "name": rdata["name"],
                "online": get_online_count(rid)
            })
    return {"rooms": result}


# ========== WebSocket ==========

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    # Проверка существования пользователя
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT role FROM users WHERE username = $1", username)
    if not user:
        await websocket.close(code=1008, reason="User not found")
        return
    role = user["role"]

    await websocket.accept()
    active_connections[username] = {"ws": websocket, "room": "general"}
    logger.info(f"WS connected: {username}")

    # Отправка инициализации
    profiles = await get_all_profiles(app.state.pool)
    await send_to_user(username, {
        "type": "init",
        "username": username,
        "role": role,
        "online_users": get_online_list(),
        "rooms": [{"id": rid, "name": rdata["name"]} for rid, rdata in rooms.items() if not rid.startswith("dm_")],
        "profiles": profiles
    })
    for msg in rooms["general"]["messages"][-50:]:
        await send_to_user(username, {"type": "history", "data": msg})
    await broadcast_to_room("general", {"type": "system", "text": f"✨ {username} присоединился"})
    await broadcast_to_all_users({"type": "online_update", "online_users": get_online_list()})

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                obj = json.loads(raw)
            except:
                continue

            t = obj.get("type", "")
            cur_room = active_connections.get(username, {}).get("room", "general")

            # ======= join_room =======
            if t == "join_room":
                new_room = obj.get("room_id", "general")
                if new_room not in rooms and not new_room.startswith("dm_"):
                    await send_to_user(username, {"type": "error", "text": "Комната не найдена"})
                    continue

                # Выход из голосового канала в старой комнате
                if cur_room in voice_rooms and username in voice_rooms.get(cur_room, set()):
                    voice_rooms[cur_room].discard(username)
                    await broadcast_to_room(cur_room, {
                        "type": "voice_update",
                        "room_id": cur_room,
                        "users": list(voice_rooms[cur_room])
                    })

                # Очистка typing в старой комнате
                if cur_room in rooms:
                    rooms[cur_room]["typing"].discard(username)
                    await broadcast_to_room(cur_room, {
                        "type": "typing",
                        "room_id": cur_room,
                        "users": list(rooms[cur_room]["typing"])
                    })

                active_connections[username]["room"] = new_room

                # Создание DM комнаты если её нет
                if new_room.startswith("dm_") and new_room not in rooms:
                    rooms[new_room] = {
                        "name": new_room,
                        "messages": [],
                        "counter": 1,
                        "typing": set(),
                        "creator": None
                    }
                    voice_rooms[new_room] = set()

                await send_to_user(username, {
                    "type": "room_joined",
                    "room_id": new_room,
                    "name": rooms[new_room].get("name", new_room)
                })

                for msg in rooms[new_room]["messages"][-50:]:
                    await send_to_user(username, {"type": "history", "data": msg})

                if new_room.startswith("dm_"):
                    other = get_dm_other(new_room, username)
                    if other:
                        await send_to_user(other, {
                            "type": "messages_read",
                            "room_id": new_room,
                            "by": username
                        })

            # ======= create_room =======
            elif t == "create_room":
                name = obj.get("name", "").strip()
                if not name or len(name) > 40:
                    await send_to_user(username, {"type": "error", "text": "Неверное название"})
                    continue
                rid = "room_" + uuid.uuid4().hex[:8]
                rooms[rid] = {
                    "name": name,
                    "messages": [],
                    "counter": 1,
                    "typing": set(),
                    "creator": username
                }
                voice_rooms[rid] = set()
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO rooms (id, name, creator, created_at) VALUES ($1, $2, $3, NOW())",
                            rid, name, username
                        )
                except Exception as e:
                    logger.warning(f"Room save failed: {e}")
                await broadcast_to_all_users({
                    "type": "room_created",
                    "room_id": rid,
                    "name": name,
                    "creator": username
                })

            # ======= delete_room =======
            elif t == "delete_room":
                rid = obj.get("room_id")
                if rid == "general" or (role != "owner" and rooms.get(rid, {}).get("creator") != username):
                    await send_to_user(username, {"type": "error", "text": "Нет прав"})
                    continue
                rooms.pop(rid, None)
                voice_rooms.pop(rid, None)
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute("DELETE FROM messages WHERE room_id = $1", rid)
                        await conn.execute("DELETE FROM rooms WHERE id = $1", rid)
                except:
                    pass
                await broadcast_to_all_users({"type": "room_deleted", "room_id": rid})

            # ======= text (send message) =======
            elif t == "text":
                text = obj.get("text", "").strip()
                if not text:
                    continue
                room = rooms.get(cur_room)
                if not room:
                    continue
                reply_to = obj.get("reply_to")

                try:
                    async with app.state.pool.acquire() as conn:
                        row = await conn.fetchrow(
                            "INSERT INTO messages (room_id, sender, text, reply_to) "
                            "VALUES ($1, $2, $3, $4) RETURNING id",
                            cur_room, username, text, reply_to
                        )
                        db_id = row["id"]
                except Exception as e:
                    logger.warning(f"DB insert failed: {e}")
                    await send_to_user(username, {"type": "error", "text": "Не удалось отправить"})
                    continue

                msg_data = {
                    "id": db_id,
                    "room_id": cur_room,
                    "sender": username,
                    "text": text,
                    "timestamp": datetime.now().isoformat(),
                    "reactions": {},
                    "edited": False
                }
                if reply_to:
                    msg_data["reply_to"] = reply_to

                room["messages"].append(msg_data)
                if len(room["messages"]) > 200:
                    room["messages"].pop(0)

                if cur_room.startswith("dm_"):
                    # В DM отправляем обоим напрямую
                    await send_to_user(username, {"type": "message", "data": msg_data})
                    other = get_dm_other(cur_room, username)
                    if other:
                        await send_to_user(other, {"type": "message", "data": msg_data})
                else:
                    await broadcast_to_room(cur_room, {"type": "message", "data": msg_data})

            # ======= typing =======
            elif t == "typing":
                rid = obj.get("room_id", cur_room)
                room = rooms.get(rid)
                if room:
                    if obj.get("typing"):
                        room["typing"].add(username)
                    else:
                        room["typing"].discard(username)
                    typing_list = list(room["typing"])
                    payload = {"type": "typing", "room_id": rid, "users": typing_list}
                    if rid.startswith("dm_"):
                        await send_to_user(username, payload)
                        other = get_dm_other(rid, username)
                        if other:
                            await send_to_user(other, payload)
                    else:
                        await broadcast_to_room(rid, payload)

            # ======= delete message =======
            elif t == "delete":
                msg_id = obj.get("msg_id")
                room = rooms.get(cur_room)
                if room:
                    for i, m in enumerate(room["messages"]):
                        if m["id"] == msg_id and (m["sender"] == username or role == "owner"):
                            del room["messages"][i]
                            try:
                                async with app.state.pool.acquire() as conn:
                                    await conn.execute("DELETE FROM messages WHERE id = $1", msg_id)
                            except:
                                pass
                            payload = {
                                "type": "delete",
                                "msg_id": msg_id,
                                "room_id": cur_room
                            }
                            if cur_room.startswith("dm_"):
                                await send_to_user(username, payload)
                                other = get_dm_other(cur_room, username)
                                if other:
                                    await send_to_user(other, payload)
                            else:
                                await broadcast_to_room(cur_room, payload)
                            break

            # ======= edit_message =======
            elif t == "edit_message":
                msg_id = obj.get("msg_id")
                new_text = obj.get("text", "").strip()
                room = rooms.get(cur_room)
                if room and new_text:
                    for m in room["messages"]:
                        if m["id"] == msg_id and m["sender"] == username:
                            m["text"] = new_text
                            m["edited"] = True
                            try:
                                async with app.state.pool.acquire() as conn:
                                    await conn.execute(
                                        "UPDATE messages SET text = $1, edited = TRUE WHERE id = $2",
                                        new_text, msg_id
                                    )
                            except:
                                pass
                            payload = {
                                "type": "message_edited",
                                "msg_id": msg_id,
                                "room_id": cur_room,
                                "text": new_text,
                                "edited": True
                            }
                            if cur_room.startswith("dm_"):
                                await send_to_user(username, payload)
                                other = get_dm_other(cur_room, username)
                                if other:
                                    await send_to_user(other, payload)
                            else:
                                await broadcast_to_room(cur_room, payload)
                            break

            # ======= react =======
            elif t == "react":
                msg_id = obj.get("msg_id")
                emoji = obj.get("emoji", "")
                room = rooms.get(cur_room)
                if room and emoji:
                    for m in room["messages"]:
                        if m["id"] == msg_id:
                            reactions = m.setdefault("reactions", {})
                            ulist = reactions.setdefault(emoji, [])
                            if username in ulist:
                                ulist.remove(username)
                                if not ulist:
                                    del reactions[emoji]
                            else:
                                ulist.append(username)
                            payload = {
                                "type": "update_reactions",
                                "msg_id": msg_id,
                                "room_id": cur_room,
                                "reactions": reactions
                            }
                            if cur_room.startswith("dm_"):
                                await send_to_user(username, payload)
                                other = get_dm_other(cur_room, username)
                                if other:
                                    await send_to_user(other, payload)
                            else:
                                await broadcast_to_room(cur_room, payload)
                            break

            # ======= update_profile =======
            elif t == "update_profile":
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO profiles (username, color, emoji, bio, status, display_name, bg_id, owner_badge)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            ON CONFLICT (username) DO UPDATE SET
                                color = EXCLUDED.color,
                                emoji = EXCLUDED.emoji,
                                bio = EXCLUDED.bio,
                                status = EXCLUDED.status,
                                display_name = EXCLUDED.display_name,
                                bg_id = EXCLUDED.bg_id,
                                owner_badge = EXCLUDED.owner_badge,
                                updated_at = NOW()
                        """,
                            username,
                            obj.get("color"),
                            obj.get("emoji"),
                            obj.get("bio"),
                            obj.get("status"),
                            obj.get("displayName"),
                            obj.get("bgId"),
                            obj.get("ownerBadge")
                        )
                    await broadcast_to_all_users({
                        "type": "profile_updated",
                        "username": username,
                        "profile": {
                            "color": obj.get("color"),
                            "emoji": obj.get("emoji"),
                            "bio": obj.get("bio"),
                            "status": obj.get("status"),
                            "displayName": obj.get("displayName"),
                            "bgId": obj.get("bgId"),
                            "ownerBadge": obj.get("ownerBadge")
                        }
                    })
                except Exception as e:
                    logger.warning(f"Profile update failed: {e}")
                    await send_to_user(username, {"type": "error", "text": "Не удалось сохранить профиль"})

            # ======= voice_join =======
            elif t == "voice_join":
                if cur_room not in voice_rooms:
                    voice_rooms[cur_room] = set()
                voice_rooms[cur_room].add(username)
                await broadcast_to_room(cur_room, {
                    "type": "voice_update",
                    "room_id": cur_room,
                    "users": list(voice_rooms[cur_room])
                })

            # ======= voice_leave =======
            elif t == "voice_leave":
                if cur_room in voice_rooms:
                    voice_rooms[cur_room].discard(username)
                    await broadcast_to_room(cur_room, {
                        "type": "voice_update",
                        "room_id": cur_room,
                        "users": list(voice_rooms[cur_room])
                    })

            # ======= call_* =======
            elif t in ("call_offer", "call_answer", "call_ice", "call_reject"):
                target = obj.get("target")
                payload = {**obj, "from": username}
                if target:
                    if target in active_connections:
                        await send_to_user(target, payload)
                    else:
                        await send_to_user(username, {
                            "type": "error",
                            "text": "Пользователь не в сети"
                        })
                else:
                    for u in voice_rooms.get(cur_room, set()):
                        if u != username:
                            await send_to_user(u, payload)

            # ======= mark_read =======
            elif t == "mark_read":
                rid = obj.get("room_id")
                if rid and rid.startswith("dm_"):
                    other = get_dm_other(rid, username)
                    if other:
                        await send_to_user(other, {
                            "type": "messages_read",
                            "room_id": rid,
                            "by": username
                        })

            # ======= get_room_members =======
            elif t == "get_room_members":
                rid = obj.get("room_id", cur_room)
                members = [
                    u for u, info in active_connections.items()
                    if info.get("room") == rid
                ]
                await send_to_user(username, {
                    "type": "room_members",
                    "room_id": rid,
                    "members": members
                })

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS error for {username}: {e}")
        logger.error(traceback.format_exc())
    finally:
        # Безопасное завершение — используем get() чтобы не упасть
        user_info = active_connections.get(username, {})
        cur_room = user_info.get("room", "general") if isinstance(user_info, dict) else "general"

        for vroom in voice_rooms.values():
            vroom.discard(username)
        for r in rooms.values():
            r["typing"].discard(username)

        active_connections.pop(username, None)
        await broadcast_to_room(cur_room, {
            "type": "system",
            "text": f"👋 {username} покинул чат"
        })
        await broadcast_to_all_users({
            "type": "online_update",
            "online_users": get_online_list()
        })
        try:
            await log_action(app.state.pool, username, "logout")
        except:
            pass
        logger.info(f"WS disconnected: {username}")


# ========== Serve index.html ==========

@app.get("/")
async def serve_index():
    base = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(base, "index.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        logger.error(f"index.html not found at {path}")
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)


logger.info("Server ready")
