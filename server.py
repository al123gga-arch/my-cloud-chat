import os
import bcrypt
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Set, Optional

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "admin")  # захардкоди свой ник здесь или в env

# ===== In-memory state =====
# connections: {username: {"ws": WebSocket, "room": str}}
active_connections: Dict[str, dict] = {}
# rooms: {room_id: {"name": str, "messages": [...], "counter": int, "typing": set()}}
rooms: Dict[str, dict] = {
    "general": {"name": "Общий чат", "messages": [], "counter": 1, "typing": set(), "creator": None}
}
# voice: {room_id: set of usernames currently in voice}
voice_rooms: Dict[str, Set[str]] = {"general": set()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    logger.info("Connecting to database...")
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username      TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                role          TEXT NOT NULL DEFAULT 'user',
                created_at    TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         SERIAL PRIMARY KEY,
                room_id    TEXT NOT NULL DEFAULT 'general',
                sender     TEXT NOT NULL,
                text       TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_logs (
                id        SERIAL PRIMARY KEY,
                username  TEXT NOT NULL,
                action    TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS blocks (
                blocker   TEXT NOT NULL,
                blocked   TEXT NOT NULL,
                PRIMARY KEY (blocker, blocked)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rooms (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                creator    TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Load saved rooms from DB into memory
        saved_rooms = await conn.fetch("SELECT id, name, creator FROM rooms")
        for r in saved_rooms:
            if r["id"] not in rooms:
                rooms[r["id"]] = {"name": r["name"], "messages": [], "counter": 1, "typing": set(), "creator": r["creator"]}
                voice_rooms[r["id"]] = set()
    logger.info("Database ready.")
    yield
    logger.info("Closing database pool...")
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


# ===== Helpers =====
def dm_room_id(user1: str, user2: str) -> str:
    return "dm_" + "_".join(sorted([user1, user2]))

def get_online_count(room_id: str) -> int:
    return sum(1 for u in active_connections.values() if u.get("room") == room_id)

def get_online_list() -> List[str]:
    return list(active_connections.keys())

async def broadcast_to_room(room_id: str, message: dict, exclude: Optional[str] = None):
    data = json.dumps(message, ensure_ascii=False)
    dead = []
    for username, info in active_connections.items():
        if info.get("room") == room_id and username != exclude:
            try:
                await info["ws"].send_text(data)
            except Exception:
                dead.append(username)
    for u in dead:
        active_connections.pop(u, None)

async def send_to_user(username: str, message: dict):
    if username in active_connections:
        try:
            await active_connections[username]["ws"].send_text(
                json.dumps(message, ensure_ascii=False)
            )
        except Exception:
            pass

async def log_action(pool, username: str, action: str):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_logs (username, action) VALUES ($1, $2)", username, action
            )
    except Exception as e:
        logger.warning(f"log_action failed: {e}")


# ===== Auth =====
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
            "INSERT INTO users (username, password_hash, role) VALUES ($1, $2, $3)",
            username, hashed, role
        )
    await log_action(app.state.pool, username, "register")
    return {"ok": True}


@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if not username or not password:
        return {"error": "Заполните все поля"}
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT password_hash, role FROM users WHERE username = $1", username
        )
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
    return {
        "rooms": [
            {"id": rid, "name": rdata["name"], "online": get_online_count(rid)}
            for rid, rdata in rooms.items()
            if not rid.startswith("dm_")
        ]
    }


# ===== WebSocket =====
@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT role FROM users WHERE username = $1", username
        )
    if not user:
        await websocket.close(code=1008)
        return

    role = user["role"]
    await websocket.accept()
    active_connections[username] = {"ws": websocket, "room": "general"}
    logger.info(f"WS connected: {username} (role={role})")

    # Send initial state
    await send_to_user(username, {
        "type": "init",
        "username": username,
        "role": role,
        "online_users": get_online_list(),
        "rooms": [
            {"id": rid, "name": rdata["name"]}
            for rid, rdata in rooms.items()
            if not rid.startswith("dm_")
        ]
    })

    # Send history of general
    for msg in rooms["general"]["messages"][-50:]:
        await send_to_user(username, {"type": "history", "data": msg})

    # Announce join
    await broadcast_to_room("general", {"type": "system", "text": f"✨ {username} присоединился"})
    await broadcast_to_all_users({"type": "online_update", "online_users": get_online_list()})

    try:
        while True:
            raw = await websocket.receive_text()
            try:
                obj = json.loads(raw)
            except Exception:
                continue

            t = obj.get("type", "")
            current_room = active_connections[username]["room"]

            # --- Switch room ---
            if t == "join_room":
                new_room = obj.get("room_id", "general")
                if new_room not in rooms and not new_room.startswith("dm_"):
                    await send_to_user(username, {"type": "error", "text": "Комната не найдена"})
                    continue
                # Leave voice if was in voice
                if current_room in voice_rooms and username in voice_rooms[current_room]:
                    voice_rooms[current_room].discard(username)
                    await broadcast_to_room(current_room, {
                        "type": "voice_update",
                        "room_id": current_room,
                        "users": list(voice_rooms[current_room])
                    })
                # Leave typing
                if current_room in rooms:
                    rooms[current_room]["typing"].discard(username)
                    await broadcast_to_room(current_room, {
                        "type": "typing",
                        "users": list(rooms[current_room]["typing"])
                    })

                active_connections[username]["room"] = new_room

                # Create DM room in memory if needed
                if new_room.startswith("dm_") and new_room not in rooms:
                    rooms[new_room] = {"name": new_room, "messages": [], "counter": 1, "typing": set(), "creator": None}
                    voice_rooms[new_room] = set()

                # Send room history
                room_msgs = rooms.get(new_room, {}).get("messages", [])
                await send_to_user(username, {"type": "room_joined", "room_id": new_room, "name": rooms.get(new_room, {}).get("name", new_room)})
                for msg in room_msgs[-50:]:
                    await send_to_user(username, {"type": "history", "data": msg})

            # --- Create room ---
            elif t == "create_room":
                room_name = str(obj.get("name", "")).strip()
                if not room_name or len(room_name) > 40:
                    await send_to_user(username, {"type": "error", "text": "Неверное название комнаты"})
                    continue
                import uuid
                room_id = "room_" + uuid.uuid4().hex[:8]
                rooms[room_id] = {"name": room_name, "messages": [], "counter": 1, "typing": set(), "creator": username}
                voice_rooms[room_id] = set()
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO rooms (id, name, creator) VALUES ($1, $2, $3)",
                            room_id, room_name, username
                        )
                except Exception as e:
                    logger.warning(f"Failed to save room: {e}")
                await broadcast_to_all_users({
                    "type": "room_created",
                    "room_id": room_id,
                    "name": room_name,
                    "creator": username
                })

            # --- Delete room (owner only) ---
            elif t == "delete_room":
                room_id = obj.get("room_id")
                if room_id == "general":
                    await send_to_user(username, {"type": "error", "text": "Нельзя удалить общий чат"})
                    continue
                if role != "owner" and rooms.get(room_id, {}).get("creator") != username:
                    await send_to_user(username, {"type": "error", "text": "Нет прав"})
                    continue
                rooms.pop(room_id, None)
                voice_rooms.pop(room_id, None)
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute("DELETE FROM rooms WHERE id = $1", room_id)
                except Exception:
                    pass
                await broadcast_to_all_users({"type": "room_deleted", "room_id": room_id})

            # --- Text message ---
            elif t == "text":
                text = str(obj.get("text", "")).strip()
                if not text:
                    continue
                room = rooms.get(current_room)
                if not room:
                    continue
                msg_id = room["counter"]
                room["counter"] += 1
                msg_data = {
                    "id": msg_id,
                    "room_id": current_room,
                    "sender": username,
                    "text": text,
                    "timestamp": datetime.now().isoformat(),
                    "reactions": {}
                }
                room["messages"].append(msg_data)
                if len(room["messages"]) > 200:
                    room["messages"].pop(0)
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO messages (room_id, sender, text) VALUES ($1, $2, $3)",
                            current_room, username, text
                        )
                except Exception as e:
                    logger.warning(f"DB message save failed: {e}")
                # For DM, send to both users
                if current_room.startswith("dm_"):
                    parts = current_room[3:].split("_")
                    for u in parts:
                        await send_to_user(u, {"type": "message", "data": msg_data})
                else:
                    await broadcast_to_room(current_room, {"type": "message", "data": msg_data})

            # --- Typing ---
            elif t == "typing":
                room = rooms.get(current_room)
                if room:
                    if obj.get("typing"):
                        room["typing"].add(username)
                    else:
                        room["typing"].discard(username)
                    if current_room.startswith("dm_"):
                        parts = current_room[3:].split("_")
                        for u in parts:
                            await send_to_user(u, {"type": "typing", "users": list(room["typing"])})
                    else:
                        await broadcast_to_room(current_room, {"type": "typing", "users": list(room["typing"])})

            # --- Delete message ---
            elif t == "delete":
                msg_id = obj.get("msg_id")
                room = rooms.get(current_room)
                if room:
                    for msg in room["messages"]:
                        if msg["id"] == msg_id and (msg["sender"] == username or role == "owner"):
                            room["messages"].remove(msg)
                            if current_room.startswith("dm_"):
                                parts = current_room[3:].split("_")
                                for u in parts:
                                    await send_to_user(u, {"type": "delete", "msg_id": msg_id, "room_id": current_room})
                            else:
                                await broadcast_to_room(current_room, {"type": "delete", "msg_id": msg_id, "room_id": current_room})
                            break

            # --- React ---
            elif t == "react":
                msg_id = obj.get("msg_id")
                emoji = str(obj.get("emoji", ""))
                room = rooms.get(current_room)
                if room and emoji:
                    for msg in room["messages"]:
                        if msg["id"] == msg_id:
                            reactions = msg.setdefault("reactions", {})
                            ulist = reactions.setdefault(emoji, [])
                            if username in ulist:
                                ulist.remove(username)
                                if not ulist:
                                    del reactions[emoji]
                            else:
                                ulist.append(username)
                            payload = {"type": "update_reactions", "msg_id": msg_id, "room_id": current_room, "reactions": reactions}
                            if current_room.startswith("dm_"):
                                parts = current_room[3:].split("_")
                                for u in parts:
                                    await send_to_user(u, payload)
                            else:
                                await broadcast_to_room(current_room, payload)
                            break

            # --- Block user ---
            elif t == "block":
                target = obj.get("target")
                if target and target != username:
                    try:
                        async with app.state.pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO blocks (blocker, blocked) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                                username, target
                            )
                        await send_to_user(username, {"type": "blocked", "target": target})
                    except Exception as e:
                        logger.warning(f"Block failed: {e}")

            # --- Kick (owner only) ---
            elif t == "kick":
                if role != "owner":
                    continue
                target = obj.get("target")
                if target and target in active_connections:
                    await send_to_user(target, {"type": "kicked", "text": "Вы были исключены владельцем"})
                    try:
                        await active_connections[target]["ws"].close()
                    except Exception:
                        pass

            # --- Mute user in room (owner only) ---
            elif t == "mute":
                if role != "owner":
                    continue
                target = obj.get("target")
                if target:
                    await send_to_user(target, {"type": "muted", "text": "Вы замьючены владельцем"})

            # --- Voice: join/leave ---
            elif t == "voice_join":
                if current_room not in voice_rooms:
                    voice_rooms[current_room] = set()
                voice_rooms[current_room].add(username)
                await broadcast_to_room(current_room, {
                    "type": "voice_update",
                    "room_id": current_room,
                    "users": list(voice_rooms[current_room])
                })

            elif t == "voice_leave":
                if current_room in voice_rooms:
                    voice_rooms[current_room].discard(username)
                    await broadcast_to_room(current_room, {
                        "type": "voice_update",
                        "room_id": current_room,
                        "users": list(voice_rooms[current_room])
                    })

            # --- WebRTC signaling ---
            elif t in ("call_offer", "call_answer", "call_ice"):
                target = obj.get("target")
                payload = {**obj, "from": username}
                if target and target in active_connections:
                    await send_to_user(target, payload)
                else:
                    # broadcast to voice room except sender
                    for uname in list(voice_rooms.get(current_room, set())):
                        if uname != username:
                            await send_to_user(uname, payload)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS error for {username}: {e}")
    finally:
        current_room = active_connections.get(username, {}).get("room", "general")
        # Leave voice
        for vroom in voice_rooms.values():
            vroom.discard(username)
        # Leave typing
        for room in rooms.values():
            room["typing"].discard(username)
        active_connections.pop(username, None)
        await broadcast_to_room(current_room, {"type": "system", "text": f"👋 {username} покинул чат"})
        await broadcast_to_all_users({"type": "online_update", "online_users": get_online_list()})
        await log_action(app.state.pool, username, "logout")
        logger.info(f"WS disconnected: {username}")


async def broadcast_to_all_users(message: dict):
    data = json.dumps(message, ensure_ascii=False)
    dead = []
    for username, info in active_connections.items():
        try:
            await info["ws"].send_text(data)
        except Exception:
            dead.append(username)
    for u in dead:
        active_connections.pop(u, None)


@app.get("/")
async def get():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)
