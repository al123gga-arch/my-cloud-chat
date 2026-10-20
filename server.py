import os
import sys
import json
import uuid
import asyncio
import traceback
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import bcrypt
import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse

# Импортируем настройки, БД и утилиты из созданных файлов
from config import (
    logger, OWNER_USERNAME, active_connections, 
    rooms, voice_rooms, user_state
)
from database import init_database, load_app_data
from helpers import (
    get_dm_other, broadcast_to_room, 
    send_to_user, broadcast_to_all_users
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управляет жизненным циклом приложения: инициализирует базу данных
    и кэширует комнаты при старте, а также корректно закрывает пул при выходе.
    """
    logger.info("Запуск жизненного цикла приложения (Lifespan starting)...")
    
    # Инициализируем таблицы и пул подключений
    await init_database(app)
    
    # Загружаем существующие комнаты и кэш истории сообщений
    await load_app_data(app.state.pool)
    
    yield
    
    # Корректно закрываем пул подключений при остановке сервера
    if hasattr(app.state, "pool") and app.state.pool:
        await app.state.pool.close()
        logger.info("Пул подключений к PostgreSQL успешно закрыт.")
    logger.info("Завершение жизненного цикла приложения (Lifespan shutdown).")

# Инициализируем FastAPI с кастомным lifespan-обработчиком
app = FastAPI(lifespan=lifespan)

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
        if await conn.fetchval("SELECT 1 FROM users WHERE username = $1", username): 
            return {"error": "Имя уже занято"}
            
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        role = "owner" if username == OWNER_USERNAME else "user"
        
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO users (username, password_hash, role, created_at) VALUES ($1, $2, $3, NOW())", 
                username, hashed, role
            )
            await conn.execute(
                "INSERT INTO profiles (username, color, emoji, status, bg_id) VALUES ($1, $2, $3, $4, $5)", 
                username, None, None, "online", "none"
            )
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
        
    return {"ok": True, "username": username, "role": row["role"]}

@app.get("/users")
async def get_users():
    online = list(active_connections.keys())
    return {"online": online, "count": len(online)}

@app.get("/rooms")
async def get_rooms():
    result = []
    for rid, rdata in rooms.items():
        if not rid.startswith("dm_") and not rid.startswith("saved_"):
            active_count = sum(1 for u in active_connections.values() if u.get("room") == rid)
            result.append({
                "id": rid, 
                "name": rdata["name"], 
                "online": active_count, 
                "voice_active": len(voice_rooms.get(rid, set())) > 0, 
                "voice_users": list(voice_rooms.get(rid, set()))
            })
    return {"rooms": result}

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT role FROM users WHERE username = $1", username)
    if not user:
        await websocket.close(code=1008, reason="User not found")
        return
        
    role = user["role"]
    await websocket.accept()
    active_connections[username] = {"ws": websocket, "room": "general"}
    logger.info(f"Веб-сокет подключен: {username}")

    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT username, color, emoji, bio, status, display_name, bg_id, owner_badge FROM profiles"
        )
    profiles = {
        r["username"]: {
            "color": r["color"], "emoji": r["emoji"], "bio": r["bio"], 
            "status": r["status"], "displayName": r["display_name"], 
            "bgId": r["bg_id"], "ownerBadge": r["owner_badge"]
        } for r in rows
    }

    last_room = user_state.get(username, {}).get("last_room", "general")
    if last_room not in rooms and not last_room.startswith("dm_"): 
        last_room = "general"

    saved_rid = "saved_" + username
    if saved_rid not in rooms:
        rooms[saved_rid] = {"name": "Избранное", "messages": [], "counter": 1, "typing": set(), "creator": username}
        voice_rooms[saved_rid] = set()

    await send_to_user(username, {
        "type": "init", "username": username, "role": role,
        "online_users": list(active_connections.keys()),
        "rooms": [{"id": rid, "name": rdata["name"]} for rid, rdata in rooms.items() if not rid.startswith("dm_") and not rid.startswith("saved_")],
        "profiles": profiles,
        "voice_rooms": {rid: list(users) for rid, users in voice_rooms.items() if users},
        "last_room": last_room
    })
    
    for msg in rooms["general"]["messages"][-50:]:
        await send_to_user(username, {"type": "history", "data": msg})
        
    await broadcast_to_room("general", {"type": "system", "text": f"✨ {username} присоединился"})
    await broadcast_to_all_users({"type": "online_update", "online_users": list(active_connections.keys())})

    try:
        while True:
            raw = await websocket.receive_text()
            try: 
                obj = json.loads(raw)
            except: 
                continue
                
            t = obj.get("type", "")
            user_info = active_connections.get(username, {})
            cur_room = user_info.get("room", "general") if isinstance(user_info, dict) else "general"

            # === ВХОД В КОМНАТУ ===
            if t == "join_room":
                new_room = obj.get("room_id", "general")
                if new_room not in rooms and not new_room.startswith("dm_"):
                    await send_to_user(username, {"type": "error", "text": "Комната не найдена"})
                    continue
                if cur_room in voice_rooms and username in voice_rooms.get(cur_room, set()):
                    voice_rooms[cur_room].discard(username)
                    await broadcast_to_room(cur_room, {"type": "voice_update", "room_id": cur_room, "users": list(voice_rooms[cur_room])})
                if cur_room in rooms:
                    rooms[cur_room]["typing"].discard(username)
                    await broadcast_to_room(cur_room, {"type": "typing", "room_id": cur_room, "users": list(rooms[cur_room]["typing"])})
                
                active_connections[username]["room"] = new_room
                if username not in user_state: 
                    user_state[username] = {}
                user_state[username]["last_room"] = new_room
                
                if new_room.startswith("dm_") and new_room not in rooms:
                    rooms[new_room] = {"name": new_room, "messages": [], "counter": 1, "typing": set(), "creator": None}
                    voice_rooms[new_room] = set()
                    
                await send_to_user(username, {"type": "room_joined", "room_id": new_room, "name": rooms[new_room].get("name", new_room)})
                for msg in rooms[new_room]["messages"][-50:]:
                    await send_to_user(username, {"type": "history", "data": msg})
                if new_room.startswith("dm_"):
                    other = get_dm_other(new_room, username)
                    if other: 
                        await send_to_user(other, {"type": "messages_read", "room_id": new_room, "by": username})

            # === СОЗДАНИЕ КОМНАТЫ ===
            elif t == "create_room":
                name = obj.get("name", "").strip()
                if not name or len(name) > 40:
                    await send_to_user(username, {"type": "error", "text": "Неверное название"})
                    continue
                rid = "room_" + uuid.uuid4().hex[:8]
                rooms[rid] = {"name": name, "messages": [], "counter": 1, "typing": set(), "creator": username}
                voice_rooms[rid] = set()
                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute("INSERT INTO rooms (id, name, creator, created_at) VALUES ($1, $2, $3, NOW())", rid, name, username)
                except Exception as e: 
                    logger.warning(f"Ошибка сохранения комнаты в БД: {e}")
                await broadcast_to_all_users({"type": "room_created", "room_id": rid, "name": name, "creator": username})

            # === УДАЛЕНИЕ КОМНАТЫ ===
            elif t == "delete_room":
                rid = obj.get("room_id")
                if rid == "general" or (role != "owner" and rooms.get(rid, {}).get("creator") != username):
                    await send_to_user(username, {"type": "error", "text": "Нет прав"})
                    continue
                rooms.pop(rid, None)
                voice_rooms.pop(rid, None)
                try:
Используйте код с осторожностью.
async with app.state.pool.acquire() as conn:
async with conn.transaction():
await conn.execute("DELETE FROM messages WHERE room_id = $1", rid)
await conn.execute("DELETE FROM rooms WHERE id = $1", rid)
except:
pass
await broadcast_to_all_users({"type": "room_deleted", "room_id": rid})
# === ОТПРАВКА ТЕКСТОВОГО СООБЩЕНИЯ ===
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
"INSERT INTO messages (room_id, sender, text, reply_to) VALUES ($1, $2, $3, $4) RETURNING id",
cur_room, username, text, reply_to
)
db_id = row["id"]
except Exception as e:
logger.warning(f"Ошибка сохранения сообщения в БД: {e}")
await send_to_user(username, {"type": "error", "text": "Не удалось отправить"})
continue
# СТРОГОЕ ФОРМАТИРОВАНИЕ ДАТЫ: Отправляем без микросекунд с суффиксом Z (UTC)
current_ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
msg_data = {
"id": db_id, "room_id": cur_room, "sender": username,
"text": text, "timestamp": current_ts, "reactions": {}, "edited": False
}
if reply_to:
msg_data["reply_to"] = reply_to
room["messages"].append(msg_data)
if len(room["messages"]) > 200:
room["messages"].pop(0)
if cur_room.startswith("dm_"):
await send_to_user(username, {"type": "message", "data": msg_data})
other = get_dm_other(cur_room, username)
if other:
await send_to_user(other, {"type": "message", "data": msg_data})
else:
await broadcast_to_room(cur_room, {"type": "message", "data": msg_data})
# === СТАТУС ПЕЧАТИ (TYPING) ===
elif t == "typing":
rid = obj.get("room_id", cur_room)
room = rooms.get(rid)
if room:
if obj.get("typing"):
room["typing"].add(username)
else:
room["typing"].discard(username)
tl = list(room["typing"])
payload = {"type": "typing", "room_id": rid, "users": tl}
if rid.startswith("dm_"):
await send_to_user(username, payload)
other = get_dm_other(rid, username)
if other:
await send_to_user(other, payload)
else:
await broadcast_to_room(rid, payload)
# === УДАЛЕНИЕ СООБЩЕНИЯ ===
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
payload = {"type": "delete", "msg_id": msg_id, "room_id": cur_room}
if cur_room.startswith("dm_"):
await send_to_user(username, payload)
other = get_dm_other(cur_room, username)
if other:
await send_to_user(other, payload)
else:
await broadcast_to_room(cur_room, payload)
break
# === РЕДАКТИРОВАНИЕ СООБЩЕНИЯ ===
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
await conn.execute("UPDATE messages SET text = $1, edited = TRUE WHERE id = $2", new_text, msg_id)
except:
pass
payload = {"type": "message_edited", "msg_id": msg_id, "room_id": cur_room, "text": new_text, "edited": True}
if cur_room.startswith("dm_"):
await send_to_user(username, payload)
other = get_dm_other(cur_room, username)
if other:
await send_to_user(other, payload)
else:
await broadcast_to_room(cur_room, payload)
break
# === РЕАКЦИИ (EMOJI) ===
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
else:
ulist.append(username)
if not ulist:
del reactions[emoji]
payload = {"type": "update_reactions", "msg_id": msg_id, "room_id": cur_room, "reactions": reactions}
if cur_room.startswith("dm_"):
await send_to_user(username, payload)
other = get_dm_other(cur_room, username)
if other:
await send_to_user(other, payload)
else:
await broadcast_to_room(cur_room, payload)
break
# === ОБНОВЛЕНИЕ ПРОФИЛЯ ===
elif t == "update_profile":
try:
async with app.state.pool.acquire() as conn:
await conn.execute("""
INSERT INTO profiles (username, color, emoji, bio, status, display_name, bg_id, owner_badge)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (username) DO UPDATE SET
color=EXCLUDED.color, emoji=EXCLUDED.emoji, bio=EXCLUDED.bio,
status=EXCLUDED.status, display_name=EXCLUDED.display_name,
bg_id=EXCLUDED.bg_id, owner_badge=EXCLUDED.owner_badge, updated_at=NOW()
""", username, obj.get("color"), obj.get("emoji"), obj.get("bio"), obj.get("status"), obj.get("displayName"), obj.get("bgId"), obj.get("ownerBadge"))
await broadcast_to_all_users({"type": "profile_updated", "username": username, "profile": {"color": obj.get("color"), "emoji": obj.get("emoji"), "bio": obj.get("bio"), "status": obj.get("status"), "displayName": obj.get("displayName"), "bgId": obj.get("bgId"), "ownerBadge": obj.get("ownerBadge")}})
except Exception as e:
logger.warning(f"Ошибка обновления профиля: {e}")
await send_to_user(username, {"type": "error", "text": "Не удалось сохранить профиль"})
# === ПОДКЛЮЧЕНИЕ К ГОЛОСУ ===
elif t == "voice_join":
if cur_room not in voice_rooms:
voice_rooms[cur_room] = set()
voice_rooms[cur_room].add(username)
await broadcast_to_room(cur_room, {"type": "voice_update", "room_id": cur_room, "users": list(voice_rooms[cur_room])})
await broadcast_to_all_users({"type": "voice_room_update", "room_id": cur_room, "users": list(voice_rooms[cur_room])})
# === ОТКЛЮЧЕНИЕ ОТ ГОЛОСА ===
elif t == "voice_leave":
if cur_room in voice_rooms:
voice_rooms[cur_room].discard(username)
await broadcast_to_room(cur_room, {"type": "voice_update", "room_id": cur_room, "users": list(voice_rooms[cur_room])})
await broadcast_to_all_users({"type": "voice_room_update", "room_id": cur_room, "users": list(voice_rooms[cur_room])})
# === СТАТУС РАЗГОВОРА (SPEAKING) ===
elif t == "voice_speaking":
speaking = obj.get("speaking", False)
if cur_room in voice_rooms:
await broadcast_to_room(cur_room, {"type": "voice_speaking", "room_id": cur_room, "username": username, "speaking": speaking}, exclude=username)
# === WebRTC СИГНАЛИНГ ===
elif t in ("call_offer", "call_answer", "call_ice", "call_reject"):
target = obj.get("target")
payload = {**obj, "from": username}
if target and target in active_connections:
await send_to_user(target, payload)
# === ПОМЕТИТЬ КАК ПРОЧИТАННОЕ ===
elif t == "mark_read":
rid = obj.get("room_id")
if rid and rid.startswith("dm_"):
other = get_dm_other(rid, username)
if other:
await send_to_user(other, {"type": "messages_read", "room_id": rid, "by": username})
# === СПИСОК УЧАСТНИКОВ КОМНАТЫ ===
elif t == "get_room_members":
rid = obj.get("room_id", cur_room)
members = [u for u, info in active_connections.items() if info.get("room") == rid]
await send_to_user(username, {"type": "room_members", "room_id": rid, "members": members})
except WebSocketDisconnect:
pass
except Exception as e:
logger.error(f"Критическая ошибка веб-сокета для пользователя {username}: {e}")
logger.error(traceback.format_exc())
finally:
user_info = active_connections.get(username, {})
cur_room = user_info.get("room", "general") if isinstance(user_info, dict) else "general"
for vroom in voice_rooms.values():
vroom.discard(username)
for r in rooms.values():
r["typing"].discard(username)
active_connections.pop(username, None)
await broadcast_to_room(cur_room, {"type": "system", "text": f"👋 {username} покинул чат"})
await broadcast_to_all_users({"type": "online_update", "online_users": list(active_connections.keys())})
@app.get("/")
async def serve_index():
base = os.path.dirname(os.path.abspath(file))
path = os.path.join(base, "index.html")
try:
with open(path, "r", encoding="utf-8") as f:
return HTMLResponse(f.read())
except FileNotFoundError:
logger.error(f"Файл index.html не найден по пути {path}")
return HTMLResponse("index.html not found", status_code=500)
logger.info("Бэкенд сервера полностью скомпилирован и готов к работе.")
