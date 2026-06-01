import os
import bcrypt
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Set

import asyncpg
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

# ===== In-memory state =====
active_connections: Dict[str, WebSocket] = {}
messages_history: List[dict] = []
message_id_counter = 1
typing_users: Set[str] = set()


# ===== Lifespan (replaces deprecated @app.on_event) =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    logger.info("Connecting to database...")
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)

    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username    TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         SERIAL PRIMARY KEY,
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

    logger.info("Database ready.")
    yield

    logger.info("Closing database pool...")
    await app.state.pool.close()


app = FastAPI(lifespan=lifespan)


# ===== Helpers =====
async def broadcast(message: dict):
    data = json.dumps(message, ensure_ascii=False)
    dead = []
    for username, conn in active_connections.items():
        try:
            await conn.send_text(data)
        except Exception:
            dead.append(username)
    for username in dead:
        active_connections.pop(username, None)


async def log_action(pool, username: str, action: str):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO user_logs (username, action) VALUES ($1, $2)",
                username, action
            )
    except Exception as e:
        logger.warning(f"Failed to log action '{action}' for '{username}': {e}")


# ===== Auth endpoints =====
@app.post("/register")
async def register(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if not username or not password:
        return {"error": "Заполните все поля"}
    if len(username) < 2 or len(username) > 32:
        return {"error": "Ник должен быть от 2 до 32 символов"}
    if len(password) < 8:
        return {"error": "Пароль должен быть минимум 8 символов"}

    async with app.state.pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM users WHERE username = $1", username
        )
        if exists:
            return {"error": "Это имя пользователя уже занято"}

        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        await conn.execute(
            "INSERT INTO users (username, password_hash) VALUES ($1, $2)",
            username, hashed
        )

    await log_action(app.state.pool, username, "register")
    logger.info(f"New user registered: {username}")
    return {"ok": True}


@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    username = username.strip()
    if not username or not password:
        return {"error": "Заполните все поля"}

    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT password_hash FROM users WHERE username = $1", username
        )

    if not row:
        return {"error": "Неверный логин или пароль"}

    if not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return {"error": "Неверный логин или пароль"}

    await log_action(app.state.pool, username, "login")
    logger.info(f"User logged in: {username}")
    return {"ok": True, "username": username}


# ===== WebSocket =====
@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    global message_id_counter

    # Verify user exists
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchval(
            "SELECT 1 FROM users WHERE username = $1", username
        )
    if not user:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    active_connections[username] = websocket
    logger.info(f"WS connected: {username} (online: {len(active_connections)})")

    # Send last 50 messages as history
    for msg in messages_history[-50:]:
        try:
            await websocket.send_text(json.dumps({"type": "history", "data": msg}, ensure_ascii=False))
        except Exception:
            break

    await broadcast({"type": "system", "text": f"✨ {username} присоединился к чату"})

    try:
        while True:
            raw = await websocket.receive_text()

            try:
                msg_obj = json.loads(raw)
            except json.JSONDecodeError:
                msg_obj = {"type": "text", "text": raw}

            msg_type = msg_obj.get("type", "text")

            # --- Text message ---
            if msg_type == "text":
                text = str(msg_obj.get("text", "")).strip()
                if not text:
                    continue

                msg_id = message_id_counter
                message_id_counter += 1

                msg_data = {
                    "id": msg_id,
                    "sender": username,
                    "text": text,
                    "timestamp": datetime.now().isoformat(),
                    "reactions": {}
                }
                messages_history.append(msg_data)
                if len(messages_history) > 200:
                    messages_history.pop(0)

                try:
                    async with app.state.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO messages (sender, text) VALUES ($1, $2)",
                            username, text
                        )
                except Exception as e:
                    logger.warning(f"Failed to save message to DB: {e}")

                await broadcast({"type": "message", "data": msg_data})

            # --- Typing indicator ---
            elif msg_type == "typing":
                if msg_obj.get("typing", False):
                    typing_users.add(username)
                else:
                    typing_users.discard(username)
                await broadcast({"type": "typing", "users": list(typing_users)})

            # --- Delete message ---
            elif msg_type == "delete":
                msg_id = msg_obj.get("msg_id")
                for msg in messages_history:
                    if msg["id"] == msg_id and msg["sender"] == username:
                        messages_history.remove(msg)
                        await broadcast({"type": "delete", "msg_id": msg_id})
                        break

            # --- Reaction ---
            elif msg_type == "react":
                msg_id = msg_obj.get("msg_id")
                emoji = str(msg_obj.get("emoji", ""))
                if not emoji:
                    continue
                for msg in messages_history:
                    if msg["id"] == msg_id:
                        reactions = msg.setdefault("reactions", {})
                        users_for_emoji = reactions.setdefault(emoji, [])
                        if username in users_for_emoji:
                            users_for_emoji.remove(username)
                            if not users_for_emoji:
                                del reactions[emoji]
                        else:
                            users_for_emoji.append(username)
                        await broadcast({
                            "type": "update_reactions",
                            "msg_id": msg_id,
                            "reactions": reactions
                        })
                        break

            # --- WebRTC signaling (relay to all others) ---
            elif msg_type in ("call_offer", "call_ice", "call_answer"):
                target = msg_obj.get("target")
                if target and target in active_connections:
                    try:
                        await active_connections[target].send_text(
                            json.dumps({**msg_obj, "from": username}, ensure_ascii=False)
                        )
                    except Exception:
                        pass
                else:
                    # broadcast to everyone except sender
                    for uname, conn in active_connections.items():
                        if uname != username:
                            try:
                                await conn.send_text(
                                    json.dumps({**msg_obj, "from": username}, ensure_ascii=False)
                                )
                            except Exception:
                                pass

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS error for {username}: {e}")
    finally:
        active_connections.pop(username, None)
        typing_users.discard(username)
        await broadcast({"type": "typing", "users": list(typing_users)})
        await broadcast({"type": "system", "text": f"👋 {username} покинул чат"})
        await log_action(app.state.pool, username, "logout")
        logger.info(f"WS disconnected: {username} (online: {len(active_connections)})")


# ===== Serve frontend =====
@app.get("/")
async def get():
    try:
        with open("index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)
