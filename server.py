import os
import bcrypt
import json
import asyncio
from datetime import datetime
from typing import Dict, List, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL")

# Хранилища в памяти
active_connections: Dict[str, WebSocket] = {}
messages_history: List[dict] = []
message_id_counter = 1
typing_users: Set[str] = set()

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_logs (
                id SERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                action TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT NOW()
            )
        ''')

@app.post("/register")
async def register(username: str = Form(...), password: str = Form(...)):
    if not username or not password:
        return {"error": "Заполните все поля"}
    if len(password) < 8:
        return {"error": "Пароль должен быть не менее 8 символов"}
    async with app.state.pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", username)
        if exists:
            return {"error": "Это имя пользователя уже занято"}
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        await conn.execute("INSERT INTO users (username, password_hash) VALUES ($1, $2)", username, hashed)
        await conn.execute("INSERT INTO user_logs (username, action) VALUES ($1, $2)", username, "register")
        return {"ok": True}

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT password_hash FROM users WHERE username = $1", username)
        if not row:
            return {"error": "Неверный логин или пароль"}
        if bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
            await conn.execute("INSERT INTO user_logs (username, action) VALUES ($1, $2)", username, "login")
            return {"ok": True, "username": username}
        return {"error": "Неверный логин или пароль"}

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT 1 FROM users WHERE username = $1", username)
        if not user:
            await websocket.close(code=1008)
            return
    await websocket.accept()
    active_connections[username] = websocket
    
    for msg in messages_history[-50:]:
        await websocket.send_text(json.dumps({"type": "history", "data": msg}))
    
    sys_msg = {"type": "system", "text": f"✨ {username} присоединился к чату"}
    await broadcast(sys_msg)
    
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg_obj = json.loads(data)
            except:
                msg_obj = {"type": "text", "text": data}
            
            if msg_obj["type"] == "text":
                global message_id_counter
                msg_id = message_id_counter
                message_id_counter += 1
                msg_data = {
                    "id": msg_id,
                    "sender": username,
                    "text": msg_obj["text"].strip(),
                    "timestamp": datetime.now().isoformat(),
                    "reactions": {}
                }
                messages_history.append(msg_data)
                if len(messages_history) > 100:
                    messages_history.pop(0)
                async with app.state.pool.acquire() as conn:
                    await conn.execute("INSERT INTO messages (sender, text) VALUES ($1, $2)", username, msg_data["text"])
                await broadcast({"type": "message", "data": msg_data})
            
            elif msg_obj["type"] == "typing":
                if msg_obj.get("typing", False):
                    typing_users.add(username)
                else:
                    typing_users.discard(username)
                await broadcast({"type": "typing", "users": list(typing_users)})
            
            elif msg_obj["type"] == "delete":
                msg_id = msg_obj["msg_id"]
                for msg in messages_history:
                    if msg["id"] == msg_id and msg["sender"] == username:
                        messages_history.remove(msg)
                        await broadcast({"type": "delete", "msg_id": msg_id})
                        break
            
            elif msg_obj["type"] == "react":
                msg_id = msg_obj["msg_id"]
                emoji = msg_obj["emoji"]
                for msg in messages_history:
                    if msg["id"] == msg_id:
                        if "reactions" not in msg:
                            msg["reactions"] = {}
                        if emoji in msg["reactions"]:
                            if username in msg["reactions"][emoji]:
                                msg["reactions"][emoji].remove(username)
                                if not msg["reactions"][emoji]:
                                    del msg["reactions"][emoji]
                            else:
                                msg["reactions"][emoji].append(username)
                        else:
                            msg["reactions"][emoji] = [username]
                        await broadcast({"type": "update_reactions", "msg_id": msg_id, "reactions": msg["reactions"]})
                        break
    
    except WebSocketDisconnect:
        pass
    finally:
        if username in active_connections:
            del active_connections[username]
        typing_users.discard(username)
        await broadcast({"type": "typing", "users": list(typing_users)})
        sys_msg = {"type": "system", "text": f"👋 {username} покинул чат"}
        await broadcast(sys_msg)
        async with app.state.pool.acquire() as conn:
            await conn.execute("INSERT INTO user_logs (username, action) VALUES ($1, $2)", username, "logout")

async def broadcast(message):
    for conn in active_connections.values():
        try:
            await conn.send_text(json.dumps(message))
        except:
            pass

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
