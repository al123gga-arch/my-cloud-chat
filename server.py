import os
import bcrypt
import json
from typing import Dict, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()
DATABASE_URL = os.getenv("DATABASE_URL")  # На Render: задать переменную окружения

active_connections: Dict[str, WebSocket] = {}  # username -> websocket
messages_history: List[dict] = []  # храним до 100 сообщений

@app.on_event("startup")
async def startup():
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.pool.acquire() as conn:
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL
            )
        ''')
        # Таблица сообщений (опционально, для постоянного хранения)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

@app.post("/register")
async def register(username: str, password: str):
    if not username or not password:
        return {"error": "Заполните все поля"}
    async with app.state.pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", username)
        if exists:
            return {"error": "Пользователь уже существует"}
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        await conn.execute("INSERT INTO users (username, password_hash) VALUES ($1, $2)", username, hashed)
        return {"ok": True}

@app.post("/login")
async def login(username: str, password: str):
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT password_hash FROM users WHERE username = $1", username)
        if not row:
            return {"error": "Неверный логин или пароль"}
        if bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
            return {"ok": True, "username": username}
        return {"error": "Неверный логин или пароль"}

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    # Проверяем, существует ли пользователь (сессия должна быть уже подтверждена через login)
    async with app.state.pool.acquire() as conn:
        user = await conn.fetchrow("SELECT 1 FROM users WHERE username = $1", username)
        if not user:
            await websocket.close(code=1008, reason="Неавторизован")
            return
    await websocket.accept()
    active_connections[username] = websocket
    
    # Отправляем историю (50 последних сообщений)
    for msg in messages_history[-50:]:
        await websocket.send_text(json.dumps(msg))
    
    # Уведомление о входе
    sys_msg = {"type": "system", "text": f"✨ {username} присоединился к чату"}
    for conn in active_connections.values():
        await conn.send_text(json.dumps(sys_msg))
    
    try:
        while True:
            data = await websocket.receive_text()
            if not data.strip():
                continue
            # Сохраняем в память и в БД (опционально)
            msg_data = {"type": "message", "sender": username, "text": data.strip()}
            messages_history.append(msg_data)
            if len(messages_history) > 100:
                messages_history.pop(0)
            # Асинхронно сохраняем в БД (не ждём)
            async with app.state.pool.acquire() as conn:
                await conn.execute("INSERT INTO messages (sender, text) VALUES ($1, $2)", username, data.strip())
            # Рассылаем всем
            for conn in active_connections.values():
                await conn.send_text(json.dumps(msg_data))
    except WebSocketDisconnect:
        sys_msg = {"type": "system", "text": f"👋 {username} покинул чат"}
        for conn in active_connections.values():
            await conn.send_text(json.dumps(sys_msg))
    finally:
        if username in active_connections:
            del active_connections[username]

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
