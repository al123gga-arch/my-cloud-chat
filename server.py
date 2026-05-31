import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg
import json

app = FastAPI()

# Список активных подключений
active_connections = {}

# СЮДА ВСТАВЬ СВОЮ ССЫЛКУ INTERNAL DATABASE URL ИЗ RENDER
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@host/database")

# Подключение к базе данных при старте сервера
@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        # Создаем таблицу сообщений, если её нет
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()

@app.get("/")
async def get():
    # Возвращаем index.html, если кто-то стучится на главную
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    # При подключении пользователя отправляем ему историю последних 50 сообщений из базы
    async with app.state.db_pool.acquire() as connection:
        rows = await connection.fetch('''
            SELECT sender, text FROM messages 
            ORDER BY timestamp ASC LIMIT 50
        ''')
        for row in rows:
            await websocket.send_text(f"{row['sender']}:{row['text']}")

    # Оповещаем всех о входе
    await broadcast(f"📢 {username} присоединился к чату")

    try:
        while True:
            # Ждем сообщение от пользователя
            data = await websocket.receive_text()
            
            # Сохраняем сообщение в базу данных
            async with app.state.db_pool.acquire() as connection:
                await connection.execute(
                    'INSERT INTO messages (sender, text) VALUES ($1, $2)', 
                    username, data
                )
            
            # Пересылаем сообщение всем активным пользователям
            await broadcast(f"{username}:{data}")
            
    except WebSocketDisconnect:
        if username in active_connections:
            del active_connections[username]
        await broadcast(f"❌ {username} покинул чат")

async def broadcast(message: str):
    for connection in list(active_connections.values()):
        try:
            await connection.send_text(message)
        except:
            pass
