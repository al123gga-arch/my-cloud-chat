import os
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()
active_connections = {}
DATABASE_URL = os.getenv("DATABASE_URL")

def get_msk_time():
    return datetime.utcnow() + timedelta(hours=3)

async def broadcast(message: str):
    for conn in active_connections.values():
        try: await conn.send_text(message)
        except: pass

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        # УДАЛЯЕМ СТАРУЮ ТАБЛИЦУ, ЧТОБЫ УБРАТЬ КРИВЫЕ ДАННЫЕ
        await connection.execute('DROP TABLE IF EXISTS messages;')
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    # Загружаем сообщения
    async with app.state.db_pool.acquire() as connection:
        rows = await connection.fetch('SELECT sender, text, timestamp FROM messages ORDER BY id ASC LIMIT 50')
        for row in rows:
            # Отправляем в строгом формате sender:timestamp:text
            await websocket.send_text(f"{row['sender']}:{row['timestamp']}:{row['text']}")

    await broadcast(f"📢 СИСТЕМА:{get_msk_time().strftime('%H:%M')}:Пользователь {username} зашел")

    try:
        while True:
            data = await websocket.receive_text()
            msg_time = get_msk_time().strftime("%H:%M")
            async with app.state.db_pool.acquire() as connection:
                await connection.execute('INSERT INTO messages (sender, text, timestamp) VALUES ($1, $2, $3)', username, data, msg_time)
            await broadcast(f"{username}:{msg_time}:{data}")
    except WebSocketDisconnect:
        del active_connections[username]
        await broadcast(f"❌ СИСТЕМА:{get_msk_time().strftime('%H:%M')}:Пользователь {username} вышел")
