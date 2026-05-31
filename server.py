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

# Эта функция должна быть определена ДО использования
async def broadcast(message: str):
    for conn in active_connections.values():
        try: await conn.send_text(message)
        except: pass

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        ''')

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    async with app.state.db_pool.acquire() as connection:
        rows = await connection.fetch('SELECT sender, text, timestamp FROM messages ORDER BY id ASC LIMIT 50')
        for row in rows:
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
