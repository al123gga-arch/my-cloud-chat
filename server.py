import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()
active_connections = {}
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as conn:
        # УДАЛЯЕМ ВСЁ СТАРОЕ, чтобы дизайн обновился корректно
        await conn.execute('DROP TABLE IF EXISTS messages;')
        await conn.execute('CREATE TABLE messages (id SERIAL PRIMARY KEY, sender TEXT, text TEXT)')

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    # Отправка истории
    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch('SELECT sender, text FROM messages ORDER BY id ASC LIMIT 50')
        for row in rows:
            await websocket.send_text(f"{row['sender']}:{row['text']}")

    await broadcast(f"СИСТЕМА:Пользователь {username} вошел")

    try:
        while True:
            data = await websocket.receive_text()
            async with app.state.db_pool.acquire() as conn:
                await conn.execute('INSERT INTO messages (sender, text) VALUES ($1, $2)', username, data)
            await broadcast(f"{username}:{data}")
    except WebSocketDisconnect:
        del active_connections[username]
        await broadcast(f"СИСТЕМА:Пользователь {username} вышел")

async def broadcast(message: str):
    for conn in active_connections.values():
        try: await conn.send_text(message)
        except: pass

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
