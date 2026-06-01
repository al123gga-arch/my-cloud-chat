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
        await conn.execute('CREATE TABLE IF NOT EXISTS messages (id SERIAL PRIMARY KEY, sender TEXT, text TEXT)')

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    # Отправка истории при подключении
    async with app.state.db_pool.acquire() as conn:
        rows = await conn.fetch('SELECT sender, text FROM messages ORDER BY id ASC LIMIT 50')
        for row in rows:
            await websocket.send_text(f"{row['sender']}:{row['text']}")

    try:
        while True:
            data = await websocket.receive_text()
            async with app.state.db_pool.acquire() as conn:
                await conn.execute('INSERT INTO messages (sender, text) VALUES ($1, $2)', username, data)
            # Рассылка всем
            for conn in active_connections.values():
                await conn.send_text(f"{username}:{data}")
    except WebSocketDisconnect:
        if username in active_connections:
            del active_connections[username]
