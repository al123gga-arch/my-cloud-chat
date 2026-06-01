import os
from fastapi import FastAPI, WebSocket
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
    # Уведомление о входе
    for conn in active_connections.values():
        await conn.send_text(f"SYSTEM:Пользователь {username} зашел в чат")
    try:
        while True:
            data = await websocket.receive_text()
            for conn in active_connections.values():
                await conn.send_text(f"{username}:{data}")
    except Exception:
        pass
    finally:
        del active_connections[username]

@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())
