import os
from datetime import datetime, timedelta
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()

# Список активных подключений
active_connections = {}

DATABASE_URL = os.getenv("DATABASE_URL")

# Функция получения времени по МСК
def get_msk_time():
    return datetime.utcnow() + timedelta(hours=3)

async def broadcast(message: str):
    for connection in list(active_connections.values()):
        try:
            await connection.send_text(message)
        except:
            pass

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        # ХАК: Принудительно сносим старую ломающую таблицу при старте сервера
        await connection.execute('DROP TABLE IF EXISTS messages;')
        
        # Создаем чистую правильную таблицу, готовую к смайликам и времени
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
async
