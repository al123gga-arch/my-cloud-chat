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

@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        # Создаем таблицу. Теперь поле timestamp сохраняет точное время строкой
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
    
    # Загружаем последние 50 сообщений вместе со временем
    async with app.state.db_pool.acquire() as connection:
        rows = await connection.fetch('''
            SELECT sender, text, timestamp FROM messages 
            ORDER BY id ASC LIMIT 50
        ''')
        for row in rows:
            await websocket.send_text(f"{row['sender']}:{row['timestamp']}:{row['text']}")

    current_time = get_msk_time().strftime("%H:%M")
    await broadcast(f"📢 СИСТЕМА:{current_time}:{username} присоединился к чату")

    try:
        while True:
            data = await websocket.receive_text()
            msg_time = get_msk_time().strftime("%H:%M")
            
            # Сохраняем в базу текст и время
            async with app.state.db_pool.acquire() as connection:
                await connection.execute(
                    'INSERT INTO messages (sender, text, timestamp) VALUES ($1, $2, $3)', 
                    username, data, msg_time
                )
            
            await broadcast(f"{username}:{msg_time}:{data}")
            
    except WebSocketDisconnect:
        if username in active_connections:
            del active_connections[username]
        current_time = get_msk_time().strftime("%H:%M")
        await broadcast(f"❌ СИСТЕМА:{current_time}:{username} покинул чат")

async def broadcast(message: str):
    for connection in list(active_connections.values()):
        try:
            await connection.send_text(message)
        except:
            pass
