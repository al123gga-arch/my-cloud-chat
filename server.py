import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncpg

app = FastAPI()

# Список активных подключений
active_connections = {}

# Сервер автоматически заберет ссылку из настроек Render
DATABASE_URL = os.getenv("DATABASE_URL")

@app.on_event("startup")
async def startup():
    # Создаем пул подключений к базе данных
    app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with app.state.db_pool.acquire() as connection:
        # Создаем таблицу для сообщений, если её ещё нет
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
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await websocket.accept()
    active_connections[username] = websocket
    
    # При подключении вытаскиваем историю сообщений из базы
    async with app.state.db_pool.acquire() as connection:
        rows = await connection.fetch('''
            SELECT sender, text FROM messages 
            ORDER BY timestamp ASC LIMIT 50
        ''')
        for row in rows:
            await websocket.send_text(f"{row['sender']}:{row['text']}")

    await broadcast(f"📢 {username} присоединился к чату")

    try:
        while True:
            data = await websocket.receive_text()
            
            # Сохраняем новое сообщение в базу данных
            async with app.state.db_pool.acquire() as connection:
                await connection.execute(
                    'INSERT INTO messages (sender, text) VALUES ($1, $2)', 
                    username, data
                )
            
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
