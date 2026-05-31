from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from typing import List, Dict

app = FastAPI()

# Хранилище активных подключений: {username: websocket}
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}

    async def connect(self, username: str, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[username] = websocket
        # Уведомляем всех, что кто-то зашел
        await self.broadcast(f"📢 {username} присоединился к чату!")

    def disconnect(self, username: str):
        if username in self.active_connections:
            del self.active_connections[username]

    async def broadcast(self, message: str):
        """Отправка сообщения вообще всем"""
        for connection in self.active_connections.values():
            await connection.send_text(message)

manager = ConnectionManager()

# Главная страница (клиентская часть)
@app.get("/")
async def get():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

# WebSocket эндпоинт для обмена сообщениями
@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    await manager.connect(username, websocket)
    try:
        while True:
            # Ждем сообщение от конкретного пользователя
            data = await websocket.receive_text()
            # Пересылаем его всем с указанием автора
            await manager.broadcast(f"✍️ {username}: {data}")
    except WebSocketDisconnect:
        manager.disconnect(username)
        await manager.broadcast(f"❌ {username} покинул чат.")