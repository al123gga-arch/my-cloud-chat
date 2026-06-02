import os
import sys
import logging

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)

# Попытка импорта библиотек – здесь часто бывают ошибки
try:
    import asyncpg
    import bcrypt
    import fastapi
    import uvicorn
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Form
    from fastapi.responses import HTMLResponse
    import websockets
    import json
    import uuid
    from datetime import datetime
    from contextlib import asynccontextmanager
    from typing import Dict, List, Set, Optional
    logging.debug("All imports successful")
except Exception as e:
    logging.error(f"Import error: {e}")
    sys.exit(1)

# ===== Конфиг =====
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "BurmaldaOwner")

if not DATABASE_URL:
    logging.error("DATABASE_URL environment variable is not set!")
    sys.exit(1)

logging.debug(f"DATABASE_URL found (first 20 chars): {DATABASE_URL[:20]}...")

# Создаём пустое состояние (без загрузки из БД, только для диагностики)
app = FastAPI()

# Проверяем подключение к БД при старте
@app.on_event("startup")
async def startup():
    logging.info("Starting up, testing database connection...")
    try:
        # Попробуем подключиться и выполнить простой запрос
        conn = await asyncpg.connect(DATABASE_URL)
        version = await conn.fetchval("SELECT version();")
        logging.info(f"Connected to PostgreSQL: {version[:50]}")
        await conn.close()
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        # Здесь падаем, чтобы Render показал ошибку в логах
        raise

@app.get("/")
async def index():
    return HTMLResponse("<h1>Test server is running</h1>")

@app.get("/health")
async def health():
    return {"status": "ok"}

# Если всё хорошо, то запускаем
if __name__ == "__main__":
    # Этот блок не выполняется на Render (там uvicorn сам запускает), но на всякий случай
    pass
