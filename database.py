import asyncio
import asyncpg
from config import logger, DATABASE_URL, rooms, voice_rooms

async def init_database(app):
    """
    Инициализирует пул подключений к PostgreSQL и создает структуру таблиц,
    если они еще не созданы в базе данных.
    """
    logger.info("Попытка подключения к базе данных...")
    pool = None
    
    # 5 попыток подключения к БД с паузой (на случай долгого старта сервера БД)
    for attempt in range(5):
        try:
            pool = await asyncpg.create_pool(
                DATABASE_URL, 
                min_size=2, 
                max_size=10,
                command_timeout=60.0
            )
            logger.info("Пул подключений к PostgreSQL успешно создан.")
            break
        except Exception as e:
            logger.warning(f"Попытка {attempt + 1} подключения к БД не удалась: {e}")
            await asyncio.sleep(2)
            
    if pool is None:
        logger.error("Не удалось подключиться к базе данных после 5 попыток!")
        raise RuntimeError("Could not connect to database")
        
    app.state.pool = pool
    
    # Создание структуры таблиц и индексов
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Таблица пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Таблица профилей пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    username TEXT PRIMARY KEY REFERENCES users(username) ON DELETE CASCADE,
                    color TEXT,
                    emoji TEXT,
                    bio TEXT,
                    status TEXT DEFAULT 'online',
                    display_name TEXT,
                    bg_id TEXT DEFAULT 'none',
                    owner_badge TEXT,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Таблица текстовых комнат
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS rooms (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    creator TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Таблица сообщений (храним даты с таймзоной)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    room_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    text TEXT NOT NULL,
                    reply_to INTEGER,
                    edited BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Таблица логов пользователей
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_logs (
                    id SERIAL PRIMARY KEY,
                    username TEXT NOT NULL,
                    action TEXT NOT NULL,
                    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """)
            
            # Таблица блокировок (черный список)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS blocks (
                    blocker TEXT NOT NULL,
                    blocked TEXT NOT NULL,
                    PRIMARY KEY (blocker, blocked)
                )
            """)
            
            # Создаем индекс для ускорения выборки истории сообщений в комнатах
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_room_id 
                ON messages(room_id, created_at DESC)
            """)

    logger.info("Структура таблиц базы данных успешно проверена/создана.")

async def load_app_data(pool):
    """
    Загружает существующие комнаты и последние 50 сообщений для каждой комнаты
    из базы данных прямо в оперативную память сервера.
    """
    async with pool.acquire() as conn:
        # 1. Загружаем созданные пользователями комнаты
        saved_rooms = await conn.fetch("SELECT id, name, creator FROM rooms")
        for r in saved_rooms:
            rid = r["id"]
            if rid not in rooms:
                rooms[rid] = {
                    "name": r["name"], 
                    "messages": [], 
                    "counter": 1, 
                    "typing": set(), 
                    "creator": r["creator"]
                }
                voice_rooms[rid] = set()
                
        # 2. Загружаем историю сообщений для каждой комнаты
        for rid in list(rooms.keys()):
            # Берем последние 50 сообщений, сортируем по ID (порядку добавления)
            rows = await conn.fetch("""
                SELECT id, sender, text, reply_to, edited, created_at 
                FROM messages 
                WHERE room_id = $1 
                ORDER BY id ASC
            """, rid)
            
            msgs = []
            max_id = 0
            
            for row in rows:
                # Жесткое форматирование даты в ISO 8601 без микросекунд с флагом Z (UTC)
                if row["created_at"]:
                    ts = row["created_at"].strftime('%Y-%m-%dT%H:%M:%SZ')
                else:
                    from datetime import datetime, timezone
                    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
                    
                msgs.append({
                    "id": row["id"],
                    "room_id": rid,
                    "sender": row["sender"],
                    "text": row["text"],
                    "reply_to": row["reply_to"],
                    "edited": row["edited"],
                    "timestamp": ts,
                    "reactions": {}
                })
                if row["id"] > max_id:
                    max_id = row["id"]
                    
            rooms[rid]["messages"] = msgs
            rooms[rid]["counter"] = max_id + 1
            
    logger.info("Данные комнат и история сообщений успешно кэшированы из БД.")
