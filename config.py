import os
import sys
import logging
import traceback

# Глобальный перехватчик критических ошибок в консоли
def global_exception_handler(exc_type, exc_value, exc_traceback):
    sys.stderr.write("".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
    sys.stderr.flush()

sys.excepthook = global_exception_handler

# Настройка логирования приложения
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr
)
logger = logging.getLogger("burmalda")

# Загрузка переменных окружения с Render
DATABASE_URL = os.getenv("DATABASE_URL")
OWNER_USERNAME = os.getenv("OWNER_USERNAME", "BurmaldaOwner")

# Жесткая проверка строки подключения к базе
if not DATABASE_URL:
    logger.error("Критическая ошибка: DATABASE_URL не задана в настройках Render! Выход.")
    sys.exit(1)

# Глобальные структуры данных в памяти сервера
active_connections = {}
rooms = {}
voice_rooms = {}
user_state = {}

# Инициализация дефолтной системной комнаты при старте
rooms["general"] = {
    "name": "Общий чат",
    "messages": [],
    "counter": 1,
    "typing": set(),
    "creator": None
}
voice_rooms["general"] = set()
