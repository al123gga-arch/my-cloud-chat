import json
from config import active_connections, logger

def get_dm_other(rid: str, current_user: str) -> str or None:
    """
    Вычисляет имя второго участника в приватной комнате (DM).
    Формат ID приватной комнаты: dm_user1_user2
    """
    if not rid.startswith("dm_"): 
        return None
        
    rest = rid[3:]
    if rest == current_user: 
        return None
        
    prefix = current_user + "_"
    if rest.startswith(prefix): 
        return rest[len(prefix):]
        
    suffix = "_" + current_user
    if rest.endswith(suffix): 
        return rest[:-len(suffix)]
        
    idx = rest.find("_")
    if idx != -1:
        a, b = rest[:idx], rest[idx+1:]
        return b if a == current_user else a
        
    return None

async def broadcast_to_room(room_id: str, msg: dict, exclude: str = None):
    """
    Отправляет сообщение всем пользователям, которые сейчас находятся 
    внутри указанной комнаты (room_id).
    """
    data = json.dumps(msg, ensure_ascii=False)
    dead_connections = []
    
    for u, info in list(active_connections.items()):
        if info.get("room") == room_id and u != exclude:
            try: 
                await info["ws"].send_text(data)
            except Exception as e:
                logger.warning(f"Ошибка отправки сообщения пользователю {u} в комнату {room_id}: {e}")
                dead_connections.append(u)
                
    for u in dead_connections: 
        active_connections.pop(u, None)

async def send_to_user(username: str, msg: dict):
    """
    Отправляет персональное сообщение конкретному пользователю по его нику.
    """
    if username in active_connections:
        try: 
            await active_connections[username]["ws"].send_text(json.dumps(msg, ensure_ascii=False))
        except Exception as e:
            logger.warning(f"Ошибка отправки персонального сообщения для {username}: {e}")
            active_connections.pop(username, None)

async def broadcast_to_all_users(msg: dict):
    """
    Глобальное вещание: отправляет сообщение абсолютно всем пользователям,
    которые сейчас подключены к мессенджеру (онлайн).
    """
    data = json.dumps(msg, ensure_ascii=False)
    dead_connections = []
    
    for u, info in list(active_connections.items()):
        try: 
            await info["ws"].send_text(data)
        except Exception as e:
            logger.warning(f"Ошибка отправки глобального уведомления для {u}: {e}")
            dead_connections.append(u)
            
    for u in dead_connections: 
        active_connections.pop(u, None)
