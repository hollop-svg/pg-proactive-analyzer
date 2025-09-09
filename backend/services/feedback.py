import threading
import queue
import json
import logging

try:
    import websockets
    import asyncio
except ImportError:
    websockets = None

# Очередь сообщений для обратной связи
feedback_queue = queue.Queue()

# 1. Логгер для CLI/CI
def log_feedback(message: dict):
    logging.warning(f"FEEDBACK: {json.dumps(message, ensure_ascii=False)}")

# 2. WebSocket broadcaster (для фронта)
class WebSocketFeedbackServer:
    def __init__(self, host='localhost', port=8765):
        self.host = host
        self.port = port
        self.clients = set()
        self.loop = None

    async def handler(self, websocket, path):
        self.clients.add(websocket)
        try:
            while True:
                msg = await websocket.recv()
                # Можно реализовать обратную связь от клиента
        finally:
            self.clients.remove(websocket)

    async def broadcast(self, message: dict):
        if self.clients:
            await asyncio.wait([client.send(json.dumps(message, ensure_ascii=False)) for client in self.clients])

    def start(self):
        if not websockets:
            print("websockets not installed")
            return
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        start_server = websockets.serve(self.handler, self.host, self.port)
        self.loop.run_until_complete(start_server)
        threading.Thread(target=self.loop.run_forever, daemon=True).start()

    def send(self, message: dict):
        if self.loop:
            asyncio.run_coroutine_threadsafe(self.broadcast(message), self.loop)

# 3. Основная точка входа
ws_server = None

def init_feedback_server():
    global ws_server
    if websockets:
        ws_server = WebSocketFeedbackServer()
        ws_server.start()

def send_feedback(message: dict, level='info'):
    """
    Отправляет сообщение обратной связи во все каналы.
    """
    # Логгер
    log_feedback(message)
    # WebSocket
    if ws_server:
        ws_server.send(message)
    # Можно добавить email, telegram, etc.

# 4. Пример использования
if __name__ == "__main__":
    init_feedback_server()
    send_feedback({
        "type": "red_flag",
        "priority": "high",
        "text": "Seq Scan на большой таблице! Рекомендуется создать индекс.",
        "query": "SELECT * FROM big_table"
    })
