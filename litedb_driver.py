import socket
import json

# --- Требования стандарта DB-API 2.0 ---
apilevel = '2.0'
threadsafety = 1
paramstyle = 'qmark' # SQLAlchemy будет использовать '?' для параметров

class Error(Exception): pass
class DatabaseError(Error): pass

def connect(**kwargs):
    """Точка входа для SQLAlchemy"""
    host = kwargs.get('host', '127.0.0.1')
    port = int(kwargs.get('port', 5555))
    return LiteDBConnection(host, port)

# --- Реализация драйвера ---
class LiteDBConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def cursor(self):
        return LiteDBCursor(self)

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass

class LiteDBCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self._results = []
        self.rowcount = -1

    def execute(self, query, parameters=None):
        # --- ДОБАВЬ ЭТУ СТРОКУ (убираем переносы строк от SQLAlchemy) ---
        query = query.replace('\n', ' ').replace('\r', ' ')
        # 1. МАГИЯ ПОДСТАНОВКИ: Заменяем '?' на реальные значения
        if parameters:
            for param in parameters:
                if isinstance(param, str):
                    val = f"'{param}'"
                elif isinstance(param, bool):
                    val = "1" if param else "0"
                else:
                    val = str(param)
                query = query.replace('?', val, 1)

        # 2. Отправляем в C++ сервер
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2.0)
            s.connect((self.connection.host, self.connection.port))
            s.sendall((query.strip() + "\n").encode('utf-8'))
            raw_response = s.recv(4096).decode('utf-8').strip()

        # 3. Обрабатываем ответ сервера для SQLAlchemy
        if raw_response.startswith("ERR"):
            raise DatabaseError(f"LiteDB Error: {raw_response}")
            
        self.description = None
        self._results = []

        if raw_response.startswith("{"):
            data = json.loads(raw_response)
            # SQLAlchemy очень капризна к порядку полей. 
            # Если в запросе было "SELECT id, name...", она ждет (id, name...)
            # Для простоты вернем значения в алфавитном порядке ключей или как они в JSON
            self.description = [(k, None, None, None, None, None, None) for k in data.keys()]
            self._results = [tuple(data.values())]
            self.rowcount = 1
        else:
            self.rowcount = 1 if "OK" in raw_response else 0

        return self

    def fetchone(self):
        return self._results.pop(0) if self._results else None

    def fetchall(self):
        res = self._results[:]
        self._results = []
        return res
    
    def close(self): pass