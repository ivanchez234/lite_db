"""Драйвер lite_db по стандарту Python DB-API 2.0 (PEP 249).

Протокол сервера построчный: одна команда — одна строка, один ответ — одна
строка. Поэтому драйвер держит ОДНО соединение на всё время жизни Connection
и читает ответы по строкам, а не открывает новый сокет на каждый запрос.

Транзакций в lite_db нет: каждая команда фиксируется сама, как только сервер
ответил OK (при wal_sync: full — уже с fsync журнала). Отсюда:
  * commit() ничего не делает — фиксировать нечего, всё уже зафиксировано;
  * rollback() бросает NotSupportedError — отменить сделанное нельзя,
    и делать вид, что отмена прошла, было бы обманом.
"""

import json
import socket

# --- Требования стандарта DB-API 2.0 ---
apilevel = "2.0"
threadsafety = 1      # модуль можно делить между потоками, соединения — нет
paramstyle = "qmark"  # SQLAlchemy будет использовать '?' для параметров


# --- Иерархия исключений из PEP 249 ---
class Warning(Exception):  # noqa: A001 — имя задано стандартом
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# Какие ответы сервера каким исключениям соответствуют.
_ERRORS = {
    "ERR_ID_EXISTS": IntegrityError,
    "ERR_CONSTRAINT_VIOLATION": IntegrityError,
    "ERR_TABLE_NOT_FOUND": ProgrammingError,
    "ERR_SQL_PARSER": ProgrammingError,
    "ERR_UNKNOWN_COMMAND": ProgrammingError,
    "ERR_WAL_WRITE_FAILED": OperationalError,
    "ERR_FLUSH_FAILED": OperationalError,
    "ERR_COMMAND_TOO_LONG": DataError,
    "ERR_TOO_MANY_CONNECTIONS": OperationalError,
    "ERR_INTERNAL_EXCEPTION": InternalError,
    "ERR_UNKNOWN_CRASH": InternalError,
}

# Символы, которые серверный разбор SQL и JSON пока не умеет экранировать.
# Строку с ними не подставляем вовсе: иначе значение параметра могло бы
# изменить сам запрос (SQL-инъекция) или тихо исказить данные.
_FORBIDDEN_IN_STRINGS = set("'\",{}:\n\r\\")


def connect(host="127.0.0.1", port=5555, timeout=5.0, **kwargs):
    """Точка входа: и для прямого использования, и для SQLAlchemy."""
    return Connection(host, int(port), float(timeout))


def _quote(value):
    """Превращает параметр в литерал SQL."""
    if value is None:
        raise NotSupportedError("lite_db не поддерживает NULL")
    if isinstance(value, bool):  # раньше int: bool — подкласс int
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, str):
        bad = sorted(set(value) & _FORBIDDEN_IN_STRINGS)
        if bad:
            raise DataError(
                "строка содержит символы, которые lite_db пока не умеет "
                f"экранировать: {''.join(bad)!r}")
        return f"'{value}'"
    raise NotSupportedError(f"тип параметра не поддерживается: {type(value).__name__}")


def _bind(query, parameters):
    """Подставляет параметры вместо '?'."""
    parts = query.split("?")
    params = list(parameters or [])
    if len(parts) - 1 != len(params):
        raise ProgrammingError(
            f"в запросе {len(parts) - 1} знаков '?', а параметров {len(params)}")
    out = [parts[0]]
    for value, tail in zip(params, parts[1:]):
        out.append(_quote(value))
        out.append(tail)
    return "".join(out)


class Connection:
    def __init__(self, host, port, timeout):
        try:
            self._sock = socket.create_connection((host, port), timeout=timeout)
        except OSError as exc:
            raise OperationalError(f"не удалось подключиться к {host}:{port}: {exc}") from exc
        self._reader = self._sock.makefile("rb")
        self._closed = False

    # --- служебное ---
    def _check_open(self):
        if self._closed:
            raise InterfaceError("соединение закрыто")

    def _request(self, command):
        """Отправляет одну команду и возвращает одну строку ответа."""
        self._check_open()
        try:
            self._sock.sendall(command.encode("utf-8") + b"\n")
            line = self._reader.readline()
        except OSError as exc:
            raise OperationalError(f"ошибка сети: {exc}") from exc
        if not line:
            raise OperationalError("сервер закрыл соединение")
        return line.decode("utf-8").rstrip("\r\n")

    # --- PEP 249 ---
    def cursor(self):
        self._check_open()
        return Cursor(self)

    def commit(self):
        # Каждая команда уже зафиксирована сервером — см. описание модуля.
        self._check_open()

    def rollback(self):
        self._check_open()
        raise NotSupportedError(
            "в lite_db нет транзакций: каждая команда фиксируется сразу, отменить её нельзя")

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self._reader.close()
        finally:
            self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Cursor:
    arraysize = 1

    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self._rows = []
        self._closed = False

    def execute(self, query, parameters=None):
        if self._closed:
            raise InterfaceError("курсор закрыт")

        # SQLAlchemy форматирует запрос в несколько строк, а перевод строки
        # в протоколе — конец команды.
        query = query.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
        command = _bind(query, parameters)
        is_select = command.lstrip().upper().startswith("SELECT")

        response = self.connection._request(command)

        self.description = None
        self._rows = []
        self.rowcount = -1

        if response.startswith("ERR"):
            # Пустой результат — не ошибка: SELECT без строк и DELETE
            # несуществующего id в SQL просто ничего не возвращают.
            if response == "ERR_NOT_FOUND":
                self.rowcount = 0
                return self
            code = response.split(":", 1)[0].split(" ", 1)[0]
            raise _ERRORS.get(code, DatabaseError)(f"LiteDB: {response}")

        if is_select and response.startswith("{"):
            data = json.loads(response)
            self.description = [(k, None, None, None, None, None, None) for k in data]
            self._rows = [tuple(data.values())]
        elif is_select and response.startswith("["):
            rows = json.loads(response)
            if rows:
                columns = ["id"] + list(rows[0]["data"].keys())
                self.description = [(c, None, None, None, None, None, None) for c in columns]
                self._rows = [(r["id"], *r["data"].values()) for r in rows]
            else:
                self.description = []
        elif is_select:
            # SELECT одного поля возвращает голое значение.
            self.description = [("value", None, None, None, None, None, None)]
            self._rows = [(response,)]

        self.rowcount = len(self._rows) if is_select else 1
        return self

    def executemany(self, query, seq_of_parameters):
        total = 0
        for parameters in seq_of_parameters:
            self.execute(query, parameters)
            total += max(self.rowcount, 0)
        self.rowcount = total
        return self

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size=None):
        size = self.arraysize if size is None else size
        chunk, self._rows = self._rows[:size], self._rows[size:]
        return chunk

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        self._closed = True
        self._rows = []
