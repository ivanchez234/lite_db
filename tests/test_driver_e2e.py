"""Сквозной тест: настоящий процесс server_app + Python-драйвер.

Проверяет то, что не видно из юнит-тестов внутри одного процесса:
  1. драйвер работает через одно постоянное соединение;
  2. остановка по Ctrl+C (SIGINT): сервер сбрасывает данные и очищает журнал;
  3. аварийное завершение (kill): после перезапуска всё подтверждённое
     восстанавливается из журнала.

Запуск: python test_driver_e2e.py <путь к server_app> <путь к litedb_driver.py>
(ctest делает это сам).
"""

import importlib.util
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

SETUP_YAML = """wal_sync: full

tables:
  - name: use
    schema:
      - name: STRING
      - age: INT
      - is_active: BOOL
"""

IS_WINDOWS = os.name == "nt"

# Сообщения теста — по-русски. Когда ctest перехватывает вывод, Python пишет
# в кодировке системы: на русской Windows это cp1251 и всё работает, а на
# английской (например, в CI) — cp1252, и первая же кириллица роняет тест
# с UnicodeEncodeError. Поэтому кодировку вывода задаём явно.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def load_driver(path):
    spec = importlib.util.spec_from_file_location("litedb_driver", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """Процесс сервера в отдельном рабочем каталоге."""

    def __init__(self, binary, workdir, port):
        self.binary, self.workdir, self.port = binary, workdir, port
        self.proc = None
        self.log_path = os.path.join(workdir, "server.log")

    def start(self):
        log = open(self.log_path, "a", encoding="utf-8")
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if IS_WINDOWS else 0
        self.proc = subprocess.Popen([self.binary, str(self.port)], cwd=self.workdir,
                                     stdout=log, stderr=subprocess.STDOUT,
                                     creationflags=flags)
        log.close()
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"сервер завершился при старте:\n{self.log()}")
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return
            except OSError:
                time.sleep(0.1)
        raise AssertionError(f"сервер не начал слушать порт:\n{self.log()}")

    def interrupt(self):
        """Ctrl+C. Возвращает код выхода."""
        self.proc.send_signal(signal.SIGINT)
        return self.proc.wait(timeout=30)

    def kill(self):
        """Аварийное завершение: SIGKILL / TerminateProcess, без деструкторов."""
        self.proc.kill()
        self.proc.wait(timeout=30)

    def log(self):
        with open(self.log_path, encoding="utf-8", errors="replace") as f:
            return f.read()


def check(condition, message):
    if not condition:
        raise AssertionError(message)
    print(f"  ok: {message}")


def test_driver(db, port):
    print("1. Драйвер DB-API")
    conn = db.connect(port=port)
    cur = conn.cursor()

    cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                (1, "Alex", 30, True))
    check(cur.rowcount == 1, "INSERT с параметрами")

    cur.execute("SELECT * FROM use WHERE id = ?", (1,))
    check(cur.fetchone() == ("Alex", 30, True), "SELECT по id возвращает строку")

    cur.execute("SELECT * FROM use WHERE id = ?", (999,))
    check(cur.fetchone() is None and cur.rowcount == 0, "SELECT без результата — пусто, а не ошибка")

    try:
        cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                    (1, "Copy", 1, False))
        raise AssertionError("повторный INSERT должен был упасть")
    except db.IntegrityError:
        print("  ok: повторный id -> IntegrityError")

    try:
        cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                    (2, "x', 1, 1); DELETE FROM use WHERE id = 1; --", 1, True))
        raise AssertionError("опасная строка должна была быть отвергнута")
    except db.DataError:
        print("  ok: строка с кавычками отвергнута до отправки (нет инъекции)")

    cur.execute("SELECT * FROM use WHERE id = ?", (1,))
    check(cur.fetchone() == ("Alex", 30, True), "после попытки инъекции данные целы")

    # Много запросов подряд — все по одному соединению.
    for i in range(10, 210):
        cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                    (i, f"user{i}", i % 90, i % 2 == 0))
    cur.execute("SELECT * FROM use")
    rows = cur.fetchall()
    check(len(rows) == 201, "200 запросов по одному соединению + SELECT ALL")
    check([d[0] for d in cur.description] == ["id", "name", "age", "is_active"],
          "SELECT ALL отдаёт id первой колонкой")

    conn.commit()
    try:
        conn.rollback()
        raise AssertionError("rollback должен честно отказать")
    except db.NotSupportedError:
        print("  ok: rollback() -> NotSupportedError")

    conn.close()


def test_graceful_shutdown(db, server):
    print("2. Остановка по Ctrl+C")
    if IS_WINDOWS:
        # Послать Ctrl+C процессу без консоли (как в CI) на Windows нельзя.
        # Обработчик проверяется вручную: запустить server_app и нажать Ctrl+C.
        print("  пропущено на Windows")
        return

    code = server.interrupt()
    check(code == 0, f"сервер завершился с кодом 0 (получили {code})")
    wal = os.path.join(server.workdir, "wal.log")
    check(os.path.getsize(wal) == 0, "журнал очищен — данные сброшены на диск")
    check(os.path.exists(os.path.join(server.workdir, "data", "use", "seg_0.db")),
          "сегмент с данными на диске")

    server.start()
    with db.connect(port=server.port) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM use WHERE id = ?", (150,))
        check(cur.fetchone() == ("user150", 150 % 90, True), "после перезапуска данные на месте")


def test_crash_recovery(db, server):
    print("3. Аварийное завершение и восстановление из журнала")
    with db.connect(port=server.port) as conn:
        cur = conn.cursor()
        for i in range(1000, 1050):
            cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                        (i, f"crash{i}", 33, True))
        cur.execute("DELETE FROM use WHERE id = ?", (1010,))

    server.kill()   # без деструкторов: буферы в памяти потеряны, остался журнал
    server.start()

    check("Crash detected" in server.log(), "сервер заметил незавершённый журнал")
    with db.connect(port=server.port) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM use WHERE id = ?", (1049,))
        check(cur.fetchone() == ("crash1049", 33, True), "подтверждённая запись восстановлена")
        cur.execute("SELECT * FROM use WHERE id = ?", (1010,))
        check(cur.fetchone() is None, "удаление тоже восстановлено")
        cur.execute("SELECT * FROM use WHERE id = ?", (1,))
        check(cur.fetchone() == ("Alex", 30, True), "старые данные на месте")


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        return 2

    binary = os.path.abspath(sys.argv[1])
    db = load_driver(os.path.abspath(sys.argv[2]))

    with tempfile.TemporaryDirectory() as workdir:
        with open(os.path.join(workdir, "setup.yaml"), "w") as f:
            f.write(SETUP_YAML)

        server = Server(binary, workdir, free_port())
        server.start()
        try:
            test_driver(db, server.port)
            test_graceful_shutdown(db, server)
            test_crash_recovery(db, server)
        except Exception:
            print("---- лог сервера ----")
            print(server.log())
            raise
        finally:
            if server.proc and server.proc.poll() is None:
                server.kill()

    print("Все сквозные проверки пройдены")
    return 0


if __name__ == "__main__":
    sys.exit(main())
