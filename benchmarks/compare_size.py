"""Сравнение размера на диске: lite_db против SQLite на одних и тех же данных.

    python benchmarks/compare_size.py --build build [--rows 100000]

Сравнивается только размер: скорость lite_db (сервер, каждый запрос идёт
через TCP) и SQLite (библиотека внутри процесса) в лоб сравнивать нечестно —
мерили бы сеть, а не движок.

Наборы данных:
  table   — обычные «табличные» строки: имя user<N>, возраст, флаг
  stealth — base64 бинарного блока с повторяющимися кусками и 64 байтами
            случайности (генератор из старого benchmark/binar_etropy_test.py)
  uuid    — случайные идентификаторы вида 1b4e28ba-2fa1-11d2-883f-0016d3cca427
  random  — base64 почти случайного блока: 128 случайных байт и немного
            повторов (генератор из старого benchmark/test_sqlite_random.py)

Случайность с фиксированным зерном: оба движка получают одинаковые строки,
а повторный запуск даёт те же данные.

lite_db: строки вставляются пакетами MPUT, затем FLUSH; размер — все файлы
каталога таблицы (сегменты и схема), журнал после FLUSH пуст.
SQLite: те же настройки, что в старых скриптах (journal_mode=WAL), после
вставки — checkpoint, чтобы всё легло в основной файл; размер — файл .db.
"""

import argparse
import base64
import json
import os
import random
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import run as bench  # noqa: E402  — запуск сервера и запросы из run.py

IS_WINDOWS = os.name == "nt"
EXE = ".exe" if IS_WINDOWS else ""

ASM_SNIPPETS = [b"\x55\x48\x89\xe5", b"\x48\x83\xec\x20", b"\xbf\x01\x00\x00\x00", b"\xe8\x00\x00\x00\x00"]
STRINGS_POOL = [b"malloc", b"free", b"printf", b"memcpy", b"std_cout"]


def gen_table(rng, i):
    return f"user{i}", i % 90, i % 2 == 0


def gen_stealth(rng, i):
    header = b"\x7FELF\x01\x01\x01\x00" + i.to_bytes(8, "little")
    prologue = b"\x55\x48\x89\xe5" * 32
    strings = b"func_init_db;func_connect;func_query;err_timeout;err_not_found;" * 4
    entropy = rng.randbytes(64)
    name = base64.b64encode(header + prologue + strings + entropy).decode("ascii")
    return name, rng.randint(18, 90), rng.choice([True, False])


def gen_random(rng, i):
    header = b"\x7FELF\x01\x01\x01\x00"
    code = b"".join(rng.choices(ASM_SNIPPETS, k=10))
    strings = b"_".join(rng.choices(STRINGS_POOL, k=5))
    noise = rng.randbytes(128)
    name = base64.b64encode(header + code + strings + noise).decode("ascii")
    return name, rng.randint(18, 90), rng.choice([True, False])


def gen_uuid(rng, i):
    # Случайные идентификаторы: 32 шестнадцатеричные цифры, как uuid4.
    value = rng.getrandbits(128)
    text = f"{value:032x}"
    name = f"{text[:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:]}"
    return name, rng.randint(18, 90), rng.choice([True, False])


DATASETS = [
    ("table", "табличные строки", gen_table),
    ("uuid", "случайные UUID", gen_uuid),
    ("stealth", "скрытые повторы (base64)", gen_stealth),
    ("random", "почти случайные (base64)", gen_random),
]


def rows_for(generator, count):
    rng = random.Random(42)
    return [(i, *generator(rng, i)) for i in range(count)]


def dir_size(path):
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            total += os.path.getsize(os.path.join(root, name))
    return total


def lite_db_size(server, rows):
    with tempfile.TemporaryDirectory() as workdir:
        bench.write_config(workdir, {"wal_sync": "none"})
        port = bench.free_port()
        proc = bench.start_server(server, workdir, port)
        try:
            import socket
            with socket.create_connection(("127.0.0.1", port)) as s:
                reader = s.makefile("rb")

                def call(cmd):
                    s.sendall(cmd.encode() + b"\n")
                    reply = reader.readline().decode().strip()
                    if not reply.startswith("OK"):
                        raise RuntimeError(f"{cmd[:60]}... -> {reply}")

                call("CREATE use")
                call("SCHEMA use name:STRING age:INT is_active:BOOL")
                for start in range(0, len(rows), 500):
                    parts = ["MPUT use"]
                    for rid, name, age, active in rows[start:start + 500]:
                        body = json.dumps({"name": name, "age": age, "is_active": active},
                                          separators=(",", ":"))
                        parts.append(f"{rid} {body}")
                    call(" ".join(parts))
                call("FLUSH")

                # Маленький размер ничего не стоит, если данные не те:
                # читаем несколько записей с диска и сверяем с исходными.
                for rid, name, age, active in (rows[0], rows[len(rows) // 2], rows[-1]):
                    s.sendall(f"SELECT use {rid}\n".encode())
                    got = json.loads(reader.readline().decode())
                    if got != {"name": name, "age": age, "is_active": active}:
                        raise RuntimeError(f"запись {rid} прочиталась не так: {got}")
            return dir_size(os.path.join(workdir, "data", "use"))
        finally:
            bench.stop_server(proc)


def sqlite_size(rows):
    with tempfile.TemporaryDirectory() as workdir:
        path = os.path.join(workdir, "test.db")
        conn = sqlite3.connect(path)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("CREATE TABLE use (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, is_active BOOLEAN)")
        conn.executemany("INSERT INTO use VALUES (?, ?, ?, ?)", rows)
        conn.commit()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()
        return os.path.getsize(path)


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", default="build")
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()

    server = os.path.abspath(os.path.join(args.build, "server_app" + EXE))
    print(f"Строк: {args.rows}, SQLite {sqlite3.sqlite_version}\n")
    print("| Данные | Сырых данных | lite_db | SQLite | SQLite / lite_db |")
    print("|---|---:|---:|---:|---:|")

    for _, title, generator in DATASETS:
        rows = rows_for(generator, args.rows)
        raw = sum(len(name) + 8 for _, name, _, _ in rows)  # строка + id и возраст по 4 байта
        lite = lite_db_size(server, rows)
        lite_mb = lite / 2**20
        sql = sqlite_size(rows)
        sql_mb = sql / 2**20
        print(f"| {title} | {raw / 2**20:.1f} МБ | {lite_mb:.1f} МБ | {sql_mb:.1f} МБ | {sql / lite:.2f}× |",
              flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
