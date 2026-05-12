import sqlite3
import time
import random
import os
import base64

DB_NAME = "test_sqlite_entropy10gb.db"
TOTAL_RECORDS = 1750000
BATCH_SIZE = 50000  # SQLite отлично переваривает большие пачки

def get_stealth_binary_data(record_id):
    # 1. Заголовок (ELF + уникальный ID)
    header = b'\x7FELF\x01\x01\x01\x00' + record_id.to_bytes(8, 'little')

    # 2. "Скрытые" повторы (пролог функции x86-64: push rbp; mov rbp, rsp)
    # В кодировке Base64 это даст строку VUiJ5VVIieVVSInl...
    prologue_padding = b'\x55\x48\x89\xe5' * 32

    # 3. Таблица строк (реалистичные названия функций)
    string_table = b'func_init_db;func_connect;func_query;err_timeout;err_not_found;' * 4

    # 4. Энтропия (абсолютный хаос из 64 байт)
    entropy = os.urandom(64)

    # Собираем бинарный блок
    binary_block = header + prologue_padding + string_table + entropy

    # Кодируем в Base64 для текстового поля 'name'
    encoded_name = base64.b64encode(binary_block).decode('ascii')
    
    age = random.randint(18, 90)
    is_active = random.choice([True, False])
    
    return encoded_name, age, is_active

def run_sqlite_test():
    print(f"--- Старт ОБЪЕКТИВНОГО (Stealth) теста SQLite ({TOTAL_RECORDS} записей) ---")
    
    # Удаляем старые файлы, чтобы тест был 100% чистым
    for ext in ["", "-wal", "-shm"]:
        file_path = DB_NAME + ext
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass
            
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    # Включаем WAL и нормальную синхронизацию (как в твоей БД)
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("PRAGMA synchronous = NORMAL")
    cursor.execute("PRAGMA temp_store = MEMORY")
    
    # Создаем таблицу в точности как твоя логическая схема
    cursor.execute('''CREATE TABLE IF NOT EXISTS use 
                      (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, is_active BOOLEAN)''')
    
    start_time = time.time()

    for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
        batch = []
        for j in range(BATCH_SIZE):
            record_id = i + j
            name, age, is_active = get_stealth_binary_data(record_id)
            batch.append((record_id, name, age, is_active))
        
        cursor.executemany("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)", batch)
        conn.commit() 
        print(f"Вставлено {i + BATCH_SIZE} / {TOTAL_RECORDS}...")

    end_time = time.time()
    conn.close()
    
    print(f"\n✅ SQLite тест завершен!")
    print(f"⏱ Время выполнения: {end_time - start_time:.2f} секунд")
    
    # Считаем итоговый размер файлов на диске (основной файл + WAL-лог)
    db_size = os.path.getsize(DB_NAME) if os.path.exists(DB_NAME) else 0
    wal_name = DB_NAME + "-wal"
    wal_size = os.path.getsize(wal_name) if os.path.exists(wal_name) else 0
    
    total_size_mb = (db_size + wal_size) / (1024 * 1024)
    print(f"💾 Итоговый размер SQLite на диске: {total_size_mb:.2f} MB")

if __name__ == "__main__":
    run_sqlite_test()