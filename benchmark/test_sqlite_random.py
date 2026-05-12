import sqlite3
import time
import random
import os
import base64

DB_NAME = "test_sqlite_random10gb.db"
TOTAL_RECORDS = 3500000
BATCH_SIZE = 50000

ASM_SNIPPETS = [b'\x55\x48\x89\xe5', b'\x48\x83\xec\x20', b'\xbf\x01\x00\x00\x00', b'\xe8\x00\x00\x00\x00']
STRINGS_POOL = [b'malloc', b'free', b'printf', b'memcpy', b'std_cout']

def get_hardcore_data(record_id):
    header = b'\x7FELF\x01\x01\x01\x00'
    code = b''.join(random.choices(ASM_SNIPPETS, k=10))
    strings = b'_'.join(random.choices(STRINGS_POOL, k=5))
    noise = os.urandom(128)
    
    binary_block = header + code + strings + noise
    return base64.b64encode(binary_block).decode('ascii'), random.randint(18, 90), random.choice([True, False])

def run_sqlite():
    print(f"--- Старт ТЯЖЕЛОГО теста SQLite ({TOTAL_RECORDS} записей) ---")
    if os.path.exists(DB_NAME): os.remove(DB_NAME)
    
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL")
    cursor.execute("CREATE TABLE use (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, is_active BOOLEAN)")
    
    start_time = time.time()
    for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
        batch = [(i+j, *get_hardcore_data(i+j)) for j in range(BATCH_SIZE)]
        cursor.executemany("INSERT INTO use VALUES (?, ?, ?, ?)", batch)
        conn.commit()
        print(f"Прогресс: {i + BATCH_SIZE}...")

    conn.close()
    size_mb = os.path.getsize(DB_NAME) / (1024 * 1024)
    print(f"✅ SQLite: {time.time() - start_time:.2f} сек, Размер: {size_mb:.2f} MB")

if __name__ == "__main__":
    run_sqlite()