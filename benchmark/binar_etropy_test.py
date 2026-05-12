import socket
import time
import random
import os
import base64
import json

HOST = '127.0.0.1'
PORT = 5555
TOTAL_RECORDS = 1750000
BATCH_SIZE = 1000

def get_stealth_binary_payload(record_id):
    # 1. Заголовок (например, ELF + уникальный ID записи)
    header = b'\x7FELF\x01\x01\x01\x00' + record_id.to_bytes(8, 'little')

    # 2. "Скрытые" повторы. 
    # Это реальные x86-64 инструкции (пролог функции), которые часто дублируются в коде.
    # В Base64 они превратятся в кашу вроде "VUiJ5...", которая не бросается в глаза.
    prologue_padding = b'\x55\x48\x89\xe5' * 32

    # 3. Таблица строк. Реалистичные названия функций, которые периодически повторяются.
    string_table = b'func_init_db;func_connect;func_query;err_timeout;err_not_found;' * 4

    # 4. Чистый хаос (Энтропия) - 64 байта непредсказуемых данных.
    entropy = os.urandom(64)

    # Собираем наш бинарный файл
    binary_block = header + prologue_padding + string_table + entropy

    # Кодируем в Base64 для передачи по текстовому протоколу
    encoded_name = base64.b64encode(binary_block).decode('ascii')

    # Формируем JSON строго по твоей YAML-схеме
    payload = {
        "name": encoded_name,
        "age": random.randint(18, 90),
        "is_active": random.choice([True, False])
    }
    return json.dumps(payload, separators=(',', ':'))

def run_my_db_test():
    print(f"--- Старт ОБЪЕКТИВНОГО (Stealth) теста YADRO DB ({TOTAL_RECORDS} записей) ---")
    print("Генерация: Скрытые паттерны ассемблера + Строки + Энтропия")
    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((HOST, PORT))
    except Exception as e:
        print(f"Ошибка подключения: {e}")
        return

    start_time = time.time()

    for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
        commands = ""
        for j in range(BATCH_SIZE):
            record_id = i + j
            json_data = get_stealth_binary_payload(record_id)
            commands += f"INSERT use {record_id} {json_data}\n"
        
        try:
            s.sendall(commands.encode('utf-8'))
            
            # Ждем подтверждения (ACKS) от сервера
            acks_received = 0
            while acks_received < BATCH_SIZE:
                data = s.recv(65536)
                if not data:
                    break
                acks_received += data.count(b'\n')
        except Exception as e:
            print(f"\nОшибка при отправке: {e}")
            break
            
        if (i + BATCH_SIZE) % 10000 == 0:
            print(f"Успешно обработано {i + BATCH_SIZE} / {TOTAL_RECORDS}...")

    # Сбрасываем на диск
    print("Отправка команды FLUSH (сброс на диск)...")
    s.sendall(b"FLUSH\n")
    try:
        print("Ответ сервера:", s.recv(1024).decode().strip())
    except:
        pass
    
    s.close()
    end_time = time.time()
    print(f"\n✅ Тест завершен!")
    print(f"⏱ Время выполнения: {end_time - start_time:.2f} секунд")

if __name__ == "__main__":
    run_my_db_test()