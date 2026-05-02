import socket
import time
import random
import string
import json

HOST = '127.0.0.1'
PORT = 5555
TOTAL_RECORDS = 1000000
BATCH_SIZE = 1000

def get_payload(record_id):
    """
    Генерирует данные строго под твою схему:
    name: STRING, age: INT, is_active: BOOL
    """
    payload = {
        # Генерируем повторяющееся имя для проверки сжатия LZ4
        "name": f"User_{record_id}_" + "A" * 20, 
        "age": random.randint(18, 90),
        "is_active": random.choice([True, False])
    }
    # separators убирает лишние пробелы, делая JSON компактнее
    return json.dumps(payload, separators=(',', ':'))

def run_my_db_test():
    print(f"--- Старт теста YADRO DB ({TOTAL_RECORDS} записей) ---")
    print(f"Ожидаемая схема: name (STRING), age (INT), is_active (BOOL)")
    
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
            # ВЫЗЫВАЕМ ПРАВИЛЬНУЮ ФУНКЦИЮ
            json_data = get_payload(record_id)
            commands += f"INSERT use {record_id} {json_data}\n"
        
        try:
            s.sendall(commands.encode('utf-8'))
            
            # Читаем подтверждения (ACKS)
            acks_received = 0
            while acks_received < BATCH_SIZE:
                data = s.recv(65536)
                if not data:
                    break
                acks_received += data.count(b'\n')
        except Exception as e:
            print(f"\nОшибка при отправке на записи {i}: {e}")
            break
            
        if (i + BATCH_SIZE) % 10000 == 0:
            print(f"Успешно обработано {i + BATCH_SIZE} / {TOTAL_RECORDS}...")

    # Сбрасываем буферы на диск
    print("Отправка команды FLUSH...")
    s.sendall(b"FLUSH\n")
    try:
        print("Ответ сервера:", s.recv(1024).decode().strip())
    except:
        print("Сервер не ответил на FLUSH (возможно, соединение закрыто)")
    
    s.close()
    end_time = time.time()
    print(f"\n✅ Тест завершен!")
    print(f"⏱ Общее время: {end_time - start_time:.2f} секунд")

if __name__ == "__main__":
    run_my_db_test()