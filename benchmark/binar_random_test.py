import socket
import time
import random
import os
import base64
import json

HOST = '127.0.0.1'
PORT = 5555
TOTAL_RECORDS = 3500000
BATCH_SIZE = 1000

# Набор реальных ассемблерных "запчастей"
ASM_SNIPPETS = [
    b'\x55\x48\x89\xe5',         # push rbp; mov rbp, rsp
    b'\x48\x83\xec\x20',         # sub rsp, 32
    b'\xbf\x01\x00\x00\x00',     # mov edi, 1
    b'\x48\x89\x7c\x24\x08',     # mov [rsp+8], rdi
    b'\xe8\x00\x00\x00\x00',     # call ...
    b'\xb8\x00\x00\x00\x00'      # mov eax, 0
]

STRINGS_POOL = [b'malloc', b'free', b'printf', b'memcpy', b'std_cout', b'pthread_create', b'vsnprintf']

def get_high_entropy_payload(record_id):
    # 1. Заголовок (8 байт)
    header = b'\x7FELF\x01\x01\x01\x00'
    
    # 2. Разнообразный "код" - выбираем 10 случайных инструкций
    code_section = b''.join(random.choices(ASM_SNIPPETS, k=10))
    
    # 3. Разнообразные строки - выбираем 5 случайных имен
    data_section = b'_'.join(random.choices(STRINGS_POOL, k=5))
    
    # 4. Уникальный хвост (увеличиваем хаос) - 128 байт чистого рандома
    noise = os.urandom(128)
    
    # Собираем блок (никаких длинных цепочек 'A')
    binary_block = header + code_section + data_section + noise
    
    encoded_name = base64.b64encode(binary_block).decode('ascii')

    payload = {
        "name": encoded_name,
        "age": random.randint(18, 90),
        "is_active": random.choice([True, False])
    }
    return json.dumps(payload, separators=(',', ':'))

def run_test():
    print(f"--- Старт ТЯЖЕЛОГО теста YADRO DB ({TOTAL_RECORDS} записей) ---")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try: s.connect((HOST, PORT))
    except Exception as e: return print(f"Ошибка: {e}")

    start_time = time.time()
    for i in range(0, TOTAL_RECORDS, BATCH_SIZE):
        commands = ""
        for j in range(BATCH_SIZE):
            json_data = get_high_entropy_payload(i + j)
            commands += f"INSERT use {i + j} {json_data}\n"
        
        s.sendall(commands.encode('utf-8'))
        acks = 0
        while acks < BATCH_SIZE:
            data = s.recv(65536)
            if not data: break
            acks += data.count(b'\n')
            
        if (i + BATCH_SIZE) % 50000 == 0:
            print(f"Прогресс: {i + BATCH_SIZE}...")

    s.sendall(b"FLUSH\n")
    s.recv(1024)
    s.close()
    print(f"✅ Готово! Время: {time.time() - start_time:.2f} сек")

if __name__ == "__main__":
    run_test()