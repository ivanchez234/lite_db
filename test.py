import socket

def send_cmd(cmd):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(('127.0.0.1', 5555))
        s.sendall((cmd + "\n").encode('utf-8'))
        return s.recv(4096).decode('utf-8').strip()

print("1. Подготовка таблицы...")
print(send_cmd("CREATE col_test"))
print(send_cmd("SCHEMA id:INT name:STRING age:INT"))

print("\n2. Пишем 500 записей (Они ложатся в write_buffer как строки)...")
for i in range(500):
    # Делаем одинаковые имена, чтобы Zlib кайфанул от колоночного формата
    send_cmd(f"INSERT INTO col_test (id, name, age) VALUES ({i}, 'Ivan_Student', 21)")
print("✅ Данные в оперативной памяти!")

print("\n3. Читаем из RAM (до FLUSH)...")
print("SELECT 250:", send_cmd("SELECT * FROM col_test WHERE id = 250"))

print("\n4. Сбрасываем на диск (Строки -> Колонки -> Zlib -> Диск)...")
print("FLUSH:", send_cmd("FLUSH"))

print("\n5. Читаем с диска (Диск -> Zlib -> Колонки -> Строки)...")
print("SELECT 250:", send_cmd("SELECT * FROM col_test WHERE id = 250"))
print("SELECT 499:", send_cmd("SELECT * FROM col_test WHERE id = 499"))

print("\n6. Проверяем, что нет мусора...")
print("SELECT 999:", send_cmd("SELECT * FROM col_test WHERE id = 999"))