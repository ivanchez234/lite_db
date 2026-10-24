# Протокол, SQL и драйвер Python

Полный справочник по командам. Краткая версия — в разделе [«Использование»](../README.md#использование).

## Протокол и команды

Протокол текстовый и построчный: одна команда — одна строка, один ответ — одна строка.

| Команда | Пример | Ответ |
|---|---|---|
| Создать таблицу | `CREATE users` | `OK: Table 'users' created` |
| Задать схему | `SCHEMA users name:STRING age:INT is_active:BOOL` | `OK: Schema applied` |
| Вставить | `INSERT users 1 {"name": "Ivan", "age": 21, "is_active": true}` | `OK` / `ERR_ID_EXISTS` |
| Изменить поля | `UPDATE users 1 {"age": 22}` | `OK` / `ERR_NOT_FOUND` |
| Прочитать запись | `SELECT users 1` | JSON записи / `ERR_NOT_FOUND` |
| Прочитать поле | `SELECT users 1 name` | значение поля |
| Прочитать таблицу | `SELECT users ALL` | JSON-массив `[{ "id": ..., "data": {...} }, ...]` |
| Вставить пакет | `MPUT users 1 {...} 2 {...} 3 {...}` | `OK` или ошибка, и тогда не вставлено ничего |
| Удалить | `DELETE users 1` | `OK` / `ERR_NOT_FOUND` |
| Контрольная точка | `FLUSH` | `OK: All buffers flushed to disk` |
| Счётчики | `STATS` | JSON: записи журнала и fsync, блоки, попадания в кеш |

Подмножество SQL (переводится в команды выше):

```sql
INSERT INTO users (id, name, age, is_active) VALUES (1, 'Ivan', 21, 1)
SELECT * FROM users WHERE id = 1
SELECT * FROM users
UPDATE users SET name = 'Ivan', age = 22, is_active = 1 WHERE id = 1
DELETE FROM users WHERE id = 1
```

Правила SQL-подмножества:

- `WHERE` — только `id = число`, с необязательным именем таблицы (`users.id = 1`).
- `LIMIT n` (n ≥ 1) и `OFFSET 0` допускаются только вместе с `WHERE id` (так пишет SQLAlchemy для `.first()`).
- `UPDATE` меняет только перечисленные колонки. `id` поменять нельзя.
- Строки — в одинарных кавычках, кавычка внутри удваивается: `'O''Brien'`.
- Запрос, который транслятор не понимает, даёт `ERR_SQL_PARSER` с объяснением.

Таблицы и их схемы можно также описать в `setup.yaml` — они создаются при старте сервера. Там же настройки:

| Ключ | По умолчанию | Что задаёт |
|---|---|---|
| `wal_sync` | `full` | надёжность журнала: `none`, `flush`, `full` (см. [журнал](architecture.md#надёжность-журнал-предзаписи)) |
| `block_cache_mb` | `32` | размер кеша распакованных блоков, 0 — выключен |
| `block_size_kb` | `4` | сколько КБ записей копить перед сжатием в блок |

Сервер ищет `setup.yaml` в папке, откуда его запустили. При сборке CMake копирует его в `build/`, поэтому `server_app` можно запускать прямо оттуда.

Основные ошибки: `ERR_NOT_FOUND`, `ERR_ID_EXISTS`, `ERR_CONSTRAINT_VIOLATION` (данные не соответствуют схеме), `ERR_FIELD_HASH_COLLISION` и `ERR_SCHEMA_HASH_COLLISION` (у двух имён полей одинаковый хеш), `ERR_DUPLICATE_COLUMN`, `ERR_INVALID_JSON`, `ERR_SQL_PARSER` (запрос вне поддерживаемого подмножества), `ERR_TABLE_NOT_FOUND`, `ERR_WAL_WRITE_FAILED`, `ERR_FLUSH_FAILED`, `ERR_COMMAND_TOO_LONG` (строка длиннее 1 МБ), `ERR_TOO_MANY_CONNECTIONS`, `ERR_UNKNOWN_COMMAND`.

## Python: драйвер и SQLAlchemy

`litedb_driver.py` реализует DB-API 2.0 (PEP 249) поверх одного постоянного соединения:

```python
import litedb_driver as db

with db.connect(port=5555) as conn:
    cur = conn.cursor()
    cur.execute("INSERT INTO use (id, name, age, is_active) VALUES (?, ?, ?, ?)",
                (10, "Ivan", 21, True))
    cur.execute("SELECT * FROM use WHERE id = ?", (10,))
    print(cur.fetchone())   # ('Ivan', 21, True)
```

- Каждая команда фиксируется сразу, как только сервер ответил `OK`, поэтому `commit()` ничего не делает, а `rollback()` честно бросает `NotSupportedError`. Полные транзакции — в [планах](../README.md#планы).
- Ответы сервера превращаются в исключения PEP 249: `IntegrityError`, `ProgrammingError`, `OperationalError` и т. д.
- Строковые параметры экранируются по правилам SQL (кавычка удваивается), поэтому значение остаётся данными и не может стать частью запроса. Отвергается только перевод строки: в протоколе это конец команды.

`main_orm.py` — пример минимального диалекта SQLAlchemy (`litedb://`) поверх драйвера. Он не входит в CI.
