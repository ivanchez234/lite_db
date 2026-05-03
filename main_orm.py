from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import registry

# 1. СОЗДАЕМ НАШ ДИАЛЕКТ
class LiteDBDialect(DefaultDialect):
    name = 'litedb'
    driver = 'litedb_driver'
    
    # Отключаем сложные фичи, чтобы SQLAlchemy не мучала наш парсер сложными системными запросами
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    supports_returns_native_bytes = False
    supports_statement_cache = False

    @classmethod
    def import_dbapi(cls):
        # Подключаем файл, который мы написали на Шаге 1
        import litedb_driver 
        return litedb_driver

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

# 2. РЕГИСТРИРУЕМ ДИАЛЕКТ В SQLAlchemy
registry.register("litedb", "main_orm", "LiteDBDialect")


# ==========================================
# КОД ДЛЯ ПРЕПОДАВАТЕЛЯ (Использование ORM)
# ==========================================

# Подключаемся к нашему C++ серверу через кастомный протокол!
engine = create_engine('litedb://127.0.0.1:5555')

Base = declarative_base()

class User(Base):
    __tablename__ = 'use'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)
    is_active = Column(Boolean)

Session = sessionmaker(bind=engine)
session = Session()

try:
    print("1. ORM: Создаем объект и делаем INSERT...")
    new_user = User(id=888, name="Ivan_ORM", age=21, is_active=True)
    session.add(new_user)
    session.commit()
    print("✅ Успешно!\n")

    print("2. ORM: Делаем SELECT...")
    user = session.query(User).filter_by(id=888).first()
    if user:
        print(f"✅ Нашли пользователя: {user.name}, возраст: {user.age}\n")
        
    print("3. ORM: Делаем DELETE...")
    session.delete(user)
    session.commit()
    print("✅ Успешно удалено!")

except Exception as e:
    print(f"❌ Ошибка: {e}")