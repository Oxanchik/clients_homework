import os
from typing import List, Tuple

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()
POSTGRES = os.getenv("POSTGRES")

def create_database_if_not_exists(db_name):
    """Создает базу данных, если она не существует"""

    conn = psycopg2.connect(  # ← НЕ with, обычное соединение!
        host="localhost",
        user="postgres",
        password=POSTGRES,
        database="postgres"
    )

    try:
        conn.autocommit = True

        with conn.cursor() as cur_db:
            cur_db.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur_db.fetchone()

            if not exists:
                cur_db.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                print(f"✅ База данных '{db_name}' создана")
            else:
                print(f"ℹ️ База данных '{db_name}' уже существует")

    finally:
        conn.close()


def create_db_structure(conn):
    """Создает структуру базы данных"""

    with conn.cursor() as cur_str:
        cur_str.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name  VARCHAR(100) NOT NULL,
                email      VARCHAR(255) NOT NULL UNIQUE
            );
        """)

        cur_str.execute("""
            CREATE TABLE IF NOT EXISTS phones (
                phone_id  SERIAL PRIMARY KEY,
                client_id INT NOT NULL,
                phone     VARCHAR(20) NOT NULL,
                CONSTRAINT fk_client
                    FOREIGN KEY (client_id)
                    REFERENCES clients (client_id)
                    ON DELETE CASCADE
            );
        """)

    conn.commit()


def add_client(conn, first_name: str, last_name: str, email: str, phones: list[str] = None) -> int:
    """Добавляет нового клиента. Возвращает ID клиента"""

    phones = phones or []

    with conn.cursor() as cur_add:
        cur_add.execute(
            "INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING client_id",
            (first_name, last_name, email)
        )
        client_id = cur_add.fetchone()[0]

        if phones:
            phone_query = "INSERT INTO phones (client_id, phone) VALUES (%s, %s) RETURNING phone_id"
            for phone in phones:
                cur_add.execute(phone_query, (client_id, phone))

        conn.commit()
        phone_count = len(phones)
        print(f"\n✅ Клиент '{first_name} {last_name}' добавлен с ID {client_id}")
        print(f"Добавлено телефонов: {phone_count}")
        return client_id


def add_phone(conn, client_id: int, phone: str) -> int:
    """Добавляет телефон для клиента. Возвращает ID телефона"""

    with conn.cursor() as cur_add:
        cur_add.execute(
            "INSERT INTO phones (client_id, phone) VALUES (%s, %s) RETURNING phone_id",
            (client_id, phone)
        )
        phone_id = cur_add.fetchone()[0]
        conn.commit()
        print(f"\n✅ Телефон '{phone}' добавлен для клиента {client_id}")
        return phone_id


def update_client(conn, client_id: int = None, first_name: str = None, last_name: str = None, **updates):
    """Изменяет данные клиента"""

    # Проверяем условия поиска
    search_conditions = []
    search_params = []

    if client_id is not None:
        search_conditions.append("client_id = %s")
        search_params.append(client_id)
    if first_name:
        search_conditions.append("first_name ILIKE %s")
        search_params.append(f"%{first_name}%")
    if last_name:
        search_conditions.append("last_name ILIKE %s")
        search_params.append(f"%{last_name}%")

    if not search_conditions:
        print("❌ Укажите ID или имя для поиска")
        return

    # Собираем обновления
    update_fields = []
    update_params = []

    if updates.get('first_name'):
        update_fields.append("first_name = %s")
        update_params.append(updates['first_name'])
    if updates.get('last_name'):
        update_fields.append("last_name = %s")
        update_params.append(updates['last_name'])
    if updates.get('email'):
        update_fields.append("email = %s")
        update_params.append(updates['email'])

    if not update_fields:
        print("❌ Нечего обновлять")
        return

    where_clause = " AND ".join(search_conditions)

    query = f"""
        UPDATE clients 
        SET {', '.join(update_fields)} 
        WHERE {where_clause}
        RETURNING client_id, first_name, last_name, email
    """
    all_params = update_params + search_params

    with conn.cursor() as cur_upd:
        cur_upd.execute(query, all_params)
        result = cur_upd.fetchone()
        conn.commit()

        if result:
            cid, cfirst, clast, cemail = result
            print(f"✅ Клиент '{cfirst} {clast}' (ID: {cid}) обновлён:")
            print(f"   Email изменён на: {cemail}")
        else:
            print("❌ Клиент не найден")


def delete_phone(conn, client_id: int = None, phone: str = None):
    """Удаляет телефон по ID клиента ИЛИ по номеру"""

    if not any([client_id, phone]):
        print("❌ Укажите ID клиента или телефон")
        return None

    conditions = []
    params = []

    if client_id is not None:
        conditions.append("p.client_id = %s")
        params.append(client_id)
    if phone:
        conditions.append("p.phone = %s")
        params.append(phone)

    where_clause = " AND ".join(conditions)
    query = f"""
        DELETE FROM phones AS p
        USING clients AS c
        WHERE {where_clause}
          AND p.client_id=c.client_id
        RETURNING p.phone_id, p.client_id, p.phone, c.first_name, c.last_name
    """

    with conn.cursor() as cur_d:
        cur_d.execute(query, params)
        result = cur_d.fetchone()  # (phone_id, client_id, phone, first_name, last_name)

        if result:
            phone_id, cl_id, ph_num, first_name, last_name = result
            print(f"✅ Телефон '{ph_num}' (ID: {phone_id}) клиента '{first_name} {last_name}' (ID: {cl_id}) удалён")
            conn.commit()
            return result
        else:
            print("❌ Телефон не найден")
            conn.rollback()
            return None

def delete_client(conn, client_id: int = None, first_name: str = None, last_name: str = None):
    """Удаляет клиента по ID или по имени(телефоны удалятся автоматически через CASCADE)"""

    if not any([client_id, first_name, last_name]):
        print("❌ Укажите ID клиента или имя/фамилию")
        return None

    conditions = []
    params = []

    if client_id is not None:
        conditions.append("client_id = %s")
        params.append(client_id)
    if first_name:
        conditions.append("first_name ILIKE %s")
        params.append(f"%{first_name}%")
    if last_name:
        conditions.append("last_name ILIKE %s")
        params.append(f"%{last_name}%")

    where_clause = " AND ".join(conditions)
    query = f"""
        DELETE FROM clients 
        WHERE {where_clause}
        RETURNING client_id, first_name, last_name
    """

    with conn.cursor() as cur_d:
        cur_d.execute(query, params)
        result = cur_d.fetchone()

        if result:
            deleted_id, deleted_first, deleted_last = result
            print(f"✅ Запись клиента '{deleted_first} {deleted_last}' (ID: {deleted_id}) удалена (телефоны тоже)")
            conn.commit()
            return result
        else:
            print(f"❌ Клиент не найден")
            conn.rollback()
            return None


def find_client(conn, client_id: int = None, first_name: str = None, last_name: str = None,
                email: str = None, phone: str = None) -> List[Tuple]:
    """Находит клиентов по любому из параметров"""

    conditions = []
    params = []

    if client_id is not None:
        conditions.append("c.client_id = %s")
        params.append(client_id)
    if first_name:
        conditions.append("c.first_name ILIKE %s")
        params.append(f"%{first_name}%")
    if last_name:
        conditions.append("c.last_name ILIKE %s")
        params.append(f"%{last_name}%")
    if email:
        conditions.append("c.email ILIKE %s")
        params.append(f"%{email}%")
    if phone:
        conditions.append("""
                EXISTS (
                    SELECT 1
                    FROM phones AS p2
                    WHERE p2.client_id = c.client_id
                      AND p2.phone = %s
                )
            """)
        params.append(phone)

    if not conditions:
        print("❌ Укажите хотя бы один параметр поиска")
        return []

    where_clause = " OR ".join(conditions)
    query = f"""
        SELECT c.client_id, c.first_name, c.last_name, c.email,
               COALESCE(STRING_AGG(p.phone, ', '), 'нет') as phones
        FROM clients AS c
        LEFT JOIN phones AS p ON c.client_id = p.client_id
        WHERE {where_clause}
        GROUP BY c.client_id, c.first_name, c.last_name, c.email
        ORDER BY c.client_id
    """

    with conn.cursor() as cur_f:
        cur_f.execute(query, params)
        results = cur_f.fetchall()
        print(f"🔍 Найдено клиентов: {len(results)}")
        print("-" * 100)
        for row in results:
            print(f"ID: {row[0]:2d} | {row[1]:10s} {row[2]:10s} | {row[3]:25s} | Телефоны: {row[4]}")
        print("-" * 100)
        return results


def print_all_clients(conn):
    """Выводит всех клиентов с телефонами"""

    with conn.cursor() as cur_all:
        cur_all.execute("""
            SELECT c.client_id, c.first_name, c.last_name, c.email,
                   COALESCE(STRING_AGG(p.phone, ', '), 'нет') as phones
            FROM clients AS c
            LEFT JOIN phones AS p ON c.client_id = p.client_id
            GROUP BY c.client_id, c.first_name, c.last_name, c.email
            ORDER BY c.client_id
        """)
        results = cur_all.fetchall()

        print(f"Все клиенты ({len(results)}):")
        print("-" * 100)
        for row in results:
            print(f"ID: {row[0]:2d} | {row[1]:10s} {row[2]:10s} | {row[3]:25s} | Телефоны: {row[4]}")
        print("-" * 100)


if __name__ == '__main__':
    new_db = "clients_db"

    # 1. Создаем БД и структуру
    print("Инициализация БД...")
    create_database_if_not_exists(new_db)

    with psycopg2.connect(
            host="localhost",
            user="postgres",
            password=POSTGRES,
            database=new_db
    ) as conn_clients:

        with conn_clients.cursor() as cur:
            cur.execute("""DROP TABLE IF EXISTS phones, clients;""")

        create_db_structure(conn_clients)

        print("\n" + "="*60)
        print("ДЕМОНСТРАЦИЯ ВСЕХ ФУНКЦИЙ")
        print("="*60)

        # 2. Добавляем клиентов
        client1_id = add_client(conn_clients, "Иван", "Петров", "ivan@example.com",  ["+7-900-123-45-67", "+7-900-111-22-33"])
        client2_id = add_client(conn_clients, "Мария", "Сидорова", "maria@example.com")
        client3_id = add_client(conn_clients, "Алексей", "Комаров", "alexey@example.ru", ["+7-910-258-79-46", "+7-916-789-46-13"])
        client4_id = add_client(conn_clients, "Сергей", "Петров", "sergey@example.ru")

        # 3. Добавляем телефоны
        add_phone(conn_clients, client1_id, "+7-903-321-11-22")
        add_phone(conn_clients, client2_id, "+7-901-987-65-43")

        # 4. Ищем клиентов
        print("\n🔍 Поиск, если забыли указать параметры:")
        find_client(conn_clients)

        f_name1 = 'Анна'
        print(f"\n🔍 Поиск по несуществующему имени: {f_name1}")
        find_client(conn_clients, last_name=f_name1)

        f_lastname1 = 'Петров'
        print(f"\n🔍 Поиск по фамилии: {f_lastname1}")
        find_client(conn_clients, last_name=f_lastname1)

        f_phone1 = '+7-916-789-46-13'
        print(f"\n🔍 Поиск по телефону: {f_phone1}")
        find_client(conn_clients, phone=f_phone1)

        print(f"\n🔍 Поиск по ID клиента: {client2_id}")
        find_client(conn_clients, client_id=client2_id)

        print("\nТаблица всех клиентов")
        print_all_clients(conn_clients)

        # 5. Обновляем клиента
        f_name2 = "Иван"
        print(f"\nОбновляем запись клиента '{f_name2}'")
        update_client(conn_clients, first_name=f_name2, email="new_ivan@example.com")

        print("\nПопытка обновить запись несуществующего клиента")
        update_client(conn_clients, first_name='Николай', email="nik@example.com")

        print("\nПопытка обновить запись и не указать новой информации")
        update_client(conn_clients, first_name='Иван', last_name='Петров')

        print("\nПопытка обновить запись и не указать параметры")
        update_client(conn_clients)

        print("\nТаблица всех клиентов")
        print_all_clients(conn_clients)

        # 6. Удаляем телефон
        f_phone2 = "+7-903-321-11-22"
        print(f"\nУдаляем телефон: {f_phone2}")
        delete_phone(conn_clients, phone=f_phone2)

        f_phone3 = "+7-900-321-11-22"
        print(f"\nПопытка удалить отсутствующий телефон: {f_phone3}")
        delete_phone(conn_clients, phone=f_phone2)

        # 7. Удаляем клиента
        f_name3 = "Алексей"
        f_last_name3 = "Комаров"
        print(f"\nУдаляем запись клиента '{f_name3} {f_last_name3}'")
        delete_client(conn_clients, first_name=f_name3, last_name=f_last_name3)

        f_name4 = "Владимир"
        f_last_name4 = "Сидоров"
        print(f"\nПопытка удалить запись несуществующего клиента '{f_name4} {f_last_name4}'")
        delete_client(conn_clients, first_name=f_name4, last_name=f_last_name4)

        print("\nВсе оставшиеся клиенты:")
        print_all_clients(conn_clients)

    print("\n✅ Демонстрация завершена!")
