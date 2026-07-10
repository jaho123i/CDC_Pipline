import uuid

import psycopg2

# Konfiguracja
conn_params = {
    "host": "localhost",
    "port": 5432,
    "database": "appdb",
    "user": "app",
    "password": "app"
}


def init_database():
    try:
        # Połączenie
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS demo;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS demo.customers (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

        random_id = str(uuid.uuid4())[:8]
        new_email = f"user_{random_id}@example.com"
        new_name = f"Test User {random_id}"

        cursor.execute("""
                       INSERT INTO demo.customers (email, full_name)
                       VALUES (%s, %s)
                       """, (new_email, new_name))

        conn.commit()
        print(f"Dodano nowego klienta: {new_email}")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Błąd: {e}")


if __name__ == "__main__":
    init_database()
