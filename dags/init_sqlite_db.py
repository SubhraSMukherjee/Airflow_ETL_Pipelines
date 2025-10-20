from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import os

DB_PATH = "/opt/airflow/shared_dbs/my_data.db"

def init_sqlite_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Example: create a simple table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert sample data if empty
    cursor.execute("SELECT COUNT(*) FROM customers")
    if cursor.fetchone()[0] == 0:
        cursor.executemany(
            "INSERT INTO customers (name, email) VALUES (?, ?)",
            [
                ("Customer 1", "alice@example.com"),
                ("Bob", "bob@example.com"),
                ("Charlie", "charlie@example.com"),
            ],
        )
        conn.commit()

    conn.close()
    print("✅ SQLite DB initialized and sample data inserted.")


with DAG(
    dag_id="init_sqlite_db",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Run manually or on demand
    catchup=False,
    tags=["setup", "sqlite","DB"],
) as dag:

    init_db_task = PythonOperator(
        task_id="init_sqlite_db_task",
        python_callable=init_sqlite_db,
    )
