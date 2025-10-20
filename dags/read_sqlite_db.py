from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import os

DB_PATH = "/opt/airflow/shared_dbs/my_data.db"

def read_customers_from_sqlite():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found at {DB_PATH}. Run init_sqlite_db first.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM customers")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print("⚠️ No data found in 'customers' table.")
    else:
        print("✅ Retrieved data from SQLite:")
        for row in rows:
            print(row)  
    return rows

with DAG(
    dag_id="read_sqlite_db",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["sqlite", "example"],
) as dag:

    read_customers_task = PythonOperator(
        task_id="read_customers",
        python_callable=read_customers_from_sqlite,
    )
