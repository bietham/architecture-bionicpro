"""
ETL DAG: BionicPRO Report Mart Builder
=======================================
Извлекает данные клиентов из CRM (PostgreSQL) и телеметрии протезов
из OLAP (ClickHouse), объединяет их и записывает в витрину отчётности
report_mart в ClickHouse.

Расписание: ежедневно в 02:00 UTC.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2
import clickhouse_connect

# ─── Константы подключений ────────────────────────────────────────────────────
CRM_CONN = {
    "host": "crm_db",
    "port": 5432,
    "dbname": "crm_db",
    "user": "crm_user",
    "password": "crm_password",
}

OLAP_CONN = {
    "host": "olap_db",
    "port": 8123,
}

# ─── Аргументы DAG ────────────────────────────────────────────────────────────
default_args = {
    "owner": "bionic_data_team",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="etl_crm_to_olap_report_mart",
    description="ETL: CRM + EMG telemetry → ClickHouse report_mart (daily)",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # ежедневно в 02:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["bionic", "etl", "reports"],
)


# ─── Task 1: Убедиться, что витрина существует ───────────────────────────────
def create_mart_table(**context):
    """
    Создаёт таблицу-витрину report_mart в ClickHouse, если она ещё не существует.
    ReplacingMergeTree обеспечивает идемпотентность: повторная вставка за
    ту же дату перезапишет строки, а не задублирует их.
    """
    client = clickhouse_connect.get_client(
        host=OLAP_CONN["host"], port=OLAP_CONN["port"]
    )

    client.command("""
        CREATE TABLE IF NOT EXISTS report_mart (
            user_id       UInt32,
            user_name     String,
            user_email    String,
            prosthesis_type String,
            total_signals UInt64,
            avg_frequency Float64,
            avg_amplitude Float64,
            avg_duration  Float64,
            max_amplitude Float64,
            min_amplitude Float64,
            first_signal_time DateTime,
            last_signal_time  DateTime,
            etl_date      Date
        )
        ENGINE = ReplacingMergeTree(etl_date)
        ORDER BY (user_id, prosthesis_type)
        PARTITION BY toYYYYMM(etl_date)
        SETTINGS index_granularity = 8192
    """)
    print("report_mart table ensured.")


# ─── Task 2: Извлечь клиентов из CRM ─────────────────────────────────────────
def extract_crm_customers(**context):
    """
    Читает всех клиентов из CRM PostgreSQL.
    Результат сохраняется в XCom для следующего шага.
    """
    conn = psycopg2.connect(**CRM_CONN)
    cur = conn.cursor()
    cur.execute("SELECT id, name, email FROM customers ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    customers = {row[0]: {"name": row[1], "email": row[2]} for row in rows}
    print(f"Extracted {len(customers)} customers from CRM.")
    context["ti"].xcom_push(key="customers", value=customers)


# ─── Task 3: Агрегировать телеметрию за обработанный период ─────────────────
def aggregate_telemetry(**context):
    """
    Агрегирует данные телеметрии из emg_sensor_data за весь доступный период.
    etl_date = дата последней записи в таблице (максимальная дата сигнала).
    Это позволяет корректно работать с историческими данными и тестовыми датасетами.
    """
    client = clickhouse_connect.get_client(
        host=OLAP_CONN["host"], port=OLAP_CONN["port"]
    )

    query = """
        SELECT
            user_id,
            prosthesis_type,
            count()                             AS total_signals,
            avg(signal_frequency)               AS avg_frequency,
            avg(toFloat64(signal_amplitude))    AS avg_amplitude,
            avg(signal_duration)                AS avg_duration,
            max(toFloat64(signal_amplitude))    AS max_amplitude,
            min(toFloat64(signal_amplitude))    AS min_amplitude,
            min(signal_time)                    AS first_signal_time,
            max(signal_time)                    AS last_signal_time
        FROM emg_sensor_data
        GROUP BY user_id, prosthesis_type
        ORDER BY user_id
    """
    result = client.query(query)

    rows = result.result_rows
    print(f"Aggregated {len(rows)} (user_id, prosthesis_type) rows from OLAP.")

    # etl_date = дата самой последней записи в источнике
    max_date_result = client.query("SELECT max(toDate(signal_time)) FROM emg_sensor_data")
    etl_date = str(max_date_result.result_rows[0][0])

    context["ti"].xcom_push(key="telemetry", value=rows)
    context["ti"].xcom_push(key="etl_date", value=etl_date)


# ─── Task 4: Объединить и записать витрину ────────────────────────────────────
def load_report_mart(**context):
    """
    Соединяет агрегированную телеметрию с данными CRM по user_id = customer.id,
    затем записывает результат в report_mart.

    Пользователь, запросивший отчёт за период, который ещё не обработан
    Airflow, получит пустой ответ из витрины (данных там нет) — это корректное
    поведение, не требующее вычислений в реальном времени.
    """
    ti = context["ti"]
    customers: dict = ti.xcom_pull(key="customers", task_ids="extract_crm_customers")
    telemetry: list = ti.xcom_pull(key="telemetry", task_ids="aggregate_telemetry")
    etl_date_str: str = ti.xcom_pull(key="etl_date", task_ids="aggregate_telemetry")

    # XCom хранит etl_date как строку ("2025-03-25"), а clickhouse_connect
    # требует datetime.date для колонки типа Date.
    etl_date = datetime.strptime(etl_date_str, "%Y-%m-%d").date()

    if not telemetry:
        print("No telemetry data for this period. Skipping load.")
        return

    rows_to_insert = []
    skipped = 0

    for row in telemetry:
        (
            user_id, prosthesis_type,
            total_signals, avg_frequency, avg_amplitude, avg_duration,
            max_amplitude, min_amplitude,
            first_signal_time, last_signal_time,
        ) = row

        customer = customers.get(str(int(user_id)))
        if not customer:
            skipped += 1
            continue  # нет клиента в CRM — пропускаем

        rows_to_insert.append([
            int(user_id),
            customer["name"],
            customer["email"],
            str(prosthesis_type),
            int(total_signals),
            float(avg_frequency),
            float(avg_amplitude),
            float(avg_duration),
            float(max_amplitude),
            float(min_amplitude),
            first_signal_time,
            last_signal_time,
            etl_date,
        ])

    print(f"Rows to insert: {len(rows_to_insert)}, skipped (no CRM match): {skipped}")

    if not rows_to_insert:
        print("Nothing to insert.")
        return

    client = clickhouse_connect.get_client(
        host=OLAP_CONN["host"], port=OLAP_CONN["port"]
    )

    columns = [
        "user_id", "user_name", "user_email", "prosthesis_type",
        "total_signals", "avg_frequency", "avg_amplitude", "avg_duration",
        "max_amplitude", "min_amplitude",
        "first_signal_time", "last_signal_time", "etl_date",
    ]

    client.insert("report_mart", rows_to_insert, column_names=columns)
    print(f"Successfully inserted {len(rows_to_insert)} rows into report_mart.")


# ─── Определение порядка задач ────────────────────────────────────────────────
t_create_mart = PythonOperator(
    task_id="create_mart_table",
    python_callable=create_mart_table,
    dag=dag,
)

t_extract_crm = PythonOperator(
    task_id="extract_crm_customers",
    python_callable=extract_crm_customers,
    dag=dag,
)

t_aggregate_telemetry = PythonOperator(
    task_id="aggregate_telemetry",
    python_callable=aggregate_telemetry,
    dag=dag,
)

t_load_mart = PythonOperator(
    task_id="load_report_mart",
    python_callable=load_report_mart,
    dag=dag,
)

# Создать таблицу → параллельно извлечь CRM и телеметрию → загрузить витрину
t_create_mart >> [t_extract_crm, t_aggregate_telemetry] >> t_load_mart
