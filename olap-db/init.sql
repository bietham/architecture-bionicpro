-- Таблица сырой телеметрии (существующая)
CREATE TABLE IF NOT EXISTS emg_sensor_data (
    user_id UInt32,
    prosthesis_type String,
    muscle_group String,
    signal_frequency UInt32,
    signal_duration UInt32,
    signal_amplitude Decimal(5,2),
    signal_time DateTime
) ENGINE = MergeTree()
ORDER BY (user_id, prosthesis_type, signal_time);

INSERT INTO emg_sensor_data
SELECT *
FROM file('olap.csv', 'CSV');

-- Витрина отчётности (наполняется Airflow DAG ежедневно)
-- ReplacingMergeTree(etl_date): при повторном запуске DAG за ту же дату
-- старые строки заменяются новыми → идемпотентность.
CREATE TABLE IF NOT EXISTS report_mart (
    user_id           UInt32,
    user_name         String,
    user_email        String,
    prosthesis_type   String,
    total_signals     UInt64,
    avg_frequency     Float64,
    avg_amplitude     Float64,
    avg_duration      Float64,
    max_amplitude     Float64,
    min_amplitude     Float64,
    first_signal_time DateTime,
    last_signal_time  DateTime,
    etl_date          Date
)
ENGINE = ReplacingMergeTree(etl_date)
ORDER BY (user_id, prosthesis_type)
PARTITION BY toYYYYMM(etl_date)
SETTINGS index_granularity = 8192;
