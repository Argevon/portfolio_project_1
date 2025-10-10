from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def transfer_data():
    # Initialize hooks
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    # Fetch data from Postgres
    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("SELECT * FROM vehicle_data LIMIT 10;")
    rows = pg_cursor.fetchall()

    # Insert data into Snowflake
    insert_query = """
        INSERT INTO vehicle_data_snowflake (
            generated, routeShortName, tripId, routeId, headsign, vehicleCode,
            vehicleService, vehicleId, speed, direction, delay,
            scheduledTripStartTime, lat, lon, gpsQuality
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    sf_conn = snowflake_hook.get_conn()
    sf_cursor = sf_conn.cursor()
    sf_cursor.executemany(insert_query, rows)
    sf_conn.commit()

    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "postgres_to_snowflake",
    default_args=default_args,
    description="Transfer raw bus data from Postgres to Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 8),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_postgres_to_snowflake",
        python_callable=transfer_data
    )
