from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

BATCH_SIZE = 5000  # number of records per batch

def transfer_data_batch():
    # --- Hooks ---
    pg_hook = PostgresHook(postgres_conn_id="postgres_local")
    sf_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    pg_conn = pg_hook.get_conn()
    pg_cursor = pg_conn.cursor()
    sf_conn = sf_hook.get_conn()
    sf_cursor = sf_conn.cursor()

    # --- last processed timestamp ---
    sf_cursor.execute("""
        SELECT COALESCE(MAX(generated), '1970-01-01T00:00:00Z') 
        FROM RAW_BUS_DATA.VEHICLE_DATA_SNOWFLAKE;
    """)
    last_max_generated = sf_cursor.fetchone()[0]
    print(f"Last max generated in Snowflake: {last_max_generated}")

    # --- batching by generated timestamp ---
    while True:
        pg_cursor.execute("""
            SELECT generated, routeShortName, tripId, routeId, headsign, vehicleCode,
                   vehicleService, vehicleId, speed, direction, delay,
                   scheduledTripStartTime, lat, lon, gpsQuality
            FROM vehicle_data
            WHERE generated > %s
            ORDER BY generated ASC
            LIMIT %s
        """, (last_max_generated, BATCH_SIZE))

        rows = pg_cursor.fetchall()
        if not rows:
            break

        # --- MERGE INTO Snowflake ---
        merge_query = """
            MERGE INTO RAW_BUS_DATA.VEHICLE_DATA_SNOWFLAKE AS target
            USING (
                SELECT column1 AS generated, column2 AS routeShortName, column3 AS tripId,
                       column4 AS routeId, column5 AS headsign, column6 AS vehicleCode,
                       column7 AS vehicleService, column8 AS vehicleId, column9 AS speed,
                       column10 AS direction, column11 AS delay, column12 AS scheduledTripStartTime,
                       column13 AS lat, column14 AS lon, column15 AS gpsQuality
                FROM VALUES %s
            ) AS source
            ON target.vehicleId = source.vehicleId AND target.generated = source.generated
            WHEN MATCHED THEN UPDATE SET
                target.delay = source.delay,
                target.speed = source.speed,
                target.lat = source.lat,
                target.lon = source.lon
            WHEN NOT MATCHED THEN
                INSERT (generated, routeShortName, tripId, routeId, headsign, vehicleCode,
                        vehicleService, vehicleId, speed, direction, delay,
                        scheduledTripStartTime, lat, lon, gpsQuality)
                VALUES (source.generated, source.routeShortName, source.tripId, source.routeId,
                        source.headsign, source.vehicleCode, source.vehicleService,
                        source.vehicleId, source.speed, source.direction, source.delay,
                        source.scheduledTripStartTime, source.lat, source.lon, source.gpsQuality);
        """
        sf_cursor.execute(merge_query, (rows,))
        sf_conn.commit()

        last_max_generated = max(row[0] for row in rows)
        print(f"Batch transferred, last generated: {last_max_generated}")

    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()
    print("All data transferred successfully.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "postgres_to_snowflake_incremental",
    default_args=default_args,
    description="Daily incremental transfer from Postgres to Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 12),
    catchup=False,
) as dag:

    transfer_task = PythonOperator(
        task_id="transfer_postgres_to_snowflake",
        python_callable=transfer_data_batch
    )
