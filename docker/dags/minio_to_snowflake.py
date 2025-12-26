import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv


#load environment variables
load_dotenv()


# ------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET", "bronze-transactions")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

RAW_PREFIX = ""                  # e.g. "raw/"
PROCESSED_PREFIX = "processed/"

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")  
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_TABLE = "BRONZE_STOCK_QUOTES_RAW"

# ------------------------------------------------------
# MINIO CLIENT
# ------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

# ------------------------------------------------------
# TASK 1: DOWNLOAD FILES FROM MINIO
# ------------------------------------------------------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = get_s3_client()

    response = s3.list_objects_v2(Bucket=BUCKET)
    objects = response.get("Contents", [])

    local_files = []
    source_keys = []

    print("object downloas from minio:",  objects)

    for obj in objects:
        key = obj["Key"]

        # Skip already processed files
        if key.startswith(PROCESSED_PREFIX):
            continue

        local_path = os.path.join(LOCAL_DIR, os.path.basename(key))
        s3.download_file(BUCKET, key, local_path)

        print(f"⬇️ Downloaded {key} → {local_path}")

        local_files.append(local_path)
        source_keys.append(key)

    return {
        "local_files": local_files,
        "source_keys": source_keys
    }

# ------------------------------------------------------
# TASK 2: LOAD FILES INTO SNOWFLAKE
# ------------------------------------------------------
def load_to_snowflake(**context):
    payload = context["ti"].xcom_pull(task_ids="download_minio")
    local_files = payload["local_files"]

    if not local_files:
        print("⚠️ No files to load.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    # Ensure table exists (Bronze = raw JSON)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
            v VARIANT
        )
    """)

    # Upload files to Snowflake internal stage
    for file_path in local_files:
        cur.execute(f"PUT file://{file_path} @%{SNOWFLAKE_TABLE}")
        print(f"📤 Uploaded {file_path} to Snowflake stage")

    # Load into Bronze table
    cur.execute(f"""
        COPY INTO {SNOWFLAKE_TABLE}
        FROM @%{SNOWFLAKE_TABLE}
        FILE_FORMAT = (TYPE = JSON)
    """)

    print("✅ COPY INTO Snowflake completed")

    cur.close()
    conn.close()

# ------------------------------------------------------
# TASK 3: MOVE PROCESSED FILES IN MINIO
# ------------------------------------------------------
def move_processed_files(**context):
    payload = context["ti"].xcom_pull(task_ids="download_minio")
    source_keys = payload["source_keys"]

    if not source_keys:
        print("⚠️ No files to move.")
        return

    s3 = get_s3_client()

    for key in source_keys:
        filename = os.path.basename(key)
        new_key = f"{PROCESSED_PREFIX}{filename}"

        print(f"🚚 Moving {key} → {new_key}")

        # Copy to processed/
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": key},
            Key=new_key,
        )

        # Delete original
        s3.delete_object(Bucket=BUCKET, Key=key)

        print(f"✅ Moved {key} to {new_key}")

# ------------------------------------------------------
# AIRFLOW DAG
# ------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 19),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="minio_to_snowflake_bronze",
    default_args=default_args,
    description="Load raw data from MinIO to Snowflake Bronze and archive processed files",
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    download_minio_task = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_snowflake_task = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    move_files_task = PythonOperator(
        task_id="move_processed_files",
        python_callable=move_processed_files,
        provide_context=True,
    )

    download_minio_task >> load_snowflake_task >> move_files_task
