import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv


# -------- Python Callables --------
def download_from_minio():
    load_dotenv()

    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    bucket = os.getenv("MINIO_BUCKET")
    local_dir = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")
    tables = ["customers", "accounts", "transactions"]

    os.makedirs(local_dir, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
    )

    local_files = {}

    for table in tables:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(local_dir, os.path.basename(key))
            s3.download_file(bucket, key, local_file)
            print(f"✅ Downloaded: {key} → {local_file}")
            local_files[table].append(local_file)

    return local_files


def load_to_snowflake(**kwargs):
    load_dotenv()

    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("⚠️ No files found in MinIO. Exiting.")
        return

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"),
        role="SYSADMIN",
    )

    cur = conn.cursor()


    try:

        cur.execute("USE WAREHOUSE COMPUTE_WH")   
        cur.execute("USE DATABASE banking")        
        cur.execute("USE SCHEMA raw")








        for table, files in local_files.items():
            if not files:
                print(f"⚠️ No files for table: {table}. Skipping.")
                continue

            for f in files:
                cur.execute(f"PUT file://{f} @%{table}")
                print(f"✅ Staged: {f} → @{table}")

            copy_sql = f"""
                COPY INTO {table}
                FROM @%{table}
                FILE_FORMAT = (TYPE = PARQUET)
                ON_ERROR = 'CONTINUE'
            """
            cur.execute(copy_sql)
            print(f"✅ Loaded data into Snowflake table: {table}")

    except Exception as e:
        print(f"❌ Error loading data: {e}")
        raise

    finally:
        cur.close()
        conn.close()
        print("🔒 Snowflake connection closed.")


# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet files into Snowflake RAW tables",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2