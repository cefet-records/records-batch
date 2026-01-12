import pandas as pd
import pendulum
import tempfile
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
from dotenv import load_dotenv
load_dotenv()

S3_BUCKET = os.getenv("BUCKET_NAME")
S3_KEY = "uploads/students.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "students"],
)
def batch_students_pipeline():
    @task
    def load_file():
        s3 = S3Hook(aws_conn_id="aws_default")

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file = s3.download_file(
                key=S3_KEY,
                bucket_name=S3_BUCKET,
                local_path=tmp_dir,
            )

            df = pd.read_csv(
                local_file,
                sep=",",
                engine="python",
                encoding="utf-8", # troque para latin1 se der erro
            )

        df.columns = [c.strip().lower() for c in df.columns]
        return df.to_dict(orient="records")


    @task
    def ensure_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    student_address VARCHAR(42) NOT NULL,
                    institution_address VARCHAR(42) NOT NULL
                );
                """
        )

        conn.commit()

    @task
    def write_to_table(records):
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        for r in records:
            cur.execute(
                """
                INSERT INTO students
                (student_address, institution_address)
                VALUES (%s,%s)
            """,
                (
                    r["studentaddress"],
                    r["institutionaddress"],
                ),
            )

        conn.commit()
        return True

    records = load_file()
    ensure = ensure_table()
    write = write_to_table(records)

    records >> ensure >> write


pipeline = batch_students_pipeline()
