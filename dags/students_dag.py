import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/opt/airflow/data/batches/students.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "students"],
)
def batch_students_pipeline():
    @task
    def load_file():
        df = pd.read_csv(FILE_PATH)
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
