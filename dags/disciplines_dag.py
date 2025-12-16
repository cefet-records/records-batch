# dags/batch_disciplines_dag.py
import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/opt/airflow/data/batches/disciplines.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "disciplines"],
)
def batch_disciplines_pipeline_dag():

    @task
    def load_file():
        df = pd.read_csv(FILE_PATH)
        df.columns = [c.strip().lower() for c in df.columns]
        return df.to_dict(orient="records")
    @task
    def ensure_batch_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()
        
        cur.execute(
            """
                CREATE TABLE IF NOT EXISTS batch_disciplines (
                    id SERIAL PRIMARY KEY,
        
                    discipline_code VARCHAR(10) NOT NULL,
                    course_code VARCHAR(10) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    syllabus TEXT,
                    workload SMALLINT NOT NULL,
                    credit_count SMALLINT NOT NULL,
        
                    processed BOOLEAN DEFAULT FALSE,
                    tx_hash TEXT,
        
                    created_at TIMESTAMP DEFAULT NOW(),
                    processed_at TIMESTAMP
                );
                """
            )
        
        conn.commit()

    @task
    def write_to_postgres(records):
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        for r in records:
            cur.execute(
                """
                INSERT INTO batch_disciplines
                (course_code, discipline_code, name, syllabus, workload, credit_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    r["coursecode"],
                    r["disciplinecode"],
                    r["name"],
                    r["syllabus"],
                    int(r["workload"]),
                    int(r["creditcount"]),
                ),
            )

        conn.commit()
        return True

    records = load_file()
    ensure_batch_table()
    write_to_postgres(records)


batch_disciplines_pipeline = batch_disciplines_pipeline_dag()
