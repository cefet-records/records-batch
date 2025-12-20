from pathlib import Path
from typing import cast

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection, cursor

FILE_PATH = "/opt/airflow/data/batches/grade.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["blockchain", "batch"],
)
def batch_grades_pipeline():
    @task
    def load_file():
        ext = Path(FILE_PATH).suffix.lower()

        if ext == ".csv":
            df = pd.read_csv(FILE_PATH, sep=",")
            df.columns = [c.strip().lower() for c in df.columns]

        else:
            raise ValueError("Formato inválido (use CSV)")

        return df.to_dict(orient="records")

    @task
    def ensure_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS grades (
                id SERIAL PRIMARY KEY,

                student_address VARCHAR(42) NOT NULL,
                course_code VARCHAR(10) NOT NULL,
                discipline_code VARCHAR(10) NOT NULL,
                semester SMALLINT NOT NULL,
                year SMALLINT NOT NULL,
                grade NUMERIC(4, 2) NOT NULL,
                attendance SMALLINT NOT NULL,
                status BOOLEAN NOT NULL
            );
            """
        )

        conn.commit()

    @task
    def write_to_table(records):
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = cast(connection, pg.get_conn())
        cur = cast(cursor, conn.cursor())

        for r in records:
            print(records)
            semester = int(r["semester"])
            year = int(r["year"])
            grade = int(r["grade"])
            attendance = int(r["attendance"])
            status = r["status"] == "true"

            cur.execute(
                """
                INSERT INTO grades
                (student_address, course_code, discipline_code,
                 semester, year, grade, attendance, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
                (
                    r["studentaddress"],
                    r["coursecode"],
                    r["disciplinecode"],
                    semester,
                    year,
                    grade,
                    attendance,
                    status,
                ),
            )

        conn.commit()
        return True

    records = load_file()
    ensure = ensure_table()
    write = write_to_table(records)

    records >> ensure >> write


pipeline = batch_grades_pipeline()
