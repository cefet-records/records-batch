import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

FILE_PATH = "/opt/airflow/data/batches/courses.csv"


@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1),
    catchup=False,
    tags=["batch", "courses"],
)
def batch_courses_pipeline():
    @task
    def load_file():
        df = pd.read_csv(FILE_PATH)
        df.columns = [c.strip().lower() for c in df.columns]
        return df.to_dict(orient="records")

    @task
    def create_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS courses (
                id SERIAL PRIMARY KEY,
                code VARCHAR(10) NOT NULL,
                name VARCHAR(255) NOT NULL,
                course_type VARCHAR(50) NOT NULL,
                number_of_semesters SMALLINT NOT NULL
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
                INSERT INTO courses
                (code, name, course_type, number_of_semesters)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    r["code"],
                    r["name"],
                    r["coursetype"],
                    int(r["numberofsemesters"]),
                ),
            )

        conn.commit()
        return True

    records = load_file()
    ensure = create_table()
    write = write_to_table(records)

    records >> ensure >> write


pipeline = batch_courses_pipeline()
