# from datetime import datetime

# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.sdk import DAG

# with DAG(
#     dag_id="my_first_dag",
#     description="a simple tutorial DAG",
#     start_date=datetime(2025, 11, 19),
#     schedule="* * * * *",
#     catchup=False,
# ) as dag:
#     bash_task = BashOperator(task_id="print_date_task", bash_command="echo 'VASCO!'")

# dags/batch_grades_dag.py
from pathlib import Path
from typing import cast

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection, cursor
from web3 import Web3
from web3_client import build_contract, get_web3, load_abi_from_file, sign_and_send_tx

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
    def ensure_batch_table():
        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS batch_grades (
                id SERIAL PRIMARY KEY,

                student_address VARCHAR(42) NOT NULL,
                course_code VARCHAR(10) NOT NULL,
                discipline_code VARCHAR(10) NOT NULL,
                semester SMALLINT NOT NULL,
                year SMALLINT NOT NULL,
                grade NUMERIC(4, 2) NOT NULL,
                attendance SMALLINT NOT NULL,
                status BOOLEAN NOT NULL,

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
                INSERT INTO batch_grades
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

    @task
    def fetch_unprocessed(limit=50):
        """Busca até 50 registros não processados (chunk)."""
        pg = PostgresHook(postgres_conn_id="postgres_default")
        df = pg.get_pandas_df(
            """
            SELECT * FROM batch_grades
            WHERE processed = FALSE
            ORDER BY id
            LIMIT %s
        """,
            parameters=(limit,),
        )

        return df.to_dict(orient="records")

    @task
    def send_to_blockchain(rows):
        if not rows:
            return None

        w3 = get_web3()

        abi = load_abi_from_file(
            "/opt/airflow/dags/../build/AcademicRecordStorage.abi.json"
        )
        contract_address = Variable.get("CONTRACT_ADDRESS")
        pk = Variable.get("PRIVATE_KEY")

        acct = w3.eth.account.from_key(pk)
        contract = build_contract(w3, contract_address, abi)

        payload = [
            {
                "studentAddress": Web3.to_checksum_address(r["student_address"]),
                "courseCode": r["course_code"],
                "disciplineCode": r["discipline_code"],
                "semester": r["semester"],
                "year": r["year"],
                "grade": r["grade"],
                "attendance": r["attendance"],
                "status": r["status"],
            }
            for r in rows
        ]

        tx = contract.functions.addBatchGrades(acct.address, payload).build_transaction(
            {
                "from": acct.address,
                "nonce": w3.eth.get_transaction_count(acct.address),
                "gas": 1_500_000,
                "maxPriorityFeePerGas": w3.to_wei(2, "gwei"),
                "maxFeePerGas": w3.to_wei(50, "gwei"),
            }
        )

        tx_hash = sign_and_send_tx(w3, tx, pk)

        return tx_hash

    @task
    def update_status(rows, tx_hash):
        if not tx_hash:
            return False

        pg = PostgresHook(postgres_conn_id="postgres_default")
        conn = cast(connection, pg.get_conn())
        cur = cast(cursor, conn.cursor())

        for r in rows:
            cur.execute(
                """
                UPDATE batch_grades
                SET processed = TRUE,
                    processed_at = NOW(),
                    tx_hash = %s
                WHERE id = %s
            """,
                (tx_hash, r["id"]),
            )

        conn.commit()
        return True

    records = load_file()
    ensure_batch_table()
    write_to_postgres(records)

    rows = fetch_unprocessed()
    tx_hash = send_to_blockchain(rows)
    update_status(rows, tx_hash)


pipeline = batch_grades_pipeline()
