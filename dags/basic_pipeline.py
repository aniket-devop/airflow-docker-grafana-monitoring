from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="basic_pipeline",
    description="A simple Start -> Task1 -> Task2 -> Task3 -> Success pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["client-project", "basic"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting the pipeline"',
    )

    task1 = BashOperator(
        task_id="task1",
        bash_command='echo "Running Task 1"',
    )

    task2 = BashOperator(
        task_id="task2",
        bash_command='echo "Running Task 2"',
    )

    task3 = BashOperator(
        task_id="task3",
        bash_command='echo "Running Task 3"',
    )

    success = BashOperator(
        task_id="success",
        bash_command='echo "Pipeline completed successfully"',
    )

    start >> task1 >> task2 >> task3 >> success
