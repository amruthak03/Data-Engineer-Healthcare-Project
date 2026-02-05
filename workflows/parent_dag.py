# import required libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator # used to trigger another DAG

# define default arguments
ARGS = {
    "owner": "Amrutha",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["•••@example.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# define the parent DAG
with DAG(
    dag_id="parent_workflow",
    schedule_interval="0 5 * * *",  # daily at 5 AM
    description="A parent workflow to trigger PySpark and BigQuery workflows",
    default_args=ARGS,
    tags = ["parent", "orchestration", "etl"]
) as dag:

    # Task to trigger the PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dataproc_workflow",  # Ensure this matches the actual DAG ID
        wait_for_completion=True
    )

    # Task to trigger the BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_workflow",  # Ensure this matches the actual DAG ID
        wait_for_completion=True
    )

# Define the sequence of tasks
trigger_pyspark_dag >> trigger_bigquery_dag