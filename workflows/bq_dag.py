# import required libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# define the variables
PROJECT_ID = "project-15f498fb-28c2-4528-bc7"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# read SQL files
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
    return sql_query

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# define default arguments
ARGS = {
    "owner": "Amrutha",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["•••@example.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# define the DAG
with DAG(
    dag_id="bigquery_workflow",
    schedule_interval=None,
    description="A workflow to execute BigQuery SQL scripts for bronze, silver, and gold layers",
    default_args=ARGS,
    tags = ["gcs", "bq", "etl", "marvel"]
) as dag:

    # Task to execute bronze layer SQL
    execute_bronze_query = BigQueryInsertJobOperator(
        task_id="execute_bronze_query",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
    )
    # Task to execute silver layer SQL
    execute_silver_query = BigQueryInsertJobOperator(
        task_id="execute_silver_query",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
    )
    # Task to execute gold layer SQL
    execute_gold_query = BigQueryInsertJobOperator(
        task_id="execute_gold_query",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH"
            }
        },
    )

# define task dependencies
execute_bronze_query >> execute_silver_query >> execute_gold_query #[bronze table is ready before silver table creation, silver table is ready before gold table creation]
