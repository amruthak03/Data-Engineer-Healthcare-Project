## import required modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator, DataprocStopClusterOperator, DataprocSubmitJobOperator

# define the variables
PROJECT_ID = "project-15f498fb-28c2-4528-bc7"
REGION = "us-central1"
CLUSTER_NAME = "my-demo-cluster"
COMPOSER_BUCKET = "us-central1-demo-instance-41884e29-bucket" # got it from composer environment under DAGs folder
BIGQUERY_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "jar_file_uris": [BIGQUERY_JAR]
        },
}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_2,
        "jar_file_uris": [BIGQUERY_JAR]
        },
}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_3,
        "jar_file_uris": [BIGQUERY_JAR]
    },
}

GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_4,
        "jar_file_uris": [BIGQUERY_JAR]
        },
}

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
    dag_id="pyspark_dataproc_workflow",
    schedule_interval=None,
    description="A workflow to start a Dataproc cluster, run PySpark jobs, and stop the cluster", # not creating cluster every time, start and stop it in pipeline
    default_args=ARGS,
    tags = ["pyspark", "dataproc", "etl", "marvel"]
) as dag:
    #define the tasks
    #start the cluster
    start_cluster = DataprocStartClusterOperator(
        task_id="start_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    pyspark_task1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID
    )
    pyspark_task2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID
    )
    pyspark_task3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3",
        job=PYSPARK_JOB_3,
        region=REGION,
        project_id=PROJECT_ID
    )
    pyspark_task4 = DataprocSubmitJobOperator(
        task_id="pyspark_task_4",
        job=PYSPARK_JOB_4,
        region=REGION,
        project_id=PROJECT_ID
    )
    # stop the cluster
    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )
# define the task dependencies
start_cluster >> pyspark_task1 >> pyspark_task2 >> pyspark_task3 >> pyspark_task4 >> stop_cluster