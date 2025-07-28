from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "Review",
    "max_active_tis_per_dag": 4,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email": ["review@example.com"],
    "email_on_failure": True,
}

DAG_NAME = "review_yellow_words_metrics_dag"

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    start_date=datetime(2023, 5, 6),
    schedule="30 3 * * *",
    tags=["Review"],
    access_control={"Quality": {"can_read", "can_delete", "can_edit"}}
) as dag:
    yellow_words_metrics = PythonOperator(
        task_id="yellow_words_metrics_daily",
        python_callable="src.data_project.yellow_words_metrics.calculate_yellow_words_metrics",
    )

    wait_for_review_db_cleaning = ExternalTaskSensor(
        task_id="wait_for_review_db_cleaning",
        timeout=7200,
        external_dag_id="review_daily_cleaning_dag",
        external_task_id="review_db_cleaning",
        execution_delta=timedelta(hours=0),
    )

    wait_for_review_db_cleaning >> yellow_words_metrics