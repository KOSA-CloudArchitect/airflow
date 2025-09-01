from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from datetime import datetime, timedelta
import logging
import json

def log_request(**context):
    conf = context['dag_run'].conf or {}
    logging.info(f"[crawler DAG Triggered] Received conf: {conf}")

def kafka_message_check(message, **context):
    """Kafka 메시지가 crawler 완료 신호인지 확인"""
    try:
        payload = json.loads(message.value().decode("utf-8"))
        job_id = context["dag_run"].conf.get("job_id")
        return payload.get("job_id") == job_id and payload.get("status") == "done"
    except Exception:
        return False

with DAG(
    dag_id="crawler_trigger_dag",
    default_args={"owner": "airflow"},
    start_date=datetime(2025, 8, 30),
    schedule=None,
    catchup=False,
    tags=["crawler", "kafka"],
) as dag:

    log_task = PythonOperator(
        task_id="log_request",
        python_callable=log_request,
    )

    call_crawler = HttpOperator(
        task_id="call_crawler",
        http_conn_id="crawler_local",
        endpoint="/collect",
        method="POST",
        data='{"job_id":"{{ dag_run.conf.get("job_id") }}"}',
        headers={"Content-Type": "application/json"},
    )

    wait_for_done = AwaitMessageSensor(
        task_id="wait_for_done",
        topics=["crawler_done"],    # 완료 이벤트를 보내는 topic
        apply_function=kafka_message_check,
        pool="crawler_pool",        # 여기서만 pool 점유
    )

    log_task >> call_crawler >> wait_for_done