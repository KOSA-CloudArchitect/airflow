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


def log_job_id_callback(context):
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    logging.info(f"[call_crawler] job_id={job_id}")


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

    # call_crawler = HttpOperator(
    #     task_id="call_crawler",
    #     # http_conn_id="crawler_local",
    #     # endpoint="/collect",
    #     http_conn_id="httpbin",   # Airflow Connection에서 httpbin 등록 필요
    #     endpoint="post",
    #     method="POST",
    #     data='{"job_id":"{{ dag_run.conf.get("job_id") }}"}',
    #     headers={"Content-Type": "application/json"},
    # )
    call_crawler = HttpOperator(
        task_id="call_crawler",
        http_conn_id="httpbin",
        endpoint="post",
        method="POST",
        data='{"job_id":"{{ dag_run.conf.get(\'job_id\') }}"}',
        headers={"Content-Type": "application/json"},
        on_execute_callback=log_job_id_callback,   # ← 추가
        log_response=True,                         # ← 응답도 로그에 찍고 싶으면
    )


    #expected = "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}"

    wait_for_done = AwaitMessageSensor(
        task_id="wait_for_done",
        kafka_config_id="new_kafka",                       # 트리거 로그에 GET .../connections/new_kafka 찍히는 그 커넥션
        topics=["crawler-done-topic"],
        apply_function="include.kafka_filters.lll",
        apply_function_kwargs={"expected_job_id": "test-001"},  # ← 핵심!
        xcom_push_key="retrieved_message",                 # 원하면 수신 payload를 XCom에 보관
        retries=0,
    )
    # wait_for_done = AwaitMessageSensor(
    #     task_id="wait_for_done",
    #     kafka_config_id = "crawl_kafka_job",
    #     topics=["crawler-done-topic"],    # 완료 이벤트를 보내는 topic
    #     apply_function="include.kafka_filters.kafka_message_check",
    #     pool="crawler_pool",        # 여기서만 pool 점유sdsds
    # )

    log_task >> call_crawler >> wait_for_done