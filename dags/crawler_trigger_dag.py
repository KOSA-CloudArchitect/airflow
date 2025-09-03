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

def kafka_message_check(expected_job_id, message):
    raw = message.value()
    text = raw.decode("utf-8","replace") if isinstance(raw,(bytes,bytearray)) else str(raw)
    print("[kafka_sensor] received:", text[:1000], flush=True)

    try:
        payload = json.loads(text)
    except Exception as e:
        print("[kafka_sensor] invalid JSON:", e, flush=True)
        return False

    recv = payload.get("job_id")
    status = str(payload.get("status","")).lower()
    ok = (recv == expected_job_id and status == "done")
    print(f"[kafka_sensor] check: expected={expected_job_id} received={recv} status={status} -> {ok}", flush=True)
    return payload if ok else None


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
        http_conn_id="httpbin",
        endpoint="post",
        method="POST",
        data='{"job_id":"{{ dag_run.conf.get(\'job_id\') }}"}',
        headers={"Content-Type": "application/json"},
        on_execute_callback=log_job_id_callback,   # ← 추가
        log_response=True,                         # ← 응답도 로그에 찍고 싶으면
    )


    expected = "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}"
    wait_for_done = AwaitMessageSensor(
        task_id="wait_for_done",
        kafka_config_id="realtime-review-collection-topic",
        topics=["realtime-review-collection-topic"],
        apply_function="crawler_trigger_dag.kafka_message_check",
        apply_function_args=[expected],     # ← 여기! args로 넘김
        poll_timeout=1,
        poll_interval=5,
        execution_timeout=timedelta(minutes=10),
        xcom_push_key="retrieved_message",
        retries=0,
        pool="crawler_pool"
    )


    log_task >> call_crawler >> wait_for_done