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

def _safe(obj, meth, default=None):
    try: return getattr(obj, meth)()
    except Exception: 
        return default
        
def kafka_message_check(message=None, **context):
    """Kafka 메시지가 crawler 완료 신호인지 확인"""
    try:
        # 템플릿 렌더링 단계에서는 message가 None로 들어옵니다.
        if message is None:
            return False

        raw = message.value()
        text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)

        # 여기까지 오면 '메시지를 Kafka에서 꺼내옴'이 확정 → 반드시 한 줄 찍힘
        logging.info(
            "[kafka_sensor] received: topic=%s partition=%s offset=%s key=%s headers=%s value=%s",
            _safe(message, "topic", "<??>"),
            _safe(message, "partition", "<??>"),
            _safe(message, "offset", "<??>"),
            _safe(message, "key"),
            _safe(message, "headers"),
            text[:2000]
        )
        payload = json.loads(message.value().decode("utf-8"))
        logging.info(f"[kafka_message_check] message={payload}")

        dag_run = context.get("dag_run")
        job_id_from_conf = (dag_run.conf or {}).get("job_id") if dag_run else None
        logging.info(f"[kafka_message_check] message={job_id_from_conf}")
        return payload.get("job_id") == job_id_from_conf and payload.get("status") == "done"
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

    wait_for_done = AwaitMessageSensor(
        task_id="wait_for_done",
        kafka_config_id = "crawl_kafka_job",
        topics=["crawler-done-topic"],    # 완료 이벤트를 보내는 topic
        apply_function="crawler_trigger_dag.kafka_message_check",
        pool="crawler_pool",        # 여기서만 pool 점유sdsds
    )

    log_task >> call_crawler >> wait_for_done