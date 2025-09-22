from airflow import DAG
from airflow.operators.python import PythonOperator
try:
    from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
    
except ImportError:
    try:
        from airflow.providers.apache.kafka.sensors.await_message import AwaitMessageSensor
    except ImportError:
        from airflow.providers.apache.kafka.sensors import AwaitMessageSensor
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json, logging

def send_to_kafka(**context):
    """Kafka에 메시지를 직접 발행하는 함수"""
    # Kubernetes 환경에서 정확한 Kafka 서비스 주소 사용
    conf = {"bootstrap.servers": "PLAINTEXT://my-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"}
    producer = Producer(conf)

    ti = context["ti"]
    summary_message = ti.xcom_pull(task_ids="prepare_summary_message", key="summary_request_message")
    job_id = summary_message.get("job_id", "unknown")

    json_str = json.dumps(summary_message, ensure_ascii=False)

    producer.produce(
        topic="overall-summary-request-topic",
        key=job_id,
        value=json_str
    )
    producer.flush()
    logging.info(f"✅ Sent Kafka message for job_id={job_id}: {json_str}")

def prepare_summary_request_message(**context):
    """Overall Summary Request 메시지 준비"""
    dag_run = context.get("dag_run")
    conf = dag_run.conf or {}
    
    # Redshift COPY DAG에서 전달받은 데이터 추출
    job_id = conf.get("job_id", "unknown")
    execution_time = conf.get("execution_time", datetime.now().isoformat())
    copy_completion_time = conf.get("copy_completion_time", datetime.now().isoformat())
    source_dag = conf.get("source_dag", "unknown")
    trigger_point = conf.get("trigger_point", "unknown")
    redshift_copy_completed = conf.get("redshift_copy_completed", False)
    
    logging.info(f"[Summary Message] Received conf: {conf}")
    logging.info(f"[Summary Message] Job ID: {job_id}")
    logging.info(f"[Summary Message] Execution Time: {execution_time}")
    
    # Overall Summary Request 메시지 구성
    summary_message = {
        "job_id": job_id,
        "execution_time": execution_time,
        "copy_completion_time": copy_completion_time,
        "source_dag": source_dag,
        "trigger_point": trigger_point,
        "redshift_copy_completed": redshift_copy_completed,
        "request_type": "overall_summary",
        "timestamp": datetime.now().isoformat(),
        "status": "requested"
    }
    
    # XCom에 저장
    context['task_instance'].xcom_push(
        key='summary_request_message',
        value=summary_message
    )
    
    logging.info(f"[Summary Message] Prepared message: {summary_message}")
    return summary_message

def handle_summary_failure(context):
    """요약 실패 시 처리 함수"""
    logging.error(f"[Summary Failure] Summary processing failed: {context}")
    # 필요시 알림이나 재시도 로직 추가 가능

with DAG(
    dag_id="summary_analysis_dag_simple",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Simple Summary Analysis DAG (Direct Kafka Producer) - v1",
    tags=["simple", "kafka", "summary"]
) as dag:

    prepare_summary_message = PythonOperator(
        task_id="prepare_summary_message",
        python_callable=prepare_summary_request_message,
    )

    publish_to_kafka = PythonOperator(
        task_id="publish_to_kafka",
        python_callable=send_to_kafka,
    )

    # Kafka 센싱 태스크 추가 - 요약 완료 메시지 대기
    wait_summary_completion = AwaitMessageSensor(
        task_id="wait_summary_completion",
        kafka_config_id="job-control-topic-overall",
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "summary"
        ],
        poll_timeout=1,
        poll_interval=10,  # 30초마다 체크
        execution_timeout=timedelta(minutes=30),  # 30분 타임아웃
        xcom_push_key="summary_completion_message",
        retries=0,
        on_failure_callback=handle_summary_failure
    )

    prepare_summary_message >> [publish_to_kafka, wait_summary_completion]


