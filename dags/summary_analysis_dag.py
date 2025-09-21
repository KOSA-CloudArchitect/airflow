from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.xcom_arg import XComArg

from datetime import datetime, timedelta
import logging
import json
import pytz
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

def publish_summary_request(**context):
    """Overall Summary Request Topic에 메시지 발행"""
    dag_run = context.get("dag_run")
    conf = dag_run.conf or {}
    
    # Redshift COPY DAG에서 전달받은 데이터 추출
    job_id = conf.get('job_id', 'unknown')
    execution_time = conf.get('execution_time', '')
    copy_completion_time = conf.get('copy_completion_time', '')
    source_dag = conf.get('source_dag', 'unknown')
    
    # 한국 시간으로 현재 시간 계산
    kst = pytz.timezone('Asia/Seoul')
    current_time_kst = datetime.now(kst)
    
    # Overall Summary Request 메시지 구성
    summary_request_message = {
        "job_id": job_id,
        "request_type": "overall_summary",
        "execution_time": execution_time,  # realtime_pipeline_monitor_dag 실행 시간
        "copy_completion_time": copy_completion_time,  # Redshift COPY 완료 시간
        "request_timestamp": current_time_kst.isoformat(),
        "metadata": {
            "source_dag": source_dag,
            "trigger_point": "redshift_copy_completed",
            "analysis_type": "llm_summary",
            "worker_id": "airflow-summary-worker",
            "pipeline_execution_time": execution_time  # 원본 파이프라인 실행 시간
        }
    }
    
    logging.info(f"[Summary Request] Publishing message for job_id={job_id}")
    logging.info(f"[Summary Request] Execution time from pipeline: {execution_time}")
    logging.info(f"[Summary Request] Copy completion time: {copy_completion_time}")
    logging.info(f"[Summary Request] Message: {summary_request_message}")
    
    # Kafka Producer를 사용하여 실제 메시지 발행
    try:
        # Kafka 설정
        kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        topic_name = "overall-summary-request-topic"
        
        if not kafka_servers:
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS 환경변수가 설정되지 않았습니다.")
        
        logging.info(f"[Summary Request] Connecting to Kafka: {kafka_servers}")
        logging.info(f"[Summary Request] Publishing to topic: {topic_name}")
        
        # Kafka Producer 초기화
        producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            acks='all',
            request_timeout_ms=30000,
            delivery_timeout_ms=120000
        )
        
        # 메시지 전송
        future = producer.send(topic_name, summary_request_message)
        
        # 전송 완료 대기
        record_metadata = future.get(timeout=30)
        
        # Producer 정리
        producer.flush()
        producer.close()
        
        logging.info(f"[Summary Request] ✅ Message published successfully!")
        logging.info(f"[Summary Request] Topic: {record_metadata.topic}")
        logging.info(f"[Summary Request] Partition: {record_metadata.partition}")
        logging.info(f"[Summary Request] Offset: {record_metadata.offset}")
        
        print(f"📤 ✅ Successfully published to {topic_name}: {json.dumps(summary_request_message, indent=2)}", flush=True)
        
    except KafkaError as e:
        error_msg = f"Kafka 메시지 발행 실패: {str(e)}"
        logging.error(f"[Summary Request] ❌ {error_msg}")
        print(f"📤 ❌ Kafka publishing failed: {error_msg}", flush=True)
        raise e
    except Exception as e:
        error_msg = f"메시지 발행 중 예상치 못한 오류: {str(e)}"
        logging.error(f"[Summary Request] ❌ {error_msg}")
        print(f"📤 ❌ Unexpected error: {error_msg}", flush=True)
        raise e
    
    # XCom에 메시지 저장 (디버깅용)
    context['task_instance'].xcom_push(key='summary_request_message', value=summary_request_message)
    
    return summary_request_message

def handle_summary_failure(context):
    """요약 분석 실패 처리 함수"""
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    task_id = context.get("task_instance").task_id
    
    logging.error(f"Summary analysis failed for job {job_id} at task {task_id}")
    print(f"🚨 Summary analysis failure: Job {job_id} failed at {task_id}", flush=True)
    
    # Discord/Slack 알림 로직 추가 가능
    # send_summary_failure_alert(context=context, job_id=job_id, task_id=task_id)

def notify_summary_completion(**context):
    """요약 분석 완료 알림"""
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    
    logging.info(f"Summary analysis completed successfully for job {job_id}")
    print(f"🎉 Summary analysis completed for job {job_id}!", flush=True)
    
    return f"Summary analysis completed for job {job_id}"

# DAG 정의
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="summary_analysis_dag",
    default_args=default_args,
    description="전체 리뷰 요약 분석 요청 및 완료 감지",
    schedule=None,
    catchup=False,
    tags=["summary", "analysis", "kafka", "llm"]
) as dag:

    # 1. Overall Summary Request Topic에 메시지 발행
    publish_summary_request_task = PythonOperator(
        task_id="publish_summary_request",
        python_callable=publish_summary_request,
    )

    # 2. Control Topic에서 요약 완료 메시지 센싱 대기
    wait_summary_completion = AwaitMessageSensor(
        task_id="wait_summary_completion",
        kafka_config_id="job-control-topic",
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "summary"
        ],
        poll_timeout=1,
        poll_interval=30,  # 30초마다 체크
        execution_timeout=timedelta(minutes=30),  # 30분 타임아웃
        xcom_push_key="summary_completion_message",
        retries=0,
        on_failure_callback=handle_summary_failure
    )

    # 3. 완료 알림
    notify_completion = PythonOperator(
        task_id="notify_summary_completion",
        python_callable=notify_summary_completion,
    )

    # 작업 순서 정의
    publish_summary_request_task >> wait_summary_completion >> notify_completion

"""
DAG 실행 방법:

1. Redshift COPY 완료 후 자동 트리거:
   airflow dags trigger summary_analysis_dag --conf '{
     "job_id": "job-2024-001",
     "execution_time": "2024-01-15T13:30:00+09:00",
     "copy_completion_time": "2024-01-15T14:45:00+09:00",
     "redshift_copy_completed": true,
     "source_dag": "redshift_s3_copy_pipeline"
   }'

2. 수동 실행:
   airflow dags trigger summary_analysis_dag --conf '{
     "job_id": "job-2024-001",
     "execution_time": "2024-01-15T13:30:00+09:00"
   }'

3. Airflow UI에서 수동 실행:
   - DAG 페이지에서 "Trigger DAG w/ Config" 클릭
   - Configuration JSON에 위 예시 입력

주의사항:
- job_id는 Control 토픽의 메시지와 일치해야 함
- execution_time은 realtime_pipeline_monitor_dag의 실행 시간
- Overall Summary Request Topic 메시지 발행 후 Control Topic에서 요약 완료 메시지 대기
- 타임아웃: 30분 (LLM 분석 시간 고려)
- 실패 시 자동으로 에러 로깅 및 알림 처리
- Kafka 연결 ID는 'job-control-topic'으로 설정 필요
- KAFKA_BOOTSTRAP_SERVERS 환경변수 설정 필요

메시지 구조:
- Overall Summary Request Topic: job_id, execution_time, copy_completion_time 포함
- Control Topic: summary 단계 완료 메시지 센싱

Kafka Producer 설정:
- 토픽: overall-summary-request-topic
- 직렬화: JSON (UTF-8 인코딩)
- 재시도: 3회
- ACK: all (모든 복제본 확인)
- 타임아웃: 30초 (요청), 120초 (전송)
"""
