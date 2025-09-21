from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.xcom_arg import XComArg

from datetime import datetime, timedelta
import logging
import json
import pytz
import os

def kafka_producer_function(job_id, **context):
    """Kafka Producer 함수 - ProduceToTopicOperator에서 사용"""
    logging.info(f"[Kafka Producer] Received job_id: {job_id}")
    
    # XCom에서 전체 summary_message 가져오기 시도
    summary_message = None
    
    # 방법 1: task_instance를 통한 XCom 접근
    try:
        task_instance = context.get('task_instance')
        if task_instance:
            summary_message = task_instance.xcom_pull(
                task_ids='prepare_summary_message',
                key='summary_request_message'
            )
            logging.info(f"[Kafka Producer] Retrieved XCom data: {summary_message}")
    except Exception as e:
        logging.warning(f"[Kafka Producer] XCom access failed: {e}")
    
    # XCom 데이터가 있으면 사용, 없으면 job_id로 간단한 메시지 생성
    if summary_message:
        logging.info(f"[Kafka Producer] Using XCom data for job_id: {job_id}")
        message_data = summary_message
    else:
        logging.warning(f"[Kafka Producer] Using fallback message for job_id: {job_id}")
        message_data = {
            "job_id": job_id,
            "timestamp": datetime.now().isoformat(),
            "message": f"Publishing summary request for {job_id}",
            "warning": "XCom data not available"
        }
    
    # JSON 직렬화
    try:
        json_str = json.dumps(message_data, ensure_ascii=False)
        logging.info(f"[Kafka Producer] JSON serialization successful: {json_str}")
    except Exception as e:
        logging.error(f"[Kafka Producer] JSON serialization failed: {e}")
        # 안전한 fallback
        fallback_data = {
            "job_id": str(job_id),
            "timestamp": datetime.now().isoformat(),
            "error": f"JSON serialization failed: {str(e)}"
        }
        json_str = json.dumps(fallback_data, ensure_ascii=False)
    
    return [{
        "key": job_id.encode("utf-8"),
        "value": json_str.encode("utf-8")
    }]

def prepare_summary_request_message(**context):
    """Overall Summary Request 메시지 준비"""
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
    
    logging.info(f"[Summary Request] Preparing message for job_id={job_id}")
    logging.info(f"[Summary Request] Execution time from pipeline: {execution_time}")
    logging.info(f"[Summary Request] Copy completion time: {copy_completion_time}")
    logging.info(f"[Summary Request] Message: {summary_request_message}")
    
    # XCom에 메시지 저장
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

    # 1. Overall Summary Request 메시지 준비
    prepare_summary_message = PythonOperator(
        task_id="prepare_summary_message",
        python_callable=prepare_summary_request_message,
    )

    # 2. Overall Summary Request Topic에 메시지 발행
    publish_summary_request = ProduceToTopicOperator(
        task_id="publish_summary_request",
        topic="overall-summary-request-topic",
        kafka_config_id="overall-summary-request-topic",
        producer_function=kafka_producer_function,
        op_kwargs={
            "job_id": "{{ ti.xcom_pull(task_ids='prepare_summary_message', key='summary_request_message')['job_id'] }}"
        },
    )

    # 3. Control Topic에서 요약 완료 메시지 센싱 대기
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
        poll_interval=30,  # 30초마다 체크
        execution_timeout=timedelta(minutes=30),  # 30분 타임아웃
        xcom_push_key="summary_completion_message",
        retries=0,
        on_failure_callback=handle_summary_failure
    )

    # 4. 완료 알림
    notify_completion = PythonOperator(
        task_id="notify_summary_completion",
        python_callable=notify_summary_completion,
    )

    # 작업 순서 정의
    prepare_summary_message >> publish_summary_request >> wait_summary_completion >> notify_completion

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

Airflow 3.0 Kafka Producer 설정:
- 토픽: overall-summary-request-topic
- 직렬화: JSON (UTF-8 인코딩)
- Producer Operator 사용으로 안정성 향상
- 환경변수 기반 Kafka 서버 설정
"""
