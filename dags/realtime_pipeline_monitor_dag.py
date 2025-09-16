from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.xcom_arg import XComArg
 
from datetime import datetime, timedelta
import logging
import json
import pytz

# Discord 알림 유틸은 dags/include/discord_notifier.py에 구현
# 준비되면 아래 import의 주석을 해제하세요.
# from include.discord_notifier import send_discord_failure_alert

def log_crawler_callback(context):
    """크롤러 호출 시 job_id 로깅 및 실행 시간 저장"""
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    
    # 한국 시간으로 현재 시간 계산
    kst = pytz.timezone('Asia/Seoul')
    current_time_kst = datetime.now(kst)
    
    logging.info(f"[call_crawler] job_id={job_id}, execution_time={current_time_kst.isoformat()}")
    
    # XCom에 실행 시간 저장
    context['task_instance'].xcom_push(key='crawler_execution_time', value=current_time_kst.isoformat())

 

def determine_crawler_type(conf):
    """크롤링 타입을 결정하는 함수
    
    Args:
        conf: DAG 실행 시 전달받은 설정 데이터
    
    Returns:
        str: 'multi' 또는 'single'
    """
    # url_list가 있으면 다중 상품 크롤링
    if 'url_list' in conf and isinstance(conf['url_list'], list):
        return 'multi'
    
    # url이 있으면 단일 상품 크롤링
    if 'url' in conf:
        return 'single'
    
    # 기본값은 단일 상품 크롤링
    return 'single'

def build_crawler_request_payload(conf):
    """크롤링 타입에 따라 요청 데이터를 준비하는 함수
    
    Args:
        conf: DAG 실행 시 전달받은 설정 데이터
    
    Returns:
        tuple: (endpoint, request_data)
    """
    job_id = conf.get('job_id')
    crawler_type = determine_crawler_type(conf)
    
    if crawler_type == 'multi':
        # 다중 상품 크롤링
        url_list = conf.get('url_list', [])
        if not url_list:
            raise ValueError("다중 상품 크롤링을 위해서는 url_list가 필요합니다.")
        
        endpoint = "/crawl/product_multi"
        request_data = {
            "url_list": url_list,
            "job_id": job_id
        }
        
        logging.info(f"[Multi Crawler] endpoint={endpoint}, url_count={len(url_list)}")
        
    else:
        # 단일 상품 크롤링
        product_id = conf.get('product_id')
        url = conf.get('url')
        review_cnt = conf.get('review_cnt', 0)
        
        if not product_id or not url:
            raise ValueError("단일 상품 크롤링을 위해서는 product_id와 url이 필요합니다.")
        
        endpoint = "/crawl/product_one"
        request_data = {
            "product_id": product_id,
            "url": url,
            "job_id": job_id,
            "review_cnt": review_cnt
        }
        
        logging.info(f"[Single Crawler] endpoint={endpoint}, product_id={product_id}")
    
    return endpoint, request_data

def call_crawler_dynamic(**context):
    """동적으로 크롤러 API를 호출하는 함수"""
    conf = context['dag_run'].conf or {}
    
    try:
        endpoint, request_data = build_crawler_request_payload(conf)
        
        logging.info(f"[Dynamic Crawler] Calling endpoint: {endpoint}")
        logging.info(f"[Dynamic Crawler] Request data: {request_data}")
        
        # 실제 HTTP 요청은 HttpOperator에서 처리되므로 여기서는 데이터만 준비
        return {
            'endpoint': endpoint,
            'data': request_data
        }
        
    except Exception as e:
        logging.error(f"[Dynamic Crawler] Error preparing request: {e}")
        raise

# Control 토픽 메시지 필터링 함수는 include/kafka_filters.py에서 import

def handle_step_failure(context):
    """단계별 실패 처리 함수"""
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    task_id = context.get("task_instance").task_id
    
    # task_id에서 단계 추출 (wait_collection -> collection)
    step = task_id.replace("wait_", "")
    
    logging.error(f"Job {job_id} failed at {step} step")
    print(f"🚨 Pipeline failure: Job {job_id} failed at {step} step", flush=True)
    
    # Discord로 실패 알림 전송 (실사용 전까지 주석 유지)
    # try:
    #     send_discord_failure_alert(context=context, job_id=job_id, step=step)
    # except Exception as e:
    #     logging.error(f"[Discord Notify] Failed to send alert: {e}")

def prepare_redshift_trigger_data(**context):
    """Redshift DAG 트리거를 위한 데이터 준비"""
    dag_run = context.get("dag_run")
    job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
    
    # call_crawler에서 저장한 실행 시간 가져오기
    crawler_execution_time = context['task_instance'].xcom_pull(
        task_ids='call_crawler', 
        key='crawler_execution_time'
    )
    
    if crawler_execution_time:
        execution_time_kst = datetime.fromisoformat(crawler_execution_time)
    else:
        # fallback: 현재 시간 사용
        kst = pytz.timezone('Asia/Seoul')
        execution_time_kst = datetime.now(kst)
    
    trigger_data = {
        'job_id': job_id,
        'execution_time': execution_time_kst.isoformat(),
        'dag_run_id': context['dag_run'].run_id,
        'source_dag': 'realtime_pipeline_monitor',
        'trigger_point': 'call_crawler'
    }
    
    logging.info(f"[Redshift Trigger] Prepared data: {trigger_data}")
    print(f"🔄 Triggering Redshift DAG with job_id={job_id}, execution_time={execution_time_kst.isoformat()} (call_crawler execution time)", flush=True)
    
    return trigger_data

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
    dag_id="realtime_pipeline_monitor",
    default_args=default_args,
    description="실시간 파이프라인 모니터링 (Control 토픽 기반)",
    schedule=None,
    catchup=False,
    tags=["pipeline", "monitor", "control-topic", "realtime"]
) as dag:

    # 1. 크롤링 타입 결정 및 요청 데이터 준비
    prepare_crawler_request_task = PythonOperator(
        task_id="prepare_crawler_request",
        python_callable=call_crawler_dynamic,
    )

    # 2. Crawler 서버에 동적 HTTP 요청
    call_crawler = HttpOperator(
        task_id="call_crawler",
        http_conn_id="crawler_server",  # 실제 크롤러 서버 연결 ID
        endpoint="{{ ti.xcom_pull(task_ids='prepare_crawler_request')['endpoint'] }}",
        method="POST",
        data="{{ ti.xcom_pull(task_ids='prepare_crawler_request')['data'] | tojson }}",
        headers={"Content-Type": "application/json"},
        on_execute_callback=log_crawler_callback,
        log_response=True,
    )

    # 3. Collection 단계 완료 대기
    wait_collection = AwaitMessageSensor(
        task_id="wait_collection",
        kafka_config_id="job-control-topic",  # Control 토픽 연결 ID
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "collection"
        ],
        poll_timeout=1,
        poll_interval=30,  # 30초마다 체크
        execution_timeout=timedelta(minutes=3),
        xcom_push_key="collection_message",
        retries=0,
        on_failure_callback=handle_step_failure
    )

    # 4. Transform 단계 완료 대기 (병렬 처리)
    wait_transform = AwaitMessageSensor(
        task_id="wait_transform",
        kafka_config_id="job-control-topic",
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "transform"
        ],
        poll_timeout=1,
        poll_interval=30,
        execution_timeout=timedelta(minutes=3),
        xcom_push_key="transform_message",
        retries=0,
        on_failure_callback=handle_step_failure
    )

    # 5. Analysis 단계 완료 대기 (병렬 처리)
    wait_analysis = AwaitMessageSensor(
        task_id="wait_analysis",
        kafka_config_id="job-control-topic",
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "analysis"
        ],
        poll_timeout=1,
        poll_interval=30,
        execution_timeout=timedelta(minutes=3),
        xcom_push_key="analysis_message",
        retries=0,
        on_failure_callback=handle_step_failure
    )

    # 6. Aggregation 단계 완료 대기
    wait_aggregation = AwaitMessageSensor(
        task_id="wait_aggregation",
        kafka_config_id="job-control-topic",
        topics=["job-control-topic"],
        apply_function="include.kafka_filters.control_message_check",
        apply_function_args=[
            "{{ dag_run.conf.get('job_id') if dag_run and dag_run.conf else run_id }}",
            "aggregation"
        ],
        poll_timeout=1,
        poll_interval=30,
        execution_timeout=timedelta(minutes=3),
        xcom_push_key="aggregation_message",
        retries=0,
        on_failure_callback=handle_step_failure
    )

    # 7. 완료 알림 및 Redshift 트리거 데이터 준비 (통합)
    notify_and_prepare_redshift = PythonOperator(
        task_id="notify_and_prepare_redshift",
        python_callable=lambda **context: (
            print("🎉 All pipeline steps completed successfully!", flush=True),
            prepare_redshift_trigger_data(**context)
        )[1],  # prepare_redshift_trigger_data의 결과 반환
    )

    # 8. Redshift DAG 트리거
    trigger_redshift_dag = TriggerDagRunOperator(
        task_id="trigger_redshift_dag",
        trigger_dag_id="redshift_s3_copy_pipeline",
        conf=XComArg(notify_and_prepare_redshift),
        wait_for_completion=False,  # 비동기 실행
        poke_interval=30,
        dag=dag
    )

    # 작업 순서 정의 (병렬 처리 포함)
    prepare_crawler_request_task >> [call_crawler, wait_collection] >> [wait_transform, wait_analysis, wait_aggregation] >> notify_and_prepare_redshift >> trigger_redshift_dag

"""
DAG 실행 방법:

1. 단일 상품 크롤링 (기본):
   airflow dags trigger realtime_pipeline_monitor --conf '{
     "job_id": "job-2024-001",
     "product_id": "product-123",
     "url": "https://example.com/product/123",
     "review_cnt": 100
   }'

2. 다중 상품 크롤링:
   airflow dags trigger realtime_pipeline_monitor --conf '{
     "job_id": "job-2024-002",
     "url_list": [
       "https://example.com/product/123",
       "https://example.com/product/456",
       "https://example.com/product/789"
     ]
   }'

3. Airflow UI에서 수동 실행:
   - DAG 페이지에서 "Trigger DAG w/ Config" 클릭
   - Configuration JSON에 위 예시 중 하나 입력

4. 외부 API로 실행:
   curl -X POST "http://airflow-server:8080/api/v1/dags/realtime_pipeline_monitor/dagRuns" \
        -H "Content-Type: application/json" \
        -d '{
          "conf": {
            "job_id": "job-2024-001",
            "product_id": "product-123",
            "url": "https://example.com/product/123",
            "review_cnt": 100
          }
        }'

API 엔드포인트 자동 선택:
- url_list가 있으면 → /crawl/product_multi (다중 상품 크롤링)
- url이 있으면 → /crawl/product_one (단일 상품 크롤링)

주의사항:
- job_id는 Control 토픽의 메시지와 일치해야 함
- 각 단계별 타임아웃: Collection(60분), Transform(30분), Analysis(45분), Aggregation(15분)
- 실패 시 자동으로 에러 로깅 및 알림 처리
- 크롤러 서버 연결 ID는 'crawler_server'로 설정 필요
"""
