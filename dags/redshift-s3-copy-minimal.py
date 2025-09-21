from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
import json
import gzip
from zoneinfo import ZoneInfo
from typing import List, Dict, Any
import pandas as pd
from pymongo import MongoClient

# 기본 설정
DAG_ID = 'redshift_s3_copy_minimal'
S3_BUCKET = 'hihypipe-raw-data'
S3_PREFIX = 'topics/review-rows'
REDSHIFT_SCHEMA = 'public'
REDSHIFT_TABLE = 'realtime_review_collection'

# 기본 DAG 인수
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# DAG 정의 (트리거 기반)
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Minimal Redshift S3 COPY Pipeline (Data Copy Only)',
    schedule=None,  # 트리거 기반 실행
    max_active_runs=1,
    tags=['redshift', 's3', 'copy', 'minimal']
)

def extract_trigger_data(**context) -> Dict[str, Any]:
    """트리거 DAG에서 전달받은 데이터 추출"""
    dag_run = context['dag_run']
    conf = dag_run.conf if dag_run.conf else {}
    
    job_id = conf.get('job_id', 'unknown')
    execution_time_str = conf.get('execution_time', context['dag_run'].logical_date.isoformat())
    source_dag = conf.get('source_dag', 'unknown')
    
    try:
        # ISO 형식 시간을 datetime으로 변환
        execution_time = datetime.fromisoformat(execution_time_str.replace('Z', '+00:00'))
    except:
        execution_time = context['dag_run'].logical_date
    
    # XCom 호환성을 위해 datetime을 문자열로 저장
    trigger_data = {
        'job_id': job_id,
        'execution_time': execution_time.isoformat(),  # 문자열로 저장
        'execution_time_str': execution_time_str,
        'source_dag': source_dag
    }
    
    print(f"[Trigger Data] Extracted: {trigger_data}")
    return trigger_data

def extract_job_id_from_s3_file(s3_key: str) -> str:
    """S3 파일에서 job_id 추출"""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        
        with gzip.GzipFile(fileobj=response['Body']) as gz_file:
            first_line = gz_file.readline()
            data = json.loads(first_line)
            return data.get('job_id', 'unknown')
    except Exception as e:
        print(f"Error extracting job_id from {s3_key}: {e}")
        return 'unknown'

def get_s3_files_all(**context) -> List[str]:
    """테스트용: 해당 폴더의 모든 S3 파일 목록을 가져오는 함수 (시간 필터링 제거)"""
    s3_client = boto3.client('s3')
    
    # 트리거 데이터 가져오기 (날짜만 사용)
    trigger_data = context['task_instance'].xcom_pull(task_ids='extract_trigger_data')
    execution_time_str = trigger_data['execution_time']  # 문자열로 받음
    
    # 문자열을 datetime으로 변환
    execution_time = datetime.fromisoformat(execution_time_str.replace('Z', '+00:00'))
    
    print(f"[S3 Filter] TEST MODE: Getting ALL files (no time filtering)")
    
    # 날짜 추출 (YYYYMMDD 형식, KST 기준) 및 접두사 구성
    kst = ZoneInfo('Asia/Seoul')
    execution_date = execution_time.astimezone(kst).strftime('%Y%m%d')
    prefix = f"{S3_PREFIX}/{execution_date}/"
    
    files = []
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.json.gz'):
                files.append({
                    's3_path': f"s3://{S3_BUCKET}/{obj['Key']}",
                    'last_modified': obj['LastModified'],
                    'size': obj['Size']
                })
                print(f"[S3 Filter] Added file: {obj['Key']} (size: {obj['Size']}, modified: {obj['LastModified']})")
    
    # 파일 크기순 정렬
    files.sort(key=lambda x: x['size'], reverse=True)
    
    print(f"[S3 Filter] Found {len(files)} files in total (TEST MODE)")
    
    # 디버깅을 위한 상세 정보 출력
    for i, file in enumerate(files[:5]):  # 처음 5개 파일 출력
        print(f"[S3 Filter] File {i+1}: {file['s3_path']} (size: {file['size']}, modified: {file['last_modified']})")
    
    return [file['s3_path'] for file in files]

def query_redshift_aggregations(**context) -> Dict[str, Any]:
    """Redshift에서 job_id 기준으로 월별/일별 집계 데이터 조회"""
    from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
    
    # 트리거 데이터에서 job_id 가져오기
    trigger_data = context['task_instance'].xcom_pull(task_ids='extract_trigger_data')
    job_id = trigger_data['job_id']
    
    print(f"[Redshift Query] Starting aggregation queries for job_id: {job_id}")
    
    # Redshift 연결
    redshift_hook = RedshiftDataHook(aws_conn_id='aws_default')
    
    # 월별 집계 쿼리 (JSON 구조에 맞춤)
    monthly_query = f"""
    SELECT 
        yyyymm,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        AVG(rating) as avg_product_rating,
        COUNT(CASE WHEN sentiment = '긍정' THEN 1 END) as positive_reviews,
        COUNT(CASE WHEN sentiment = '부정' THEN 1 END) as negative_reviews,
        COUNT(CASE WHEN sentiment = '중립' THEN 1 END) as neutral_reviews,
        COUNT(CASE WHEN sentiment IS NULL OR sentiment = '' THEN 1 END) as unknown_sentiment
    FROM public.realtime_review_collection 
    WHERE job_id = '{job_id}'
    GROUP BY yyyymm
    ORDER BY yyyymm;
    """
    
    # 일별 집계 쿼리 (JSON 구조에 맞춤)
    daily_query = f"""
    SELECT 
        yyyymmdd,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        AVG(rating) as avg_product_rating,
        COUNT(CASE WHEN sentiment = '긍정' THEN 1 END) as positive_reviews,
        COUNT(CASE WHEN sentiment = '부정' THEN 1 END) as negative_reviews,
        COUNT(CASE WHEN sentiment = '중립' THEN 1 END) as neutral_reviews,
        COUNT(CASE WHEN sentiment IS NULL OR sentiment = '' THEN 1 END) as unknown_sentiment
    FROM public.realtime_review_collection 
    WHERE job_id = '{job_id}'
    GROUP BY yyyymmdd
    ORDER BY yyyymmdd;
    """
    
    # 전체 집계 쿼리 (한국어 감정분석 결과에 맞춤)
    overall_query = f"""
    SELECT 
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        AVG(rating) as avg_product_rating,
        COUNT(CASE WHEN sentiment = '긍정' THEN 1 END) as positive_reviews,
        COUNT(CASE WHEN sentiment = '부정' THEN 1 END) as negative_reviews,
        COUNT(CASE WHEN sentiment = '중립' THEN 1 END) as neutral_reviews,
        COUNT(CASE WHEN sentiment IS NULL OR sentiment = '' THEN 1 END) as unknown_sentiment,
        MIN(yyyymmdd) as earliest_date,
        MAX(yyyymmdd) as latest_date
    FROM public.realtime_review_collection 
    WHERE job_id = '{job_id}';
    """
    
    try:
        # 쿼리 실행
        monthly_result = redshift_hook.execute_query(
            workgroup_name='hihypipe-redshift-workgroup',
            database='hihypipe',
            sql=monthly_query
        )
        
        daily_result = redshift_hook.execute_query(
            workgroup_name='hihypipe-redshift-workgroup',
            database='hihypipe',
            sql=daily_query
        )
        
        overall_result = redshift_hook.execute_query(
            workgroup_name='hihypipe-redshift-workgroup',
            database='hihypipe',
            sql=overall_query
        )
        
        # 결과 정리
        aggregation_data = {
            'job_id': job_id,
            'query_timestamp': datetime.now().isoformat(),
            'monthly_stats': monthly_result,
            'daily_stats': daily_result,
            'overall_stats': overall_result[0] if overall_result else {}
        }
        
        print(f"[Redshift Query] Completed aggregation queries. Monthly: {len(monthly_result)} records, Daily: {len(daily_result)} records")
        
        # XCom에 저장
        context['task_instance'].xcom_push(key='aggregation_data', value=aggregation_data)
        
        return aggregation_data
        
    except Exception as e:
        print(f"[Redshift Query] Error executing queries: {e}")
        raise

def save_to_mongodb(**context) -> None:
    """집계 데이터를 MongoDB에 저장"""
    import os
    from airflow.models import Variable
    
    # 집계 데이터 가져오기
    aggregation_data = context['task_instance'].xcom_pull(
        task_ids='query_redshift_aggregations',
        key='aggregation_data'
    )
    
    if not aggregation_data:
        print("[MongoDB] No aggregation data found, skipping save")
        return
    
    # Kubernetes Secret에서 MongoDB 연결 정보 가져오기
    mongodb_url = os.getenv('MONGODB_URL')
    mongodb_db_name = os.getenv('MONGODB_DB_NAME')
    mongodb_username = os.getenv('mongodb-username')
    mongodb_password = os.getenv('mongodb-password')
    mongodb_database = os.getenv('mongodb-database')
    
    
    # MongoDB 연결 정보 검증
    if not mongodb_url and not (mongodb_username and mongodb_password and mongodb_database):
        print("[MongoDB] MongoDB connection information not found in Kubernetes Secret or Airflow Variables")
        print("[MongoDB] Please configure MongoDB connection in Kubernetes Secret or Airflow Variables")
        return
    
    # MongoDB URI 구성
    if mongodb_url:
        mongodb_uri = mongodb_url
        print("[MongoDB] Using MongoDB URL from Kubernetes Secret/Airflow Variable")
    else:
        # 개별 값들로 URI 구성
        mongodb_uri = f"mongodb://{mongodb_username}:{mongodb_password}@mongodb-service.web-tier.svc.cluster.local:27017/{mongodb_database}?authSource=admin"
        print("[MongoDB] Using constructed MongoDB URI from individual connection parameters")
    
    # 데이터베이스와 컬렉션 설정
    mongodb_database = mongodb_db_name or mongodb_database
    mongodb_collection = Variable.get("MONGODB_COLLECTION", default_var="daily_monthly_agg_collection")
    
    print(f"[MongoDB] Connection info - Database: {mongodb_database}, Collection: {mongodb_collection}")
    
    try:
        # MongoDB 연결
        client = MongoClient(mongodb_uri)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        # 데이터 저장 (upsert 방식으로 job_id 기준)
        result = collection.replace_one(
            {'job_id': aggregation_data['job_id']},
            aggregation_data,
            upsert=True
        )
        
        print(f"[MongoDB] Data saved successfully. Job ID: {aggregation_data['job_id']}, Operation: {result.upserted_id or 'updated'}")
        
        # 연결 종료
        client.close()
        
    except Exception as e:
        print(f"[MongoDB] Error saving data: {e}")
        raise


# 1. 트리거 데이터 추출
extract_trigger_data_task = PythonOperator(
    task_id='extract_trigger_data',
    python_callable=extract_trigger_data,
    dag=dag
)

# 2. S3 파일 목록 가져오기 (테스트용: 모든 파일)
get_s3_files = PythonOperator(
    task_id='get_s3_files_all',
    python_callable=get_s3_files_all,
    dag=dag
)

# 3. S3에서 Redshift로 데이터 복사 (job_id 기준)
copy_to_redshift = RedshiftDataOperator(
    task_id='copy_to_redshift',
    workgroup_name='hihypipe-redshift-workgroup',
    database='hihypipe',
    sql="""
    -- 디버깅용: 현재 데이터베이스와 테이블 확인
    SELECT current_database(), current_schema();
    
    -- 테이블 존재 여부 확인
    SELECT COUNT(*) as table_exists 
    FROM information_schema.tables 
    WHERE table_schema = '{{ params.schema }}' 
    AND table_name = '{{ params.table }}';
    
    -- 실제 COPY 명령 (JSON 구조에 정확히 맞춤)
    COPY {{ params.schema }}.{{ params.table }} (
        review_id, sentiment, keywords, year, rating, weekday, review_count,
        invalid_reason, title, crawled_at, final_price, has_content, product_id,
        review_help_count, tag, clean_text, day, summary, is_coupang_trial,
        is_valid_rating, is_valid_date, review_date, month, job_id, yyyymmdd,
        is_valid, sales_price, is_empty_review, review_text, yyyymm, quarter
    )
    FROM '{{ ti.xcom_pull(task_ids="get_s3_files_all") | first }}'
    IAM_ROLE '{{ params.iam_role }}'
    JSON 'auto'
    GZIP
    COMPUPDATE OFF
    STATUPDATE OFF
    EMPTYASNULL
    BLANKSASNULL
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    ACCEPTINVCHARS
    ACCEPTANYDATE
    MAXERROR 1000
    REGION 'ap-northeast-2';
    """,
    params={
        'schema': 'public',
        'table': 'realtime_review_collection',
        'iam_role': "arn:aws:iam::914215749228:role/hihypipe-redshift-s3-copy-role"
    },
    aws_conn_id='aws_default',
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
)

# 4. Redshift 집계 쿼리 실행
query_aggregations = PythonOperator(
    task_id='query_redshift_aggregations',
    python_callable=query_redshift_aggregations,
    dag=dag
)

# 5. MongoDB에 집계 데이터 저장
save_aggregations_to_mongodb = PythonOperator(
    task_id='save_aggregations_to_mongodb',
    python_callable=save_to_mongodb,
    dag=dag
)

# 작업 순서 정의 (COPY 완료 후 집계 및 저장)
extract_trigger_data_task >> get_s3_files >> copy_to_redshift >> query_aggregations >> save_aggregations_to_mongodb
