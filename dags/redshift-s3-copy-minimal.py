from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python import PythonOperator
import boto3
import json
import gzip
from zoneinfo import ZoneInfo
from typing import List, Dict, Any

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
    
    -- 실제 COPY 명령 (일별/월별 집계용 최소 컬럼)
    COPY {{ params.schema }}.{{ params.table }} (
        review_date, product_id, job_id, product_title, product_category, 
        product_rating, review_count, sales_price, final_price, review_rating,
        review_text, clean_text, review_summary, sentiment_score,
        year, month, day, quarter, yyyymm, yyyymmdd, weekday, crawled_at
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
        'table': 'daily_review_summary',
        'iam_role': "arn:aws:iam::914215749228:role/hihypipe-redshift-s3-copy-role"
    },
    aws_conn_id='aws_default',
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
)

# 작업 순서 정의 (최소화된 파이프라인)
extract_trigger_data_task >> get_s3_files >> copy_to_redshift
