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
import urllib3

# SSL 경고 비활성화
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    description='Minimal Redshift S3 COPY Pipeline (Data Copy Only) - v27',
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
    
    print(f"[Trigger Data] Extracted from conf: {trigger_data}")
    print(f"[Trigger Data] Using execution_time from conf: {execution_time_str}")
    return trigger_data

def extract_job_id_from_s3_file(s3_key: str) -> str:
    """S3 파일에서 job_id 추출"""
    # SSL 검증 비활성화로 boto3 클라이언트 생성
    s3_client = boto3.client(
        's3',
        verify=False,  # SSL 검증 비활성화
        region_name='ap-northeast-2'
    )
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        
        # .json.gz 파일이므로 gzip 압축 해제 필요
        with gzip.GzipFile(fileobj=response['Body']) as gz_file:
            first_line = gz_file.readline()
            data = json.loads(first_line)
            return data.get('job_id', 'unknown')
    except Exception as e:
        print(f"Error extracting job_id from {s3_key}: {e}")
        return 'unknown'

def get_s3_files_all(**context) -> List[str]:
    """테스트용: 해당 폴더의 모든 S3 파일 목록을 가져오는 함수 (시간 필터링 제거)"""
    # SSL 검증 비활성화로 boto3 클라이언트 생성
    s3_client = boto3.client(
        's3',
        verify=False,  # SSL 검증 비활성화
        region_name='ap-northeast-2'
    )
    
    # 트리거 데이터 가져오기 (날짜만 사용)
    trigger_data = context['task_instance'].xcom_pull(task_ids='extract_trigger_data')
    execution_time_str = trigger_data['execution_time']  # 문자열로 받음
    
    # 문자열을 datetime으로 변환
    execution_time = datetime.fromisoformat(execution_time_str.replace('Z', '+00:00'))
    
    print(f"[S3 Filter] TEST MODE: Getting ALL files (no time filtering)")
    
    # 날짜 추출 (YYYYMMDD 형식, UTC 기준으로 날짜만 추출) 및 접두사 구성
    execution_date = execution_time.strftime('%Y%m%d')  # UTC 기준으로 날짜만 추출
    prefix = f"{S3_PREFIX}/{execution_date}/"
    
    print(f"[S3 Filter] Searching S3 bucket: {S3_BUCKET}")
    print(f"[S3 Filter] Searching prefix: {prefix}")
    print(f"[S3 Filter] Source: conf.execution_time -> {execution_time_str}")
    
    files = []
    
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        print(f"[S3 Filter] S3 API Response Status: {response.get('ResponseMetadata', {}).get('HTTPStatusCode', 'Unknown')}")
        
        if 'Contents' in response:
            print(f"[S3 Filter] Found {len(response['Contents'])} objects in S3 response")
            for obj in response['Contents']:
                print(f"[S3 Filter] Object: {obj['Key']} (size: {obj['Size']}, modified: {obj['LastModified']})")
                if obj['Key'].endswith('.json.gz'):
                    files.append({
                        's3_path': f"s3://{S3_BUCKET}/{obj['Key']}",
                        'last_modified': obj['LastModified'],
                        'size': obj['Size']
                    })
                    print(f"[S3 Filter] Added file: {obj['Key']} (size: {obj['Size']}, modified: {obj['LastModified']})")
        else:
            print(f"[S3 Filter] No Contents found in S3 response")
            print(f"[S3 Filter] S3 Response: {response}")
            
            # 더 자세한 디버깅을 위해 다른 패턴들도 확인
            print(f"[S3 Filter] Checking alternative patterns...")
            
            # 1. 날짜 형식 확인 (YYYY-MM-DD vs YYYYMMDD)
            alt_date_formats = [
                execution_time.strftime('%Y-%m-%d'),  # 2025-09-21 (UTC 기준)
                execution_time.strftime('%Y%m%d'),     # 20250921 (UTC 기준)
            ]
            
            for alt_date in alt_date_formats:
                alt_prefix = f"{S3_PREFIX}/{alt_date}/"
                print(f"[S3 Filter] Trying alternative prefix: {alt_prefix}")
                alt_response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=alt_prefix)
                if 'Contents' in alt_response:
                    print(f"[S3 Filter] Found {len(alt_response['Contents'])} files with alternative date format: {alt_date}")
                    for obj in alt_response['Contents'][:3]:
                        print(f"[S3 Filter] Alt file: {obj['Key']}")
            
            # 2. 상위 디렉토리 확인
            print(f"[S3 Filter] Checking parent directory...")
            parent_prefix = f"{S3_PREFIX}/"
            parent_response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=parent_prefix, MaxKeys=20)
            if 'Contents' in parent_response:
                print(f"[S3 Filter] Found {len(parent_response['Contents'])} files in parent directory")
                for obj in parent_response['Contents'][:5]:
                    print(f"[S3 Filter] Parent file: {obj['Key']}")
            
            # 3. 다른 날짜들도 확인해보기 (디버깅용)
            print(f"[S3 Filter] Checking other dates for debugging...")
            for days_back in range(1, 8):  # 최근 7일 확인
                check_date = (execution_time - timedelta(days=days_back)).strftime('%Y%m%d')  # UTC 기준
                check_prefix = f"{S3_PREFIX}/{check_date}/"
                check_response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=check_prefix)
                if 'Contents' in check_response:
                    print(f"[S3 Filter] Found {len(check_response['Contents'])} files on {check_date}")
                    # 첫 번째 파일의 경로를 반환하여 테스트 가능하게 함
                    first_file = check_response['Contents'][0]['Key']
                    print(f"[S3 Filter] First file: {first_file}")
                    return [f"s3://{S3_BUCKET}/{first_file}"]
            
            # 4. 루트 디렉토리 확인
            print(f"[S3 Filter] No files found in recent 7 days. Checking root directory...")
            root_response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX, MaxKeys=10)
            if 'Contents' in root_response:
                print(f"[S3 Filter] Found {len(root_response['Contents'])} files in root directory")
                for obj in root_response['Contents'][:3]:  # 처음 3개 파일 출력
                    print(f"[S3 Filter] Root file: {obj['Key']}")
            else:
                print(f"[S3 Filter] No files found in root directory either")
                
    except Exception as e:
        print(f"[S3 Filter] ERROR: Failed to list S3 objects: {e}")
        print(f"[S3 Filter] Error type: {type(e).__name__}")
        return [""]
    
    # 파일 크기순 정렬
    files.sort(key=lambda x: x['size'], reverse=True)
    
    print(f"[S3 Filter] Found {len(files)} files in total (TEST MODE)")
    
    # 디버깅을 위한 상세 정보 출력
    for i, file in enumerate(files[:5]):  # 처음 5개 파일 출력
        print(f"[S3 Filter] File {i+1}: {file['s3_path']} (size: {file['size']}, modified: {file['last_modified']})")
    
    s3_paths = [file['s3_path'] for file in files]
    
    if not s3_paths:
        print(f"[S3 Filter] WARNING: No files found for date {execution_date}")
        print(f"[S3 Filter] Searched prefix: {prefix}")
        # 테스트용으로 빈 문자열 반환 (COPY 명령이 실행되지 않도록)
        return [""]
    
    return s3_paths

def query_redshift_aggregations(**context) -> Dict[str, Any]:
    """Redshift에서 job_id 기준으로 월별/일별 집계 데이터 조회"""
    from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
    
    # 트리거 데이터에서 job_id 가져오기
    trigger_data = context['task_instance'].xcom_pull(task_ids='extract_trigger_data')
    job_id = trigger_data['job_id']
    
    print(f"[Redshift Query] Starting aggregation queries for job_id: {job_id}")
    
    # Redshift 연결
    redshift_hook = RedshiftDataHook()
    
    # 월별 집계 쿼리 (JSON 구조에 맞춤)
    monthly_query = f"""
    SELECT 
        yyyymm,
        COUNT(*) as total_reviews,
        AVG(rating) as avg_rating,
        AVG(rating) as avg_product_rating,
        COUNT(CASE WHEN sentiment = '긍정' THEN 1 END) as positive_reviews,
        COUNT(CASE WHEN sentiment = '부정' THEN 1 END) as negative_reviews,
        COUNT(CASE WHEN sentiment = '중립' THEN 1 END) as neutral_reviews
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
        COUNT(CASE WHEN sentiment = '중립' THEN 1 END) as neutral_reviews
    FROM public.realtime_review_collection 
    WHERE job_id = '{job_id}'
    GROUP BY yyyymmdd
    ORDER BY yyyymmdd;
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
        
        # 결과 정리 (QueryExecutionOutput에서 실제 데이터 추출)
        monthly_data = monthly_result.records if hasattr(monthly_result, 'records') else []
        daily_data = daily_result.records if hasattr(daily_result, 'records') else []
        
        aggregation_data = {
            'job_id': job_id,
            'query_timestamp': datetime.now().isoformat(),
            'monthly_stats': monthly_data,
            'daily_stats': daily_data
        }
        
        print(f"[Redshift Query] Completed aggregation queries. Monthly: {len(monthly_data)} records, Daily: {len(daily_data)} records")
        
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
    
    # MongoDB 연결 정보 가져오기 (환경 변수 또는 Airflow Variables)
    mongodb_url = os.getenv('MONGODB_URL')
    mongodb_db_name = os.getenv('MONGODB_DB_NAME')
    mongodb_username = os.getenv('mongodb-username')
    mongodb_password = os.getenv('mongodb-password')
    mongodb_database = os.getenv('mongodb-database')
    
    # Airflow Variables에서도 시도
    if not mongodb_url:
        try:
            mongodb_url = Variable.get("MONGODB_URL", default_var=None)
        except:
            pass
    
    if not mongodb_database:
        try:
            mongodb_database = Variable.get("MONGODB_DATABASE", default_var=None)
        except:
            pass
    
    # 디버깅: 환경 변수 상태 출력
    print(f"[MongoDB DEBUG] mongodb_url: {mongodb_url}")
    print(f"[MongoDB DEBUG] mongodb_database: {mongodb_database}")
    print(f"[MongoDB DEBUG] mongodb_username: {mongodb_username}")
    print(f"[MongoDB DEBUG] mongodb_password: {'***' if mongodb_password else None}")
    
    # 테스트용 하드코딩 (실제 환경에서는 환경 변수 사용)
    if not mongodb_url and not (mongodb_username and mongodb_password and mongodb_database):
        print("[MongoDB] Using test MongoDB connection (hardcoded)")
        mongodb_url = "mongodb://mongodb-service.web-tier.svc.cluster.local:27017/reviewdb"
        mongodb_database = "reviewdb"
    else:
        print("[MongoDB] Using MongoDB connection from environment variables")
    
    # MongoDB URI 구성
    if mongodb_url:
        mongodb_uri = mongodb_url
        print("[MongoDB] Using MongoDB URL from environment variables")
    else:
        # 개별 값들로 URI 구성
        mongodb_uri = f"mongodb://{mongodb_username}:{mongodb_password}@mongodb-service.web-tier.svc.cluster.local:27017/{mongodb_database}?authSource=admin"
        print("[MongoDB] Using constructed MongoDB URI from individual connection parameters")
    
    # 데이터베이스와 컬렉션 설정
    mongodb_database = mongodb_db_name or mongodb_database
    try:
        mongodb_collection = Variable.get("MONGODB_COLLECTION", default_var="daily_monthly_agg_collection")
    except:
        mongodb_collection = "daily_monthly_agg_collection"
    
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
    {% set s3_files = ti.xcom_pull(task_ids="get_s3_files_all") %}
    {% if s3_files and s3_files | select('ne', '') | list %}
    -- 권한 부여 (필요한 경우)
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {{ params.schema }}.{{ params.table }} TO "IAMR:hihypipe-airflow-irsa";
    
    -- 중복 방지: 해당 날짜 데이터 삭제
    DELETE FROM {{ params.schema }}.{{ params.table }} 
    WHERE yyyymmdd = '20250921';
    
    -- 실제 COPY 명령 (실제 스키마에 맞춤)
    COPY {{ params.schema }}.{{ params.table }} (
        review_id, sentiment, keywords, year, rating, weekday, review_count,
        title, crawled_at, final_price, has_content, product_id,
        review_help_count, clean_text, day, summary, is_coupang_trial,
        review_date, month, job_id, yyyymmdd, sales_price, is_empty_review, 
        review_text, yyyymm, quarter, category
    )
    FROM 's3://hihypipe-raw-data/topics/review-rows/20250921/'
    IAM_ROLE 'arn:aws:iam::914215749228:role/hihypipe-redshift-s3-copy-role'
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
    {% else %}
    -- S3 파일이 없는 경우 메시지 출력
    SELECT 'No S3 files found for processing' as message;
    {% endif %}
    """,
    params={
        'schema': 'public',
        'table': 'realtime_review_collection',
        'iam_role': "arn:aws:iam::914215749228:role/hihypipe-redshift-s3-copy-role"
    },
    retries=3,
    retry_delay=timedelta(minutes=1),
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
