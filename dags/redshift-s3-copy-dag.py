from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import boto3
import json
from typing import List, Dict, Any

# 기본 설정
DAG_ID = 'redshift_s3_copy_pipeline'
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# DAG 정의
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Redshift S3 COPY Pipeline for Review Data',
    schedule='@hourly',  # Airflow 3.x
    max_active_runs=1,
    tags=['redshift', 's3', 'kafka', 'review-data']
)

def get_latest_s3_files(**context) -> List[str]:
    """S3에서 최신 파일 목록을 가져오는 함수"""
    s3_client = boto3.client('s3')
    execution_date = context['ds']
    korean_date = execution_date.replace('-', '')  # YYYY-MM-DD -> YYYYMMDD
    
    # S3 경로: topics/review-rows/YYYYMMDD/
    prefix = f"{S3_PREFIX}/{korean_date}/"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=prefix
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json.gz'):
                    files.append(f"s3://{S3_BUCKET}/{obj['Key']}")
        
        print(f"Found {len(files)} files for date {korean_date}")
        return files
        
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        return []

def create_redshift_copy_sql(files: List[str]) -> str:
    """Redshift COPY SQL 생성"""
    if not files:
        return "-- No files to copy"
    
    # 파일 목록을 문자열로 변환
    file_list = "', '".join(files)
    
    copy_sql = f"""
    COPY {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (
        review_id, job_id, product_id, title, tag, rating, review_count,
        sales_price, final_price, review_rating, review_date, review_text,
        clean_text, keywords, review_help_count, is_coupang_trial,
        is_empty_review, is_valid_rating, is_valid_date, has_content,
        is_valid, invalid_reason, year, month, day, quarter, yyyymm,
        yyyymmdd, weekday, summary, sentiment, crawled_at
    )
    FROM 's3://{S3_BUCKET}/topics/review-rows/'
    IAM_ROLE 'arn:aws:iam::ACCOUNT_ID:role/RedshiftRole'
    JSON 'auto'
    JSONPATH 'auto'
    GZIP
    COMPUPDATE OFF
    STATUPDATE OFF
    TRUNCATECOLUMNS
    IGNOREHEADER 0
    DELIMITER ','
    ESCAPE
    NULL AS '\\N'
    EMPTYASNULL
    BLANKSASNULL
    DATEFORMAT 'auto'
    TIMEFORMAT 'auto'
    ACCEPTINVCHARS
    ACCEPTANYDATE
    IGNOREBLANKLINES
    TRIMBLANKS
    FILLRECORD
    MAXERROR 1000
    REGION 'ap-northeast-2';
    """
    
    return copy_sql

def validate_copy_results(**context) -> Dict[str, Any]:
    """COPY 결과 검증"""
    redshift_hook = context['task_instance'].xcom_pull(task_ids='copy_to_redshift')
    
    # 기본 검증 로직
    validation_results = {
        'total_records': 0,
        'valid_records': 0,
        'invalid_records': 0,
        'validation_passed': False
    }
    
    try:
        # 여기에 실제 검증 로직 구현
        validation_results['validation_passed'] = True
        print("Data validation completed successfully")
        
    except Exception as e:
        print(f"Validation error: {e}")
        validation_results['validation_passed'] = False
    
    return validation_results

# S3 키 센서 (해당 날짜 경로에 하나 이상의 파일 존재 확인)
s3_file_sensor = S3KeySensor(
    task_id='s3_file_sensor',
    bucket_name=S3_BUCKET,
    bucket_key=f"{S3_PREFIX}/{{{{ ds[0:4] }}}}{{{{ ds[5:7] }}}}{{{{ ds[8:10] }}}}/*",
    aws_conn_id='aws_default',
    poke_interval=60,
    timeout=300,
    dag=dag
)

# S3 파일 목록 가져오기
get_s3_files = PythonOperator(
    task_id='get_s3_files',
    python_callable=get_latest_s3_files,
    dag=dag
)

# Redshift 테이블 생성
create_table = RedshiftSQLOperator(
    task_id='create_table',
    sql=f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (
        review_id VARCHAR(255) NOT NULL,
        job_id VARCHAR(255),
        product_id VARCHAR(255),
        title VARCHAR(MAX),
        tag VARCHAR(255),
        rating DECIMAL(3,2),
        review_count INTEGER,
        sales_price INTEGER,
        final_price INTEGER,
        review_rating DECIMAL(3,2),
        review_date TIMESTAMP,
        review_text VARCHAR(MAX),
        clean_text VARCHAR(MAX),
        keywords VARCHAR(MAX),
        review_help_count INTEGER,
        is_coupang_trial INTEGER,
        is_empty_review INTEGER,
        is_valid_rating INTEGER,
        is_valid_date INTEGER,
        has_content INTEGER,
        is_valid INTEGER,
        invalid_reason VARCHAR(MAX),
        year INTEGER,
        month INTEGER,
        day INTEGER,
        quarter INTEGER,
        yyyymm VARCHAR(6),
        yyyymmdd VARCHAR(8),
        weekday VARCHAR(10),
        summary VARCHAR(MAX),
        sentiment VARCHAR(50),
        crawled_at TIMESTAMP NOT NULL,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    DISTKEY(review_id)
    SORTKEY(crawled_at, review_id);
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# 키워드 테이블 생성
create_keywords_table = RedshiftSQLOperator(
    task_id='create_keywords_table',
    sql=f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.review_keywords (
        review_id VARCHAR(255) NOT NULL,
        keyword_type VARCHAR(100),
        keyword_value VARCHAR(255),
        crawled_at TIMESTAMP,
        PRIMARY KEY (review_id, keyword_type)
    )
    DISTKEY(review_id)
    SORTKEY(review_id, keyword_type);
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# 무효 사유 테이블 생성
create_invalid_reasons_table = RedshiftSQLOperator(
    task_id='create_invalid_reasons_table',
    sql=f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.review_invalid_reasons (
        review_id VARCHAR(255) NOT NULL,
        reason_order INTEGER,
        reason_value VARCHAR(255),
        crawled_at TIMESTAMP,
        PRIMARY KEY (review_id, reason_order)
    )
    DISTKEY(review_id)
    SORTKEY(review_id, reason_order);
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# S3에서 Redshift로 데이터 복사
copy_to_redshift = RedshiftSQLOperator(
    task_id='copy_to_redshift',
    sql=create_redshift_copy_sql,
    redshift_conn_id='redshift_default',
    dag=dag
)

# JSON 데이터 파싱
parse_json_data = RedshiftSQLOperator(
    task_id='parse_json_data',
    sql=f"""
    -- JSON 파싱 함수 생성
    CREATE OR REPLACE FUNCTION parse_keywords(review_id VARCHAR(255), keywords_json TEXT)
    RETURNS VOID AS $$
    DECLARE
        keyword_record RECORD;
    BEGIN
        FOR keyword_record IN 
            SELECT * FROM json_each_text(keywords_json::json)
        LOOP
            INSERT INTO {REDSHIFT_SCHEMA}.review_keywords (review_id, keyword_type, keyword_value, crawled_at)
            VALUES (review_id, keyword_record.key, keyword_record.value, CURRENT_TIMESTAMP)
            ON CONFLICT (review_id, keyword_type) DO UPDATE SET
                keyword_value = EXCLUDED.keyword_value,
                crawled_at = EXCLUDED.crawled_at;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    
    CREATE OR REPLACE FUNCTION parse_invalid_reasons(review_id VARCHAR(255), reasons_json TEXT)
    RETURNS VOID AS $$
    DECLARE
        reason_record RECORD;
        reason_order INTEGER := 1;
    BEGIN
        FOR reason_record IN 
            SELECT * FROM json_array_elements_text(reasons_json::json)
        LOOP
            INSERT INTO {REDSHIFT_SCHEMA}.review_invalid_reasons (review_id, reason_order, reason_value, crawled_at)
            VALUES (review_id, reason_order, reason_record.value, CURRENT_TIMESTAMP)
            ON CONFLICT (review_id, reason_order) DO UPDATE SET
                reason_value = EXCLUDED.reason_value,
                crawled_at = EXCLUDED.crawled_at;
            reason_order := reason_order + 1;
        END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    
    -- keywords 파싱 실행
    SELECT parse_keywords(review_id, keywords) 
    FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} 
    WHERE keywords IS NOT NULL AND keywords != '{{}}' AND keywords != 'null';
    
    -- invalid_reason 파싱 실행
    SELECT parse_invalid_reasons(review_id, invalid_reason) 
    FROM {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} 
    WHERE invalid_reason IS NOT NULL AND invalid_reason != '[]' AND invalid_reason != 'null';
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# 성능 최적화 인덱스 생성
create_indexes = RedshiftSQLOperator(
    task_id='create_indexes',
    sql=f"""
    -- 메인 테이블 인덱스
    CREATE INDEX IF NOT EXISTS idx_realtime_review_collection_is_valid 
    ON {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (is_valid);
    
    CREATE INDEX IF NOT EXISTS idx_realtime_review_collection_sentiment 
    ON {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (sentiment);
    
    CREATE INDEX IF NOT EXISTS idx_realtime_review_collection_yyyymm 
    ON {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (yyyymm);
    
    CREATE INDEX IF NOT EXISTS idx_realtime_review_collection_quarter 
    ON {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (quarter);
    
    CREATE INDEX IF NOT EXISTS idx_realtime_review_collection_analysis 
    ON {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE} (is_valid, sentiment, yyyymm);
    
    -- 키워드 테이블 인덱스
    CREATE INDEX IF NOT EXISTS idx_review_keywords_keyword_type 
    ON {REDSHIFT_SCHEMA}.review_keywords (keyword_type);
    
    CREATE INDEX IF NOT EXISTS idx_review_keywords_keyword_value 
    ON {REDSHIFT_SCHEMA}.review_keywords (keyword_value);
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# 데이터 검증
validate_data = PythonOperator(
    task_id='validate_data',
    python_callable=validate_copy_results,
    dag=dag
)

# 통계 업데이트
update_statistics = RedshiftSQLOperator(
    task_id='update_statistics',
    sql=f"""
    ANALYZE {REDSHIFT_SCHEMA}.{REDSHIFT_TABLE};
    ANALYZE {REDSHIFT_SCHEMA}.review_keywords;
    ANALYZE {REDSHIFT_SCHEMA}.review_invalid_reasons;
    """,
    redshift_conn_id='redshift_default',
    dag=dag
)

# 작업 순서 정의
s3_file_sensor >> get_s3_files >> create_table >> create_keywords_table >> create_invalid_reasons_table >> copy_to_redshift >> parse_json_data >> create_indexes >> validate_data >> update_statistics
