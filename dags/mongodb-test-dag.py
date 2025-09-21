from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

# DAG 설정
DAG_ID = 'mongodb_connection_test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# DAG 정의
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='MongoDB Connection Test Only - v2 (Airflow Variables)',
    schedule=None,  # 트리거 기반 실행
    max_active_runs=1,
    tags=['mongodb', 'test', 'connection', 'variables']
)

def test_mongodb_connection(**context) -> None:
    """MongoDB 연결 테스트 및 테스트 데이터 저장"""
    import os
    from airflow.models import Variable
    from pymongo import MongoClient
    
    # 테스트용 하드코딩 데이터 생성
    aggregation_data = {
        'job_id': 'test-job-001',
        'query_timestamp': '2025-09-21T19:30:00.000Z',
        'monthly_stats': [
            {
                'year': 2025,
                'month': 9,
                'total_reviews': 1500,
                'avg_rating': 4.2,
                'positive_count': 1200,
                'negative_count': 300,
                'category': '가전디지털'
            }
        ],
        'daily_stats': [
            {
                'year': 2025,
                'month': 9,
                'day': 21,
                'total_reviews': 150,
                'avg_rating': 4.3,
                'positive_count': 120,
                'negative_count': 30,
                'category': '가전디지털'
            }
        ]
    }
    
    print("[MongoDB] Using test hardcoded data for MongoDB connection test")
    print(f"[MongoDB] Test data: {aggregation_data}")
    
    # MongoDB 연결 정보 가져오기 (Airflow Variables 우선 사용)
    mongodb_url = None
    mongodb_database = None
    mongodb_username = None
    mongodb_password = None
    
    # Airflow Variables에서 MongoDB 연결 정보 가져오기
    try:
        mongodb_url = Variable.get("MONGODB_URL", default_var=None)
        print(f"[MongoDB Variables] MONGODB_URL: {mongodb_url}")
    except Exception as e:
        print(f"[MongoDB Variables] MONGODB_URL not found: {e}")
    
    try:
        mongodb_database = Variable.get("MONGODB_DATABASE", default_var=None)
        print(f"[MongoDB Variables] MONGODB_DATABASE: {mongodb_database}")
    except Exception as e:
        print(f"[MongoDB Variables] MONGODB_DATABASE not found: {e}")
    
    try:
        mongodb_username = Variable.get("MONGODB_USERNAME", default_var=None)
        print(f"[MongoDB Variables] MONGODB_USERNAME: {mongodb_username}")
    except Exception as e:
        print(f"[MongoDB Variables] MONGODB_USERNAME not found: {e}")
    
    try:
        mongodb_password = Variable.get("MONGODB_PASSWORD", default_var=None)
        print(f"[MongoDB Variables] MONGODB_PASSWORD: {'***' if mongodb_password else None}")
    except Exception as e:
        print(f"[MongoDB Variables] MONGODB_PASSWORD not found: {e}")
    
    # 환경 변수도 시도 (fallback)
    if not mongodb_url:
        mongodb_url = os.getenv('MONGODB_URL')
        print(f"[MongoDB Env] MONGODB_URL from env: {mongodb_url}")
    
    if not mongodb_database:
        mongodb_database = os.getenv('MONGODB_DATABASE')
        print(f"[MongoDB Env] MONGODB_DATABASE from env: {mongodb_database}")
    
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
        print("[MongoDB] Using MongoDB URL from Airflow Variables")
    elif mongodb_username and mongodb_password and mongodb_database:
        # 개별 값들로 URI 구성
        mongodb_uri = f"mongodb://{mongodb_username}:{mongodb_password}@mongodb-service.web-tier.svc.cluster.local:27017/{mongodb_database}?authSource=admin"
        print("[MongoDB] Using constructed MongoDB URI from Airflow Variables")
    else:
        # 테스트용 하드코딩 (fallback)
        mongodb_uri = "mongodb://mongodb-service.web-tier.svc.cluster.local:27017/reviewdb"
        mongodb_database = "reviewdb"
        print("[MongoDB] Using test MongoDB connection (hardcoded fallback)")
    
    # 컬렉션 설정
    try:
        mongodb_collection = Variable.get("MONGODB_COLLECTION", default_var="daily_monthly_agg_collection")
        print(f"[MongoDB Variables] MONGODB_COLLECTION: {mongodb_collection}")
    except Exception as e:
        mongodb_collection = "daily_monthly_agg_collection"
        print(f"[MongoDB Variables] MONGODB_COLLECTION not found, using default: {mongodb_collection}")
    
    print(f"[MongoDB] Connection info - Database: {mongodb_database}, Collection: {mongodb_collection}")
    
    try:
        # MongoDB 연결
        print(f"[MongoDB] Attempting to connect to: {mongodb_uri}")
        client = MongoClient(mongodb_uri)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        
        # 연결 테스트
        client.admin.command('ping')
        print("[MongoDB] Successfully connected to MongoDB!")
        
        # 테스트 데이터 저장
        result = collection.insert_one(aggregation_data)
        print(f"[MongoDB] Successfully saved test data with ID: {result.inserted_id}")
        
        # 저장된 데이터 확인
        saved_data = collection.find_one({"_id": result.inserted_id})
        print(f"[MongoDB] Verified saved data: {saved_data}")
        
        # 연결 종료
        client.close()
        print("[MongoDB] Connection closed successfully")
        
    except Exception as e:
        print(f"[MongoDB] Error connecting to MongoDB: {e}")
        print(f"[MongoDB] Connection URI: {mongodb_uri}")
        print(f"[MongoDB] Database: {mongodb_database}")
        print(f"[MongoDB] Collection: {mongodb_collection}")
        raise

# MongoDB 테스트 태스크
test_mongodb_task = PythonOperator(
    task_id='test_mongodb_connection',
    python_callable=test_mongodb_connection,
    dag=dag
)
