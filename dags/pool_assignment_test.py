"""
Pool Assignment Test DAG
풀 존재 확인, 할당, 할당 상태 확인을 위한 간단한 테스트 DAG
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Pool
from airflow import settings
from sqlalchemy.orm import sessionmaker

# 기본 인수 설정
default_args = {
    'owner': 'hihypipe',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG 정의
dag = DAG(
    'pool_assignment_test',
    default_args=default_args,
    description='풀 할당 테스트 DAG',
    schedule='@once',  # 한 번만 실행
    catchup=False,
    tags=['hihypipe', 'pool-test', 'assignment'],
)

def check_pool_exists(**context):
    """crawling_servers 풀이 존재하는지 확인"""
    print("🔍 풀 존재 여부 확인 시작...")
    
    try:
        # 데이터베이스 세션 생성
        Session = sessionmaker(bind=settings.engine)
        session = Session()
        
        # 풀 조회
        pool = session.query(Pool).filter(Pool.pool == 'crawling_servers').first()
        
        if pool:
            print(f"✅ 풀 발견: {pool.pool}")
            print(f"   - 슬롯 수: {pool.slots}")
            print(f"   - 설명: {pool.description}")
            print(f"   - 사용 중: {pool.used_slots}")
            print(f"   - 대기 중: {pool.queued_slots}")
            print(f"   - 사용 가능: {pool.open_slots}")
            
            session.close()
            return {
                "status": "exists",
                "pool_name": pool.pool,
                "slots": pool.slots,
                "used_slots": pool.used_slots,
                "queued_slots": pool.queued_slots,
                "open_slots": pool.open_slots
            }
        else:
            print("❌ crawling_servers 풀이 존재하지 않습니다.")
            session.close()
            return {
                "status": "not_exists",
                "message": "풀을 먼저 생성해야 합니다."
            }
            
    except Exception as e:
        print(f"❌ 풀 확인 중 오류 발생: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

def create_pool_if_not_exists(**context):
    """풀이 없으면 생성"""
    print("🔧 풀 생성/확인 작업 시작...")
    
    try:
        # 데이터베이스 세션 생성
        Session = sessionmaker(bind=settings.engine)
        session = Session()
        
        # 기존 풀 확인
        existing_pool = session.query(Pool).filter(Pool.pool == 'crawling_servers').first()
        
        if existing_pool:
            print("✅ 풀이 이미 존재합니다.")
            session.close()
            return {
                "status": "already_exists",
                "message": "풀이 이미 생성되어 있습니다."
            }
        else:
            # 새 풀 생성
            new_pool = Pool(
                pool='crawling_servers',
                slots=3,
                description='Local crawling servers concurrency control'
            )
            
            session.add(new_pool)
            session.commit()
            session.close()
            
            print("✅ 새 풀 생성 완료!")
            print("   - 이름: crawling_servers")
            print("   - 슬롯: 3")
            print("   - 설명: Local crawling servers concurrency control")
            
            return {
                "status": "created",
                "message": "새 풀이 성공적으로 생성되었습니다."
            }
            
    except Exception as e:
        print(f"❌ 풀 생성 중 오류 발생: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

def assign_task_to_pool(**context):
    """태스크를 풀에 할당 (시뮬레이션)"""
    print("📋 태스크 풀 할당 시작...")
    
    try:
        # 데이터베이스 세션 생성
        Session = sessionmaker(bind=settings.engine)
        session = Session()
        
        # 풀 상태 확인
        pool = session.query(Pool).filter(Pool.pool == 'crawling_servers').first()
        
        if not pool:
            print("❌ 풀이 존재하지 않습니다.")
            session.close()
            return {
                "status": "error",
                "message": "풀이 존재하지 않습니다."
            }
        
        print(f"📊 풀 상태:")
        print(f"   - 총 슬롯: {pool.slots}")
        print(f"   - 사용 중: {pool.used_slots}")
        print(f"   - 대기 중: {pool.queued_slots}")
        print(f"   - 사용 가능: {pool.open_slots}")
        
        # 할당 시뮬레이션
        if pool.open_slots > 0:
            print("✅ 태스크를 풀에 할당할 수 있습니다!")
            print(f"   - 사용 가능한 슬롯: {pool.open_slots}")
            print(f"   - 할당 후 사용 중: {pool.used_slots + 1}")
            print(f"   - 할당 후 사용 가능: {pool.open_slots - 1}")
            
            session.close()
            return {
                "status": "can_assign",
                "available_slots": pool.open_slots,
                "message": "태스크 할당 가능"
            }
        else:
            print("⚠️ 풀에 사용 가능한 슬롯이 없습니다.")
            print(f"   - 사용 중: {pool.used_slots}")
            print(f"   - 대기 중: {pool.queued_slots}")
            
            session.close()
            return {
                "status": "no_slots",
                "used_slots": pool.used_slots,
                "queued_slots": pool.queued_slots,
                "message": "사용 가능한 슬롯 없음"
            }
            
    except Exception as e:
        print(f"❌ 풀 할당 확인 중 오류 발생: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

def verify_pool_assignment(**context):
    """풀 할당 상태 최종 확인"""
    print("🔍 풀 할당 상태 최종 확인...")
    
    try:
        # 이전 태스크들의 결과 수집
        ti = context['task_instance']
        
        check_result = ti.xcom_pull(task_ids='check_pool_exists')
        create_result = ti.xcom_pull(task_ids='create_pool_if_not_exists')
        assign_result = ti.xcom_pull(task_ids='assign_task_to_pool')
        
        print("📋 테스트 결과 요약:")
        print(f"   - 풀 존재 확인: {check_result.get('status', 'N/A')}")
        print(f"   - 풀 생성/확인: {create_result.get('status', 'N/A')}")
        print(f"   - 풀 할당 확인: {assign_result.get('status', 'N/A')}")
        
        # 최종 상태 판단
        if (check_result.get('status') in ['exists', 'created'] and 
            assign_result.get('status') in ['can_assign', 'no_slots']):
            print("✅ 풀 테스트 완료!")
            print("💡 crawling_servers 풀이 정상적으로 작동합니다.")
        else:
            print("⚠️ 풀 테스트에 문제가 있습니다.")
        
        return {
            "status": "completed",
            "check_result": check_result,
            "create_result": create_result,
            "assign_result": assign_result
        }
        
    except Exception as e:
        print(f"❌ 최종 확인 중 오류 발생: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# 태스크 정의
check_pool_task = PythonOperator(
    task_id='check_pool_exists',
    python_callable=check_pool_exists,
    dag=dag,
)

create_pool_task = PythonOperator(
    task_id='create_pool_if_not_exists',
    python_callable=create_pool_if_not_exists,
    dag=dag,
)

assign_task = PythonOperator(
    task_id='assign_task_to_pool',
    python_callable=assign_task_to_pool,
    pool='crawling_servers',  # 실제로 풀에 할당
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_pool_assignment',
    python_callable=verify_pool_assignment,
    dag=dag,
)

# 태스크 의존성 설정
check_pool_task >> create_pool_task >> assign_task >> verify_task
