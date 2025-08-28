"""
Airflow Pool Management DAG
HiHyPipe 프로젝트의 crawling_servers 풀을 생성하고 관리하는 DAG

이 DAG은 Airflow REST API를 사용하여 풀을 생성, 업데이트, 모니터링합니다.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import json
import logging

# 기본 인수 설정
default_args = {
    'owner': 'hihypipe',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'airflow_pool_management',
    default_args=default_args,
    description='Airflow 풀 관리 및 모니터링',
    schedule_interval='@daily',  # 매일 실행
    catchup=False,
    tags=['hihypipe', 'pool-management', 'admin'],
)

def create_crawling_servers_pool(**context):
    """
    crawling_servers 풀을 생성하거나 업데이트합니다.
    
    Returns:
        dict: 생성/업데이트 결과
    """
    # Airflow 설정에서 기본값 가져오기
    airflow_base_url = Variable.get("AIRFLOW_BASE_URL", "http://localhost:8080")
    airflow_token = Variable.get("AIRFLOW_TOKEN", "admin")
    
    # 풀 설정
    pool_config = {
        "name": "crawling_servers",
        "slots": 3,
        "description": "Local crawling servers concurrency control"
    }
    
    headers = {
        "Authorization": f"Bearer {airflow_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # 기존 풀 확인
        check_url = f"{airflow_base_url}/api/v1/pools/crawling_servers"
        response = requests.get(check_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            # 기존 풀이 존재하는 경우 업데이트
            logging.info("기존 crawling_servers 풀을 업데이트합니다.")
            update_url = f"{airflow_base_url}/api/v1/pools/crawling_servers"
            response = requests.patch(update_url, json=pool_config, headers=headers, timeout=30)
            action = "updated"
        else:
            # 풀이 존재하지 않는 경우 생성
            logging.info("새로운 crawling_servers 풀을 생성합니다.")
            create_url = f"{airflow_base_url}/api/v1/pools"
            response = requests.post(create_url, json=pool_config, headers=headers, timeout=30)
            action = "created"
        
        if response.status_code in [200, 201]:
            logging.info(f"crawling_servers 풀이 성공적으로 {action}되었습니다.")
            return {
                "status": "success",
                "action": action,
                "pool": pool_config,
                "response": response.json()
            }
        else:
            error_msg = f"풀 {action} 실패: {response.status_code} - {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"API 요청 실패: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"풀 생성/업데이트 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def verify_pool_configuration(**context):
    """
    crawling_servers 풀의 설정을 확인하고 검증합니다.
    
    Returns:
        dict: 검증 결과
    """
    airflow_base_url = Variable.get("AIRFLOW_BASE_URL", "http://localhost:8080")
    airflow_token = Variable.get("AIRFLOW_TOKEN", "admin")
    
    headers = {
        "Authorization": f"Bearer {airflow_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # 풀 정보 조회
        url = f"{airflow_base_url}/api/v1/pools/crawling_servers"
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            pool_info = response.json()
            
            # 설정 검증
            expected_slots = 3
            expected_description = "Local crawling servers concurrency control"
            
            if (pool_info.get("slots") == expected_slots and 
                pool_info.get("description") == expected_description):
                
                logging.info("풀 설정이 올바르게 구성되었습니다.")
                return {
                    "status": "verified",
                    "pool_info": pool_info,
                    "message": "풀 설정이 요구사항과 일치합니다."
                }
            else:
                error_msg = f"풀 설정이 올바르지 않습니다. 예상: slots={expected_slots}, description='{expected_description}', 실제: {pool_info}"
                logging.error(error_msg)
                raise Exception(error_msg)
        else:
            error_msg = f"풀 정보 조회 실패: {response.status_code} - {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"API 요청 실패: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"풀 검증 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def monitor_pool_usage(**context):
    """
    crawling_servers 풀의 사용 현황을 모니터링합니다.
    
    Returns:
        dict: 모니터링 결과
    """
    airflow_base_url = Variable.get("AIRFLOW_BASE_URL", "http://localhost:8080")
    airflow_token = Variable.get("AIRFLOW_TOKEN", "admin")
    
    headers = {
        "Authorization": f"Bearer {airflow_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # 풀 정보 조회
        url = f"{airflow_base_url}/api/v1/pools/crawling_servers"
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            pool_info = response.json()
            
            # 사용 현황 분석
            total_slots = pool_info.get("slots", 0)
            used_slots = pool_info.get("used_slots", 0)
            queued_slots = pool_info.get("queued_slots", 0)
            open_slots = pool_info.get("open_slots", 0)
            
            usage_info = {
                "total_slots": total_slots,
                "used_slots": used_slots,
                "queued_slots": queued_slots,
                "open_slots": open_slots,
                "usage_percentage": (used_slots / total_slots * 100) if total_slots > 0 else 0,
                "timestamp": datetime.now().isoformat()
            }
            
            logging.info(f"풀 사용 현황: {usage_info}")
            
            # 경고 조건 확인
            if usage_percentage > 80:
                logging.warning(f"풀 사용률이 높습니다: {usage_percentage:.1f}%")
            elif usage_percentage < 20:
                logging.info(f"풀 사용률이 낮습니다: {usage_percentage:.1f}%")
            
            return {
                "status": "monitored",
                "usage_info": usage_info,
                "message": "풀 사용 현황이 성공적으로 모니터링되었습니다."
            }
        else:
            error_msg = f"풀 정보 조회 실패: {response.status_code} - {response.text}"
            logging.error(error_msg)
            raise Exception(error_msg)
            
    except requests.exceptions.RequestException as e:
        error_msg = f"API 요청 실패: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f"풀 모니터링 중 오류 발생: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# 태스크 정의
create_pool_task = PythonOperator(
    task_id='create_crawling_servers_pool',
    python_callable=create_crawling_servers_pool,
    dag=dag,
)

verify_pool_task = PythonOperator(
    task_id='verify_pool_configuration',
    python_callable=verify_pool_configuration,
    dag=dag,
)

monitor_pool_task = PythonOperator(
    task_id='monitor_pool_usage',
    python_callable=monitor_pool_usage,
    dag=dag,
)

# 태스크 의존성 설정
create_pool_task >> verify_pool_task >> monitor_pool_task

