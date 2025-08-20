from airflow import DAG
from airflow.decorators import task
# days_ago 대신 datetime 모듈을 사용합니다.
from datetime import datetime, timedelta
import os
import uuid
from kafka import KafkaProducer
import logging

logger = logging.getLogger(__name__)

def _produce_message():
    """Kafka 토픽으로 메시지를 전송하는 함수"""
    topic = os.getenv("KAFKA_TEST_TOPIC", "test.airflow.kafka")
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    
    if not servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS 환경변수가 설정되지 않았습니다.")
    
    trace_id = str(uuid.uuid4())
    logger.info(f"Kafka 연결 시도: {servers}, 토픽: {topic}, trace_id: {trace_id}")
    
    try:
        # Kafka Producer 초기화
        producer = KafkaProducer(
            bootstrap_servers=servers,
            value_serializer=lambda v: v.encode("utf-8"),
            retries=3,
            acks='all'
        )
        
        # 메시지 전송
        payload = f"airflow-test:{trace_id}"
        future = producer.send(topic, payload)
        
        # 전송 완료 대기
        record_md = future.get(timeout=10)
        
        # Producer 정리
        producer.flush()
        producer.close()
        
        logger.info(f"메시지 전송 성공: {record_md.topic}:{record_md.partition}@{record_md.offset} trace={trace_id}")
        return {
            "status": "success",
            "topic": record_md.topic,
            "partition": record_md.partition,
            "offset": record_md.offset,
            "trace_id": trace_id,
            "payload": payload
        }
        
    except Exception as e:
        logger.error(f"Kafka 메시지 전송 실패: {str(e)}")
        raise e

@task()
def test_kafka_connection():
    """Kafka 연결 및 메시지 전송 테스트"""
    try:
        result = _produce_message()
        logger.info(f"테스트 성공: {result}")
        return result
    except Exception as e:
        logger.error(f"테스트 실패: {str(e)}")
        raise e

@task()
def verify_environment():
    """환경변수 및 설정 검증"""
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = os.getenv("KAFKA_TEST_TOPIC")
    
    logger.info(f"KAFKA_BOOTSTRAP_SERVERS: {kafka_servers}")
    logger.info(f"KAFKA_TEST_TOPIC: {kafka_topic}")
    
    if not kafka_servers:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS 환경변수가 설정되지 않았습니다.")
    
    if not kafka_topic:
        raise ValueError("KAFKA_TEST_TOPIC 환경변수가 설정되지 않았습니다.")
    
    return {
        "kafka_servers": kafka_servers,
        "kafka_topic": kafka_topic
    }

# DAG 정의
with DAG(
    dag_id="airflow_to_kafka_connectivity_test",
    # days_ago(1) 대신 datetime.now() - timedelta(days=1)를 사용합니다.
    start_date=datetime.now() - timedelta(days=1),
    schedule_interval=None,
    catchup=False,
    tags=["kafka", "connectivity", "test"],
    description="Airflow에서 Kafka로의 연결 및 메시지 전송 테스트"
) as dag:
    
    # 환경 검증
    env_check = verify_environment()
    
    # Kafka 연결 테스트
    kafka_test = test_kafka_connection()
    
    # 태스크 의존성 설정
    env_check >> kafka_test