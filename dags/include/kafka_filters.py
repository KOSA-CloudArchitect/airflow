# dags/lib/kafka_filters.py
import json

def kafka_message_check(message=None, **context):
    """Kafka 메시지가 crawler 완료 신호인지 확인"""
    if message is None:
        # 트리거러가 apply_function 경로를 import하기 전에 프리콜되는 상황 예방
        return False
    try:
        payload = json.loads(message.value().decode("utf-8"))
        dag_run = context.get("dag_run")
        job_id = (dag_run.conf or {}).get("job_id") if dag_run else None
        return payload.get("job_id") == job_id and payload.get("status") == "done"
    except Exception:
        return False