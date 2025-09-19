import json

import json

def kafka_message_check(expected_job_id, message):
    raw = message.value()
    text = raw.decode("utf-8","replace") if isinstance(raw,(bytes,bytearray)) else str(raw)
    print("[kafka_sensor] received:", text[:1000], flush=True)

    try:
        payload = json.loads(text)
    except Exception as e:
        print("[kafka_sensor] invalid JSON:", e, flush=True)
        return False

    recv = payload.get("job_id")
    status = str(payload.get("status","")).lower()
    ok = (recv == expected_job_id and status == "done")
    print(f"[kafka_sensor] check: expected={expected_job_id} received={recv} status={status} -> {ok}", flush=True)
    return payload if ok else None

# include/kafka_filters.py
def any_message_ok(message=None):
    if message is None:
        return False
    val = message.value()
    try:
        s = val.decode("utf-8", errors="replace") if isinstance(val, (bytes, bytearray)) else str(val)
    except Exception:
        s = str(val)
    print("[ANY] got:", s[:1000], flush=True)  # ← 반드시 트리거러 로그에 찍혀야 함
    return True

from airflow.exceptions import AirflowException
from datetime import datetime
from typing import Optional

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # support 'Z' suffix
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except Exception:
        return None

def control_message_check(expected_job_id, expected_step, message, min_timestamp_iso: Optional[str] = None):
    """Control 토픽 메시지 필터링 함수
    
    Args:
        expected_job_id: 예상 job_id
        expected_step: 예상 단계 (collection, transform, analysis, aggregation)
        message: Kafka 메시지
    
    Returns:
        dict: 필터링된 메시지 데이터 또는 None
    """
    raw = message.value()
    text = raw.decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    print(f"[control_sensor] received: {text[:1000]}", flush=True)

    try:
        payload = json.loads(text)
    except Exception as e:
        print(f"[control_sensor] invalid JSON: {e}", flush=True)
        return None

    # 필수 필드 검증
    job_id = payload.get("job_id")
    step = payload.get("step")
    status = str(payload.get("status", "")).lower()
    event_ts = _parse_iso(payload.get("timestamp"))
    min_ts = _parse_iso(min_timestamp_iso)
    
    # 실패 즉시 예외 발생
    if job_id == expected_job_id and step == expected_step and status in {"fail", "failed", "error"}:
        err = payload.get("error_message") or payload
        raise AirflowException(f"Job {job_id} failed at {step}: {err}")

    # 성공 조건 검사
    is_match = (job_id == expected_job_id and step == expected_step and status == "done")

    # 타임스탬프 필터링 - 24시간 이내 메시지는 허용
    # (메시지 발송 시간과 센서 시작 시간의 차이로 인한 문제 해결)
    if is_match and min_ts and event_ts and event_ts < min_ts:
        # 24시간 이내 메시지는 허용
        time_diff_hours = (datetime.now() - event_ts).total_seconds() / 3600
        if time_diff_hours <= 24:
            print(f"[control_sensor] allow recent event: event_ts={event_ts.isoformat()} < min_ts={min_ts.isoformat()} (within 24h: {time_diff_hours:.1f}h)", flush=True)
        else:
            print(f"[control_sensor] skip old event: event_ts={event_ts.isoformat()} < min_ts={min_ts.isoformat()} (too old: {time_diff_hours:.1f}h)", flush=True)
            return None
    
    print(f"[control_sensor] check: expected_job_id={expected_job_id}, expected_step={expected_step}")
    print(f"[control_sensor] received: job_id={job_id}, step={step}, status={status} -> {is_match}", flush=True)
    print(f"[control_sensor] DEBUG - event_ts={event_ts}, min_ts={min_ts}, current_year={datetime.now().year}", flush=True)
    
    return payload if is_match else None
