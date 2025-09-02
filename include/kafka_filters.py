import json

def kafka_message_check(message=None, expected_job_id=None):
    if message is None:
        return False

    raw = message.value()
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    print("[kafka_sensor] received:", text[:1000], flush=True)  # 소비 사실을 확실히 노출

    try:
        payload = json.loads(text)
    except Exception as e:
        print("[kafka_sensor] invalid JSON:", e, flush=True)
        return False

    recv_job = payload.get("job_id")
    status   = str(payload.get("status", "")).lower()
    match = (expected_job_id is not None and recv_job == expected_job_id and status == "done")
    print(f"[kafka_sensor] check: expected={expected_job_id} received={recv_job} status={status} -> {match}", flush=True)

    # 성공 시 센서가 이벤트로 xcom에 실어줄 수 있도록 payload 자체를 반환
    return payload if match else None

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
