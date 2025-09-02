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
