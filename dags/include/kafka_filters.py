import json, logging

def _safe(obj, meth, default=None):
    try: return getattr(obj, meth)()
    except Exception: return default

def kafka_message_check(message=None, **context):
    if message is None:
        logging.debug("[kafka_sensor] called with message=None (render phase)")
        return False

    raw = message.value()
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)

    # 여기까지 오면 '메시지를 Kafka에서 꺼내옴'이 확정 → 반드시 한 줄 찍힘
    logging.info(
        "[kafka_sensor] received: topic=%s partition=%s offset=%s key=%s headers=%s value=%s",
        _safe(message, "topic", "<??>"),
        _safe(message, "partition", "<??>"),
        _safe(message, "offset", "<??>"),
        _safe(message, "key"),
        _safe(message, "headers"),
        text[:2000]
    )

    try:
        payload = json.loads(text)
    except Exception as e:
        logging.warning("[kafka_sensor] invalid JSON; skip. err=%s", e, exc_info=False)
        return False

    dag_run = context.get("dag_run")
    expected = (dag_run.conf or {}).get("job_id") or dag_run.run_id if dag_run else None
    recv_job = payload.get("job_id")
    status = str(payload.get("status", "")).lower()

    match = (recv_job == expected and status == "done")
    logging.info("[kafka_sensor] check: expected=%s received=%s status=%s -> match=%s",expected, recv_job, status, match)

    if recv_job == expected and status in {"error","failed","fail"}:
        raise RuntimeError(f"[kafka_sensor] job failed signaled via Kafka: {payload}")

    return match