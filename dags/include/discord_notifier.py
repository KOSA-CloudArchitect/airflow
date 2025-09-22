from datetime import datetime
import json
import logging

from airflow.models import Variable

try:
    import requests
except Exception:  # 런타임에 requests 부재 시 ImportError 방지
    requests = None


def send_discord_failure_alert(context, job_id, step):
    """Discord Webhook으로 실패 알림 전송

    요구사항:
    - Airflow Variable: DISCORD_WEBHOOK_URL 에 Webhook URL 저장
    - 런타임 환경에 requests 설치
    """
    webhook_url = Variable.get("DISCORD_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        logging.warning("[Discord Notify] DISCORD_WEBHOOK_URL Airflow Variable not set. Skipping Discord alert.")
        return

    if requests is None:
        logging.warning("[Discord Notify] requests package not available. Skipping Discord alert.")
        return

    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else (getattr(context.get("dag"), "dag_id", None) if context.get("dag") else None)
    run_id = context.get("run_id") or (context.get("dag_run").run_id if context.get("dag_run") else None)
    try_number = ti.try_number if ti else None
    log_url = ti.log_url if ti else None
    execution_date = (context.get("execution_date") or datetime.utcnow()).isoformat()

    content = (
        f"🚨 Airflow Task Failure\n"
        f"• DAG: {dag_id}\n"
        f"• Task: {ti.task_id if ti else 'unknown'}\n"
        f"• Step: {step}\n"
        f"• Job ID: {job_id}\n"
        f"• Run ID: {run_id}\n"
        f"• Try: {try_number}\n"
        f"• When: {execution_date}\n"
        f"• Logs: {log_url}"
    )

    payload = {"content": content}
    headers = {"Content-Type": "application/json"}
    response = requests.post(webhook_url, headers=headers, data=json.dumps(payload), timeout=5)
    if response.status_code >= 300:
        raise RuntimeError(f"Discord webhook returned status {response.status_code}: {response.text}")








