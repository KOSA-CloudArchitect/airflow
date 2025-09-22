from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


with DAG(
    dag_id="redshift_query_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description="Simple Redshift query test DAG (hardcoded job_id)",
    tags=["test", "redshift"],
):

    # 테스트용 하드코딩 job_id (필요 시 UI에서 이 파일 수정하거나 DAG Param으로 확장)
    JOB_ID = "product_20250922_26399"

    # 1) 존재 카운트 확인
    count_query = f"""
    SELECT COUNT(*) AS cnt
    FROM public.realtime_review_collection
    WHERE TRIM(job_id) = '{JOB_ID}';
    """

    count_rows = RedshiftDataOperator(
        task_id="count_rows",
        workgroup_name="hihypipe-redshift-workgroup",
        database="hihypipe",
        sql=count_query,
    )

    # 2) 일별 집계 간단 확인
    daily_agg_query = f"""
    SELECT yyyymmdd,
           COUNT(*) AS total_reviews,
           AVG(rating) AS avg_rating
    FROM public.realtime_review_collection
    WHERE TRIM(job_id) = '{JOB_ID}'
    GROUP BY yyyymmdd
    ORDER BY yyyymmdd
    LIMIT 20;
    """

    daily_agg = RedshiftDataOperator(
        task_id="daily_agg",
        workgroup_name="hihypipe-redshift-workgroup",
        database="hihypipe",
        sql=daily_agg_query,
    )

    count_rows >> daily_agg


