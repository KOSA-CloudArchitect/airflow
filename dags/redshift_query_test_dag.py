from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook


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

    def print_query_results(sql: str, max_rows: int = 50, **_):
        hook = RedshiftDataHook()
        stmt = hook.execute_query(
            workgroup_name="hihypipe-redshift-workgroup",
            database="hihypipe",
            sql=sql,
        )
        result = hook.get_query_results(stmt.id)
        # 컬럼명 추출
        cols = [c.name for c in getattr(result, "column_metadata", [])]
        print(f"[TEST] Columns: {cols}")
        rows = getattr(result, "records", []) or []
        print(f"[TEST] Row count (capped to print): {len(rows)}")
        for i, rec in enumerate(rows[:max_rows]):
            values = []
            for f in rec:
                if f.string_value is not None:
                    values.append(f.string_value)
                elif f.long_value is not None:
                    values.append(f.long_value)
                elif f.double_value is not None:
                    values.append(f.double_value)
                elif f.boolean_value is not None:
                    values.append(f.boolean_value)
                else:
                    values.append(None)
            print(f"[TEST] Row {i}: {values}")

    print_count = PythonOperator(
        task_id="print_count",
        python_callable=print_query_results,
        op_kwargs={"sql": count_query, "max_rows": 5},
    )

    print_daily = PythonOperator(
        task_id="print_daily",
        python_callable=print_query_results,
        op_kwargs={"sql": daily_agg_query, "max_rows": 50},
    )

    count_rows >> daily_agg >> [print_count, print_daily]


