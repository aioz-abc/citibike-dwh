"""DAG витрин: ods -> datamart -> проверки качества -> копия в ClickHouse.

Запускается основным DAG final_ozbekova_pipeline с той же Logical date.
"""
import os
import sys
from datetime import timedelta

import pendulum
from _lib.tenant import student_dag  # обёртка стенда: префикс "<логин>." и доступ к DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ozbekova_common as c  # noqa: E402

default_args = {
    "owner": c.LOGIN,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    **c.LOG_CALLBACKS,
}

with student_dag(
    __file__,
    dag_id=f"final_{c.LOGIN}_datamart",
    description="ods -> 4 витрины (день, месяц × размер, час × размер, станция) -> ClickHouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[c.LOGIN, "final"],
) as dag:
    ds = {"ds": "{{ ds }}"}

    marts = [
        PythonOperator(task_id=name, python_callable=c.run_sql_file, op_kwargs={"name": name, **ds})
        for name in ("dm_daily_station_activity", "dm_station_size_monthly",
                     "dm_hourly_station_state", "dm_station_daily")
    ]
    dq_checks = PythonOperator(
        task_id="dq_checks", python_callable=c.run_dq_checks, op_kwargs=ds, retries=0)
    copy_to_clickhouse = PythonOperator(
        task_id="copy_to_clickhouse", python_callable=c.copy_daily_to_clickhouse, op_kwargs=ds)

    marts >> dq_checks >> copy_to_clickhouse
