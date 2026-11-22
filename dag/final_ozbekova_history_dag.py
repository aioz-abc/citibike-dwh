"""DAG истории: месячный архив поездок Citi Bike + архив погоды -> витрина «поездки и погода».

Помесячно, с catchup: Airflow сам создаёт запуск на каждый месяц от start_date до end_date.
Logical date = первый день месяца, это и есть ds для всех задач.
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
    "retry_delay": timedelta(minutes=1),
    **c.LOG_CALLBACKS,
}

with student_dag(
    __file__,
    dag_id=f"final_{c.LOGIN}_history",
    description="tripdata S3 + Open-Meteo archive -> stg -> ods -> dm_weather_trips_daily",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 7, 1, tz="America/New_York"),
    # архив поездок за месяц публикуется в середине следующего; дальше — продлить end_date
    end_date=pendulum.datetime(2026, 9, 30, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=[c.LOGIN, "final", "history"],
) as dag:
    ds = {"ds": "{{ ds }}"}

    extract_trips = PythonOperator(
        task_id="extract_trips", python_callable=c.extract_trips_month, op_kwargs=ds,
        execution_timeout=timedelta(minutes=40))
    stg_trips = PythonOperator(
        task_id="stg_trips_daily", python_callable=c.load_stg_trips_daily, op_kwargs=ds)
    ods_trips = PythonOperator(
        task_id="ods_trips_daily", python_callable=c.run_sql_file,
        op_kwargs={"name": "ods_trips_daily", **ds})
    stg_weather = PythonOperator(
        task_id="stg_weather_history", python_callable=c.load_stg_weather_history, op_kwargs=ds)
    ods_weather = PythonOperator(
        task_id="ods_weather_history", python_callable=c.run_sql_file_for_month,
        op_kwargs={"name": "ods_weather", **ds})
    mart = PythonOperator(
        task_id="dm_weather_trips_daily", python_callable=c.run_sql_file,
        op_kwargs={"name": "dm_weather_trips_daily", **ds})
    dq = PythonOperator(
        task_id="dq_history", python_callable=c.run_history_checks, op_kwargs=ds, retries=0)

    extract_trips >> stg_trips >> ods_trips
    stg_weather >> ods_weather
    [ods_trips, ods_weather] >> mart >> dq
