"""Основной DAG: файлы + API за дату ds -> stg -> ods -> запуск DAG витрин."""
import os
import sys
from datetime import timedelta

import pendulum
from _lib.tenant import student_dag  # обёртка стенда: префикс "<логин>." и доступ к DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
    dag_id=f"final_{c.LOGIN}_pipeline",
    description="Citi Bike GBFS + Open-Meteo -> stg -> ods",
    # 02:00 по Нью-Йорку: файл снимков за прошедший день уже полный
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=[c.LOGIN, "final"],
) as dag:
    ds = {"ds": "{{ ds }}"}

    stg_stations = PythonOperator(
        task_id="stg_stations", python_callable=c.load_stg_stations, op_kwargs=ds)
    stg_station_status = PythonOperator(
        task_id="stg_station_status", python_callable=c.load_stg_station_status, op_kwargs=ds)
    stg_weather = PythonOperator(
        task_id="stg_weather",
        python_callable=c.load_stg_weather,
        op_kwargs={
            **ds,
            # для проверки НФТ-4 адрес можно подменить переменной Airflow
            "api_url": "{{ var.value.get('ozbekova_weather_api_url', '%s') }}" % c.WEATHER_API_URL,
        },
        retries=2,
    )

    ods_tasks = {
        name: PythonOperator(
            task_id=name, python_callable=c.run_sql_file, op_kwargs={"name": name, **ds})
        for name in ("ods_stations", "ods_station_status", "ods_weather")
    }

    trigger_datamart = TriggerDagRunOperator(
        task_id="trigger_datamart",
        trigger_dag_id=f"{c.LOGIN}.final_{c.LOGIN}_datamart",  # student_dag добавляет префикс "<логин>."
        trigger_run_id="pipeline__{{ run_id }}",
        execution_date="{{ logical_date.isoformat() }}",
        reset_dag_run=True,  # повторный запуск за ту же дату пересчитывает витрины
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    stg_stations >> ods_tasks["ods_stations"]
    stg_station_status >> ods_tasks["ods_station_status"]
    stg_weather >> ods_tasks["ods_weather"]
    list(ods_tasks.values()) >> trigger_datamart
