"""Сборщик снимков GBFS station_status каждые 15 минут -> data/station_status/*.csv.

Это источник CSV-событий для основного DAG; в хранилище он ничего не пишет,
поэтому в etl_load_log не журналируется.
"""
import os
import sys
from datetime import timedelta

import pendulum
from _lib.tenant import student_dag  # обёртка стенда: префикс "<логин>." и доступ к DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import ozbekova_common as c  # noqa: E402

with student_dag(
    __file__,
    dag_id=f"final_{c.LOGIN}_collector",
    description="GBFS station_status -> CSV-снимок каждые 15 минут",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": c.LOGIN, "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=[c.LOGIN, "final"],
) as dag:
    PythonOperator(task_id="collect_station_status", python_callable=c.collect_status_snapshot)
