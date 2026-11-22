"""Общий код проекта ozbekova: загрузка stg/ods, витрины, проверки, ClickHouse, журнал.

Модуль назван с логином, чтобы не пересечься с модулями других студенток
на общем Airflow (у всех DAG один sys.path).
"""
import csv
import io
import json
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import requests

LOGIN = "ozbekova"
PG_CONN_ID = f"aiacademy_pg_{LOGIN}"
CH_CONN_ID = f"ch_{LOGIN}"
STG, ODS, DM = f"{LOGIN}_stg", f"{LOGIN}_ods", f"{LOGIN}_datamart"

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_DIR, "data")
SQL_DIR = os.path.join(PROJECT_DIR, "sql")
STATIONS_FILE = os.path.join(DATA_DIR, "citibike_stations.csv")
STATUS_DIR = os.path.join(DATA_DIR, "station_status")
TRIPS_DIR = os.path.join(DATA_DIR, "trips")

NYC_TZ = ZoneInfo("America/New_York")
NYC_LAT, NYC_LON = 40.7128, -74.0060
GBFS_STATUS_URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TRIPS_URL = "https://s3.amazonaws.com/tripdata/{ym}-citibike-tripdata.zip"
TRIP_MIN_SEC = 60          # короче минуты — ложный старт, Citi Bike их тоже исключает
TRIP_MAX_SEC = 3 * 3600    # в сумму длительности поездка идёт не больше чем на 3 часа
WEATHER_DAILY = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "wind_speed_10m_max"]
HTTP_TIMEOUT = (5, 20)  # connect, read — секунды
HTTP_ATTEMPTS = 3

STATIONS_COLUMNS = [
    "station_id", "name", "lat", "lon", "capacity", "bikes_available",
    "ebikes_available", "docks_available", "is_renting", "is_returning",
]
STATUS_COLUMNS = [
    "snapshot_ts", "station_id", "legacy_id", "num_bikes_available", "num_ebikes_available",
    "num_bikes_disabled", "num_docks_available", "num_docks_disabled",
    "is_installed", "is_renting", "is_returning", "last_reported",
]
TRIPS_COLUMNS = [
    "trip_date", "start_station_id", "member_casual", "rideable_type", "trips", "duration_sec_sum",
]

# task_id -> источник для журнала etl_load_log
TASK_SOURCES = {
    "stg_stations": "file:citibike_stations.csv",
    "stg_station_status": "file:station_status_<ds>.csv",
    "stg_weather": "api:open-meteo",
    "ods_stations": f"{STG}.stg_stations",
    "ods_station_status": f"{STG}.stg_station_status",
    "ods_weather": f"{STG}.stg_weather",
    "trigger_datamart": f"dag:final_{LOGIN}_datamart",
    "dm_daily_station_activity": f"{ODS}.ods_station_status+ods_weather",
    "dm_station_size_monthly": f"{ODS}.ods_station_status+ods_stations+ods_weather",
    "dm_hourly_station_state": f"{ODS}.ods_station_status+ods_stations",
    "dm_station_daily": f"{ODS}.ods_station_status+ods_stations",
    "dq_checks": f"{ODS}+{DM}",
    "copy_to_clickhouse": f"{DM}.dm_daily_station_activity",
    # DAG истории: поездки за месяц + архив погоды
    "extract_trips": "s3:tripdata/<YYYYMM>-citibike-tripdata.zip",
    "stg_trips_daily": "file:trips_daily_<YYYYMM>.csv",
    "stg_weather_history": "api:open-meteo-archive",
    "ods_trips_daily": f"{STG}.stg_trips_daily",
    "ods_weather_history": f"{STG}.stg_weather",
    "dm_weather_trips_daily": f"{ODS}.ods_trips_daily+ods_weather",
    "dq_history": f"{ODS}+{DM}",
}


# ---------------------------------------------------------------- подключения

def pg_connect():
    """psycopg2-подключение через Airflow Connection (пароль хранится только там)."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=PG_CONN_ID).get_conn()


def run_sql_file(name, ds):
    """Выполняет sql/<name>.sql одной транзакцией. Возвращает rowcount последней команды."""
    with open(os.path.join(SQL_DIR, f"{name}.sql"), encoding="utf-8") as f:
        sql = f.read()
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, {"ds": ds})
            return cur.rowcount
    finally:
        conn.close()


# ---------------------------------------------------------------- журнал (callbacks)

def _log_conn():
    conn = pg_connect()
    conn.autocommit = True  # журнал пишется отдельно от транзакции задачи
    return conn


def _log_key(context):
    ti = context["ti"]
    return ti.dag_id, ti.task_id, context["run_id"], ti.try_number


def log_start(context):
    dag_id, task_id, run_id, try_number = _log_key(context)
    conn = _log_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""insert into {STG}.etl_load_log
                        (dag_id, task_id, run_id, try_number, source, ds, status)
                    values (%s, %s, %s, %s, %s, %s, 'running')""",
                (dag_id, task_id, run_id, try_number,
                 TASK_SOURCES.get(task_id, task_id), context["ds"]),
            )
    finally:
        conn.close()


def _log_finish(context, status, rows=None, error=None):
    dag_id, task_id, run_id, try_number = _log_key(context)
    conn = _log_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""update {STG}.etl_load_log
                    set finished_at = now(), status = %s, rows_count = %s, error_text = %s
                    where id = (select max(id) from {STG}.etl_load_log
                                where dag_id = %s and task_id = %s and run_id = %s
                                  and try_number = %s)""",
                (status, rows, error, dag_id, task_id, run_id, try_number),
            )
    finally:
        conn.close()


def log_success(context):
    rows = context["ti"].xcom_pull(task_ids=context["ti"].task_id)
    _log_finish(context, "success", rows if isinstance(rows, int) else None)


def log_failure(context):
    exc = context.get("exception")
    _log_finish(context, "failed", error=f"{type(exc).__name__}: {exc}"[:4000] if exc else "unknown error")


LOG_CALLBACKS = {
    "on_execute_callback": log_start,
    "on_success_callback": log_success,
    "on_failure_callback": log_failure,
    "on_retry_callback": log_failure,
}


# ---------------------------------------------------------------- stg

def _copy_rows(cur, table, columns, rows):
    buf = io.StringIO()
    csv.writer(buf, lineterminator="\n").writerows(rows)
    buf.seek(0)
    cur.copy_expert(
        f"copy {table} ({', '.join(columns)}) from stdin with (format csv)", buf
    )


def _read_csv(path, expected_columns):
    if not os.path.exists(path):
        raise FileNotFoundError(f"нет файла {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header != expected_columns:
            raise ValueError(f"{os.path.basename(path)}: неожиданный заголовок {header}")
        return [row for row in reader if row]


def load_stg_stations(ds):
    """Справочник станций: перезаписывается целиком."""
    rows = _read_csv(STATIONS_FILE, STATIONS_COLUMNS)
    loaded_at = datetime.now(timezone.utc).isoformat()
    fname = os.path.basename(STATIONS_FILE)
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"truncate {STG}.stg_stations")
            _copy_rows(cur, f"{STG}.stg_stations",
                       STATIONS_COLUMNS + ["_ds", "_source_file", "_loaded_at"],
                       [r + [ds, fname, loaded_at] for r in rows])
    finally:
        conn.close()
    return len(rows)


def status_file(ds):
    return os.path.join(STATUS_DIR, f"station_status_{ds}.csv")


def load_stg_station_status(ds):
    """События: файл снимков station_status за дату ds."""
    path = status_file(ds)
    rows = _read_csv(path, STATUS_COLUMNS)
    if not rows:
        raise ValueError(f"{path}: файл пустой")
    loaded_at = datetime.now(timezone.utc).isoformat()
    fname = os.path.basename(path)
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"delete from {STG}.stg_station_status where _ds = %s", (ds,))
            _copy_rows(cur, f"{STG}.stg_station_status",
                       STATUS_COLUMNS + ["_ds", "_source_file", "_loaded_at"],
                       [r + [ds, fname, loaded_at] for r in rows])
    finally:
        conn.close()
    return len(rows)


def _http_get_json(url, params=None):
    """GET с таймаутом и повторами; после последней неудачи — исключение."""
    last_error = None
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            last_error = e
            if attempt < HTTP_ATTEMPTS:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{url}: {HTTP_ATTEMPTS} попытки не удались: {last_error}")


def load_stg_weather(ds, api_url=WEATHER_API_URL):
    """API Open-Meteo: погода в Нью-Йорке за день ds. Сначала весь ответ, потом запись."""
    payload = _http_get_json(api_url, params={
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": ds,
        "end_date": ds,
        "daily": ",".join(WEATHER_DAILY),
        "timezone": "America/New_York",
    })
    daily = payload.get("daily") or {}
    if daily.get("time") != [ds]:
        raise ValueError(f"ответ API не за {ds}: {daily.get('time')}")
    values = [daily.get(k, [None])[0] for k in WEATHER_DAILY]
    row = [ds] + ["" if v is None else str(v) for v in values] + [
        json.dumps(payload, ensure_ascii=False), ds, api_url,
        datetime.now(timezone.utc).isoformat(),
    ]
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"delete from {STG}.stg_weather where _ds = %s", (ds,))
            _copy_rows(cur, f"{STG}.stg_weather",
                       ["date"] + WEATHER_DAILY + ["raw_response", "_ds", "_source_file", "_loaded_at"],
                       [row])
    finally:
        conn.close()
    return 1


# ---------------------------------------------------------------- сбор снимков GBFS

def collect_status_snapshot(url=GBFS_STATUS_URL):
    """Скачивает station_status и дописывает снимок в data/station_status/station_status_<дата NY>.csv."""
    payload = _http_get_json(url)
    snapshot = datetime.fromtimestamp(payload["last_updated"], tz=timezone.utc)
    path = status_file(snapshot.astimezone(NYC_TZ).date().isoformat())
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            if any(r[:1] == [snapshot.isoformat()] for r in csv.reader(f)):
                return 0  # этот снимок уже записан
    rows = [
        [snapshot.isoformat(), s["station_id"], s.get("legacy_id", "")]
        + [s.get(c, 0) for c in STATUS_COLUMNS[3:11]]
        + [s.get("last_reported", "")]
        for s in payload["data"]["stations"]
    ]
    os.makedirs(STATUS_DIR, exist_ok=True)
    new_file = not os.path.exists(path)
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    if new_file:
        writer.writerow(STATUS_COLUMNS)
    writer.writerows(rows)
    with open(path, "a", newline="", encoding="utf-8") as f:
        f.write(buf.getvalue())  # одна запись — снимок не разрывается
    return len(rows)


# ---------------------------------------------------------------- история: поездки и архив погоды
# DAG истории работает помесячно: ds = первый день месяца.

def _month_days(ds):
    start = date.fromisoformat(ds).replace(day=1)
    nxt = (start.replace(day=28) + (date.resolution * 4)).replace(day=1)
    return [date.fromordinal(d).isoformat() for d in range(start.toordinal(), nxt.toordinal())]


def trips_file(ds):
    return os.path.join(TRIPS_DIR, f"trips_daily_{ds[:7].replace('-', '')}.csv")


def extract_trips_month(ds, url_template=TRIPS_URL):
    """Месячный архив поездок Citi Bike -> data/trips/trips_daily_<YYYYMM>.csv.

    В архиве 4–5 млн поездок в месяц; класть их сырыми в общий Postgres стенда не стоит,
    поэтому при извлечении они сворачиваются до день × станция × тип клиента × тип велосипеда.
    Этот файл и есть источник для stg_trips_daily. Поездки короче минуты не считаются.
    """
    import tempfile
    import zipfile

    month = ds[:7]
    url = url_template.format(ym=month.replace("-", ""))
    agg = {}
    with tempfile.TemporaryDirectory(prefix=f"{LOGIN}_trips_") as tmp:
        zpath = os.path.join(tmp, "trips.zip")
        with requests.get(url, stream=True, timeout=(10, 120)) as resp:
            resp.raise_for_status()
            with open(zpath, "wb") as f:
                for chunk in resp.iter_content(8 << 20):
                    f.write(chunk)
        with zipfile.ZipFile(zpath) as z:
            names = [n for n in z.namelist()
                     if n.lower().endswith(".csv") and "__MACOSX" not in n]
            if not names:
                raise ValueError(f"{url}: в архиве нет CSV")
            for name in names:
                with z.open(name) as raw:
                    for r in csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline="")):
                        started = r["started_at"]
                        if started[:7] != month:
                            continue  # поездка начата в соседнем месяце
                        try:
                            sec = (datetime.fromisoformat(r["ended_at"])
                                   - datetime.fromisoformat(started)).total_seconds()
                        except ValueError:
                            continue
                        if sec < TRIP_MIN_SEC:
                            continue
                        key = (started[:10], r["start_station_id"] or "unknown",
                               r["member_casual"], r["rideable_type"])
                        a = agg.setdefault(key, [0, 0.0])
                        a[0] += 1
                        a[1] += min(sec, TRIP_MAX_SEC)
    if not agg:
        raise ValueError(f"{url}: нет поездок за {month}")
    os.makedirs(TRIPS_DIR, exist_ok=True)
    path = trips_file(ds)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(TRIPS_COLUMNS)
        for key in sorted(agg):
            w.writerow(list(key) + [agg[key][0], int(agg[key][1])])
    os.replace(tmp_path, path)  # файл появляется целиком или не появляется
    os.chmod(path, 0o666)       # файл пишет airflow, а читать/перезаписывать может и Jupyter
    return sum(v[0] for v in agg.values())


def load_stg_trips_daily(ds):
    """Файл-агрегат поездок за месяц ds -> stg_trips_daily (как есть, text)."""
    path = trips_file(ds)
    rows = _read_csv(path, TRIPS_COLUMNS)
    if not rows:
        raise ValueError(f"{path}: файл пустой")
    loaded_at = datetime.now(timezone.utc).isoformat()
    fname = os.path.basename(path)
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"delete from {STG}.stg_trips_daily where _ds = %s", (ds,))
            _copy_rows(cur, f"{STG}.stg_trips_daily",
                       TRIPS_COLUMNS + ["_ds", "_source_file", "_loaded_at"],
                       [r + [ds, fname, loaded_at] for r in rows])
    finally:
        conn.close()
    return len(rows)


def load_stg_weather_history(ds, api_url=WEATHER_ARCHIVE_URL):
    """Архив Open-Meteo: погода за каждый день месяца ds -> stg_weather (по строке на день, _ds = день)."""
    days = _month_days(ds)
    payload = _http_get_json(api_url, params={
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": days[0],
        "end_date": days[-1],
        "daily": ",".join(WEATHER_DAILY),
        "timezone": "America/New_York",
    })
    daily = payload.get("daily") or {}
    if daily.get("time") != days:
        raise ValueError(f"архив вернул не те дни: {daily.get('time')}")
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for i, day in enumerate(days):
        values = [daily.get(k, [None] * len(days))[i] for k in WEATHER_DAILY]
        rows.append([day] + ["" if v is None else str(v) for v in values] + [
            json.dumps({"time": day, **dict(zip(WEATHER_DAILY, values))}),
            day, api_url, loaded_at,
        ])
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"delete from {STG}.stg_weather where _ds between %s and %s",
                        (days[0], days[-1]))
            _copy_rows(cur, f"{STG}.stg_weather",
                       ["date"] + WEATHER_DAILY + ["raw_response", "_ds", "_source_file", "_loaded_at"],
                       rows)
    finally:
        conn.close()
    return len(rows)


def run_sql_file_for_month(name, ds):
    """sql/<name>.sql, рассчитанный на один день, — по очереди за каждый день месяца ds."""
    return sum(run_sql_file(name, day) for day in _month_days(ds))


# ---------------------------------------------------------------- проверки качества

ACCEPTED_STATION_STATE = ("offline", "empty", "full", "normal")
ACCEPTED_TICKET_STATUS = ("open", "in_progress", "closed")
ACCEPTED_CAPACITY_CATEGORY = ("small", "medium", "large", "unknown")

DQ_CHECKS = [
    ("ods_stations.station_id unique",
     f"select count(*) - count(distinct station_id) from {ODS}.ods_stations"),
    ("ods_stations.station_id not_null",
     f"select count(*) from {ODS}.ods_stations where station_id is null"),
    ("ods_stations.capacity_category accepted_values",
     f"select count(*) from {ODS}.ods_stations where capacity_category not in %(capacity)s"),
    ("ods_station_status (station_id, snapshot_ts) unique за ds",
     f"""select count(*) from (select 1 from {ODS}.ods_station_status where status_date = %(ds)s
         group by station_id, snapshot_ts having count(*) > 1) d"""),
    ("ods_station_status keys not_null за ds",
     f"""select count(*) from {ODS}.ods_station_status
         where status_date = %(ds)s and (station_id is null or snapshot_ts is null)"""),
    ("ods_station_status.station_state accepted_values",
     f"""select count(*) from {ODS}.ods_station_status
         where status_date = %(ds)s and station_state not in %(state)s"""),
    ("ods_weather за ds есть ровно одна строка",
     f"select abs(count(*) - 1) from {ODS}.ods_weather where weather_date = %(ds)s"),
    ("ods_maintenance_tickets.ticket_id unique",
     f"select count(*) - count(distinct ticket_id) from {ODS}.ods_maintenance_tickets"),
    ("ods_maintenance_tickets.status accepted_values",
     f"select count(*) from {ODS}.ods_maintenance_tickets where status not in %(ticket)s"),
    ("dm_daily_station_activity.activity_date unique",
     f"select count(*) - count(distinct activity_date) from {DM}.dm_daily_station_activity"),
    ("dm_daily_station_activity за ds есть ровно одна строка",
     f"select abs(count(*) - 1) from {DM}.dm_daily_station_activity where activity_date = %(ds)s"),
    ("dm_station_size_monthly (month_start, capacity_category) unique + not_null",
     f"""select count(*) - count(distinct (month_start, capacity_category))
                + count(*) filter (where month_start is null or capacity_category is null)
         from {DM}.dm_station_size_monthly"""),
    ("сверка empty_snapshots: день-витрина = месяц-витрина за месяц ds",
     f"""select abs(
           coalesce((select sum(empty_snapshots) from {DM}.dm_daily_station_activity
                     where date_trunc('month', activity_date) = date_trunc('month', %(ds)s::date)), 0)
         - coalesce((select sum(empty_snapshots) from {DM}.dm_station_size_monthly
                     where month_start = date_trunc('month', %(ds)s::date)), 0))"""),
    ("сверка empty_snapshots за ds: день-витрина = часовая витрина",
     f"""select abs(
           coalesce((select sum(empty_snapshots) from {DM}.dm_daily_station_activity
                     where activity_date = %(ds)s), 0)
         - coalesce((select sum(empty_snapshots) from {DM}.dm_hourly_station_state
                     where activity_date = %(ds)s), 0))"""),
    ("сверка empty_snapshots за ds: день-витрина = витрина по станциям",
     f"""select abs(
           coalesce((select sum(empty_snapshots) from {DM}.dm_daily_station_activity
                     where activity_date = %(ds)s), 0)
         - coalesce((select sum(empty_snapshots) from {DM}.dm_station_daily
                     where activity_date = %(ds)s), 0))"""),
]


# DAG истории, ds = первый день месяца
HISTORY_MONTH = "trip_date >= %(ds)s::date and trip_date < %(ds)s::date + interval '1 month'"
HISTORY_DQ_CHECKS = [
    ("ods_trips_daily: поездки есть за каждый день месяца",
     f"""select count(*) from generate_series(%(ds)s::date, %(ds)s::date + interval '1 month' - interval '1 day',
                                       interval '1 day') d(day)
         where not exists (select 1 from {ODS}.ods_trips_daily t where t.trip_date = d.day)"""),
    ("ods_trips_daily: trips > 0",
     f"select count(*) from {ODS}.ods_trips_daily where {HISTORY_MONTH} and trips <= 0"),
    ("ods_trips_daily.member_casual accepted_values",
     f"""select count(*) from {ODS}.ods_trips_daily
         where {HISTORY_MONTH} and member_casual not in ('member', 'casual')"""),
    ("ods_weather: ровно одна строка на каждый день месяца",
     f"""select count(*) from generate_series(%(ds)s::date, %(ds)s::date + interval '1 month' - interval '1 day',
                                       interval '1 day') d(day)
         where (select count(*) from {ODS}.ods_weather w where w.weather_date = d.day) <> 1"""),
    ("dm_weather_trips_daily: строка на каждый день месяца",
     f"""select abs(extract(day from %(ds)s::date + interval '1 month' - interval '1 day')
                    - (select count(*) from {DM}.dm_weather_trips_daily where {HISTORY_MONTH}))"""),
    ("сверка trips: витрина = ods за месяц",
     f"""select abs(
           coalesce((select sum(trips_cnt) from {DM}.dm_weather_trips_daily where {HISTORY_MONTH}), 0)
         - coalesce((select sum(trips) from {ODS}.ods_trips_daily where {HISTORY_MONTH}), 0))"""),
]


def _run_checks(checks, params):
    failed = []
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            for name, sql in checks:
                cur.execute(sql, params)
                bad = cur.fetchone()[0]
                print(f"[{'FAIL' if bad else 'OK  '}] {name}: {bad}")
                if bad:
                    failed.append(f"{name} (плохих строк: {bad})")
    finally:
        conn.close()
    if failed:
        raise ValueError("проверки качества не прошли: " + "; ".join(failed))
    return len(checks)


def run_dq_checks(ds):
    return _run_checks(DQ_CHECKS, {
        "ds": ds,
        "state": ACCEPTED_STATION_STATE,
        "ticket": ACCEPTED_TICKET_STATUS,
        "capacity": ACCEPTED_CAPACITY_CATEGORY,
    })


def run_history_checks(ds):
    return _run_checks(HISTORY_DQ_CHECKS, {"ds": ds})


# ---------------------------------------------------------------- ClickHouse

CH_DDL_PATH = os.path.join(SQL_DIR, "02_ddl_clickhouse.sql")
CH_TABLE = f"{LOGIN}.dm_daily_station_activity"


def _ch_client():
    """Функция-запрос к ClickHouse по HTTP (порт 8123) с данными из Airflow Connection."""
    from airflow.hooks.base import BaseHook

    c = BaseHook.get_connection(CH_CONN_ID)
    base = f"{c.schema or 'http'}://{c.host}:{c.port or 8123}/"
    headers = {"X-ClickHouse-User": c.login or "default", "X-ClickHouse-Key": c.password or ""}

    def query(sql, data=None):
        resp = requests.post(base, params={"query": sql}, data=data,
                             headers=headers, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"ClickHouse {resp.status_code}: {resp.text[:500]}")
        return resp.text

    return query


def _json_value(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    return v


def copy_daily_to_clickhouse(ds):
    """Копирует строку витрины за ds: удалить в ClickHouse за ds, затем вставить."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"select * from {DM}.dm_daily_station_activity where activity_date = %s", (ds,)
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, map(_json_value, r))) for r in cur.fetchall()]
    finally:
        conn.close()

    ch = _ch_client()
    with open(CH_DDL_PATH, encoding="utf-8") as f:
        statements = [
            "\n".join(l for l in s.splitlines() if not l.strip().startswith("--")).strip()
            for s in f.read().split(";")
        ]
    for stmt in filter(None, statements):
        ch(stmt)
    ch(f"alter table {CH_TABLE} delete where activity_date = '{ds}' settings mutations_sync = 2")
    if rows:
        ch(f"insert into {CH_TABLE} format JSONEachRow",
           data="\n".join(json.dumps(r) for r in rows).encode())
    return len(rows)
