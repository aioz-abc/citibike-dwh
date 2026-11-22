-- Схемы и таблицы проекта ozbekova (база aiacademy).
-- Скрипт идемпотентен: можно запускать повторно.
-- psql "host=... dbname=aiacademy user=ozbekova" -f sql/01_ddl_postgres.sql

create schema if not exists ozbekova_stg;
create schema if not exists ozbekova_ods;
create schema if not exists ozbekova_datamart;

-- ---------------------------------------------------------------- stg
-- Данные "как есть": все колонки text + служебные _ds, _source_file, _loaded_at.

create table if not exists ozbekova_stg.stg_stations (
    station_id        text,
    name              text,
    lat               text,
    lon               text,
    capacity          text,
    bikes_available   text,
    ebikes_available  text,
    docks_available   text,
    is_renting        text,
    is_returning      text,
    _ds               date        not null,
    _source_file      text        not null,
    _loaded_at        timestamptz not null default now()
);

create table if not exists ozbekova_stg.stg_station_status (
    snapshot_ts           text,
    station_id            text,
    legacy_id             text,
    num_bikes_available   text,
    num_ebikes_available  text,
    num_bikes_disabled    text,
    num_docks_available   text,
    num_docks_disabled    text,
    is_installed          text,
    is_renting            text,
    is_returning          text,
    last_reported         text,
    _ds                   date        not null,
    _source_file          text        not null,
    _loaded_at            timestamptz not null default now()
);
create index if not exists stg_station_status_ds_idx on ozbekova_stg.stg_station_status (_ds);

create table if not exists ozbekova_stg.stg_weather (
    date                text,
    temperature_2m_max  text,
    temperature_2m_min  text,
    precipitation_sum   text,
    wind_speed_10m_max  text,
    raw_response        text,
    _ds                 date        not null,
    _source_file        text        not null,
    _loaded_at          timestamptz not null default now()
);

-- Поездки Citi Bike за месяц, свёрнутые при извлечении до день × станция × типы (DAG истории).
create table if not exists ozbekova_stg.stg_trips_daily (
    trip_date         text,
    start_station_id  text,
    member_casual     text,
    rideable_type     text,
    trips             text,
    duration_sec_sum  text,
    _ds               date        not null,  -- первый день месяца
    _source_file      text        not null,
    _loaded_at        timestamptz not null default now()
);

-- Журнал загрузок: одна строка на каждую попытку каждой задачи.
create table if not exists ozbekova_stg.etl_load_log (
    id           bigserial primary key,
    dag_id       text        not null,
    task_id      text        not null,
    run_id       text        not null,
    try_number   int         not null,
    source       text        not null,
    ds           date        not null,
    started_at   timestamptz not null default now(),
    finished_at  timestamptz,
    status       text        not null check (status in ('running', 'success', 'failed')),
    rows_count   bigint,
    error_text   text
);

-- ---------------------------------------------------------------- ods

create table if not exists ozbekova_ods.ods_stations (
    station_id         text primary key,
    station_name       text          not null,
    latitude           numeric(9, 6),
    longitude          numeric(9, 6),
    capacity           int,
    capacity_category  text          not null,  -- small / medium / large / unknown
    is_renting         boolean,
    is_returning       boolean,
    _ds                date          not null,
    _loaded_at         timestamptz   not null default now()
);

create table if not exists ozbekova_ods.ods_station_status (
    station_id         text        not null,
    snapshot_ts        timestamptz not null,
    status_date        date        not null,  -- дата файла (= ds загрузки), Нью-Йорк
    legacy_id          text,
    bikes_available    int         not null,
    ebikes_available   int         not null,
    bikes_disabled     int         not null,
    docks_available    int         not null,
    docks_disabled     int         not null,
    is_installed       boolean     not null,
    is_renting         boolean     not null,
    is_returning       boolean     not null,
    last_reported_at   timestamptz,
    station_state      text        not null,  -- offline / empty / full / normal
    _loaded_at         timestamptz not null default now(),
    primary key (station_id, snapshot_ts)
);
create index if not exists ods_station_status_date_idx on ozbekova_ods.ods_station_status (status_date);

create table if not exists ozbekova_ods.ods_weather (
    weather_date      date primary key,
    temp_max_c        numeric(5, 2),
    temp_min_c        numeric(5, 2),
    precipitation_mm  numeric(6, 2),
    wind_max_kmh      numeric(6, 2),
    is_rainy          boolean      not null,  -- осадки >= 1 мм
    _ds               date         not null,
    _loaded_at        timestamptz  not null default now()
);

create table if not exists ozbekova_ods.ods_trips_daily (
    trip_date         date          not null,
    start_station_id  text          not null,  -- короткий id из архива поездок (= legacy_id в GBFS)
    member_casual     text          not null,  -- member / casual
    rideable_type     text          not null,  -- classic_bike / electric_bike
    trips             int           not null,
    duration_sec_sum  bigint        not null,
    _ds               date          not null,
    _loaded_at        timestamptz   not null default now(),
    primary key (trip_date, start_station_id, member_casual, rideable_type)
);

-- Текущее состояние таблицы "базы сайта" ozbekova.maintenance_tickets.
-- Пишет JDBC sink-коннектор sink-ozbekova (upsert по ticket_id, удаление по tombstone).
create table if not exists ozbekova_ods.ods_maintenance_tickets (
    ticket_id    int primary key,
    station_id   text      not null,
    status       text      not null,  -- open / in_progress / closed
    priority     text      not null,  -- low / medium / high
    description  text,
    opened_at    timestamp not null,
    updated_at   timestamp not null
);

-- ---------------------------------------------------------------- datamart

create table if not exists ozbekova_datamart.dm_daily_station_activity (
    activity_date         date primary key,
    stations_reported     int           not null,
    snapshots_cnt         bigint        not null,
    empty_snapshots       bigint        not null,
    full_snapshots        bigint        not null,
    offline_snapshots     bigint        not null,
    empty_share           numeric(6, 4),
    avg_bikes_available   numeric(8, 2),
    avg_ebikes_available  numeric(8, 2),
    temp_max_c            numeric(5, 2),
    temp_min_c            numeric(5, 2),
    precipitation_mm      numeric(6, 2),
    is_rainy              boolean,
    _updated_at           timestamptz   not null default now()
);

create table if not exists ozbekova_datamart.dm_station_size_monthly (
    month_start                date   not null,
    capacity_category          text   not null,
    stations_cnt               int    not null,
    snapshots_cnt              bigint not null,
    empty_snapshots            bigint not null,
    full_snapshots             bigint not null,
    offline_snapshots          bigint not null,
    rainy_day_empty_snapshots  bigint not null,
    empty_share                numeric(6, 4),
    avg_fill_rate              numeric(6, 4),
    _updated_at                timestamptz not null default now(),
    primary key (month_start, capacity_category)
);

create table if not exists ozbekova_datamart.dm_weather_trips_daily (
    trip_date          date          primary key,
    iso_weekday        int           not null,  -- 1 = понедельник … 7 = воскресенье
    is_weekend         boolean       not null,
    trips_cnt          int           not null,
    member_trips       int           not null,
    casual_trips       int           not null,
    ebike_trips        int           not null,
    avg_duration_min   numeric(6, 1),
    temp_max_c         numeric(5, 2),
    temp_min_c         numeric(5, 2),
    precipitation_mm   numeric(6, 2),
    is_rainy           boolean,
    rain_level         text,          -- dry < 1 мм, rain 1–10 мм, heavy_rain ≥ 10 мм
    _updated_at        timestamptz   not null default now()
);

create table if not exists ozbekova_datamart.dm_hourly_station_state (
    activity_date        date          not null,
    hour_of_day          int           not null,  -- час снимка по Нью-Йорку, 0–23
    capacity_category    text          not null,
    snapshots_cnt        bigint        not null,
    working_snapshots    bigint        not null,  -- station_state <> 'offline'
    empty_snapshots      bigint        not null,
    full_snapshots       bigint        not null,
    empty_share          numeric(6, 4),
    avg_bikes_available  numeric(8, 2),
    _updated_at          timestamptz   not null default now(),
    primary key (activity_date, hour_of_day, capacity_category)
);

create table if not exists ozbekova_datamart.dm_station_daily (
    activity_date        date          not null,
    station_id           text          not null,
    station_name         text,
    latitude             numeric(9, 6),
    longitude            numeric(9, 6),
    capacity             int,
    capacity_category    text          not null,
    snapshots_cnt        bigint        not null,
    working_snapshots    bigint        not null,
    empty_snapshots      bigint        not null,
    full_snapshots       bigint        not null,
    empty_share          numeric(6, 4),
    avg_bikes_available  numeric(8, 2),
    _updated_at          timestamptz   not null default now(),
    primary key (activity_date, station_id)
);
