-- Копия витрины dm_daily_station_activity в ClickHouse.
-- Задача copy_to_clickhouse создаёт базу и таблицу сама (if not exists),
-- этот файл — для ручного запуска и для справки.
create database if not exists ozbekova;

create table if not exists ozbekova.dm_daily_station_activity (
    activity_date         Date,
    stations_reported     UInt32,
    snapshots_cnt         UInt64,
    empty_snapshots       UInt64,
    full_snapshots        UInt64,
    offline_snapshots     UInt64,
    empty_share           Nullable(Decimal(6, 4)),
    avg_bikes_available   Nullable(Decimal(8, 2)),
    avg_ebikes_available  Nullable(Decimal(8, 2)),
    temp_max_c            Nullable(Decimal(5, 2)),
    temp_min_c            Nullable(Decimal(5, 2)),
    precipitation_mm      Nullable(Decimal(6, 2)),
    is_rainy              Nullable(UInt8),
    _updated_at           DateTime
)
engine = ReplacingMergeTree(_updated_at)
order by activity_date;
