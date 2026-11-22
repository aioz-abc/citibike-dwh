-- Витрина по времени. Зерно: один день (activity_date). Пересчитывается день ds.
delete from ozbekova_datamart.dm_daily_station_activity
where activity_date = %(ds)s::date;

insert into ozbekova_datamart.dm_daily_station_activity (
    activity_date, stations_reported, snapshots_cnt,
    empty_snapshots, full_snapshots, offline_snapshots, empty_share,
    avg_bikes_available, avg_ebikes_available,
    temp_max_c, temp_min_c, precipitation_mm, is_rainy
)
select
    s.status_date,
    count(distinct s.station_id),
    count(*),
    count(*) filter (where s.station_state = 'empty'),
    count(*) filter (where s.station_state = 'full'),
    count(*) filter (where s.station_state = 'offline'),
    round(
        (count(*) filter (where s.station_state = 'empty'))::numeric
        / nullif(count(*) filter (where s.station_state <> 'offline'), 0), 4),
    round(avg(s.bikes_available) filter (where s.station_state <> 'offline'), 2),
    round(avg(s.ebikes_available) filter (where s.station_state <> 'offline'), 2),
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.is_rainy
from ozbekova_ods.ods_station_status s
left join ozbekova_ods.ods_weather w on w.weather_date = s.status_date
where s.status_date = %(ds)s::date
group by s.status_date, w.temp_max_c, w.temp_min_c, w.precipitation_mm, w.is_rainy;
