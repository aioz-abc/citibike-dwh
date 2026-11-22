-- dbt-версия витрины dm_daily_station_activity (та же логика, что sql/dm_daily_station_activity.sql).
-- Зерно: один день.
select
    s.status_date                                                       as activity_date,
    count(distinct s.station_id)::int                                   as stations_reported,
    count(*)                                                            as snapshots_cnt,
    count(*) filter (where s.station_state = 'empty')                   as empty_snapshots,
    count(*) filter (where s.station_state = 'full')                    as full_snapshots,
    count(*) filter (where s.station_state = 'offline')                 as offline_snapshots,
    round(
        (count(*) filter (where s.station_state = 'empty'))::numeric
        / nullif(count(*) filter (where s.station_state <> 'offline'), 0), 4)  as empty_share,
    round(avg(s.bikes_available) filter (where s.station_state <> 'offline'), 2)  as avg_bikes_available,
    round(avg(s.ebikes_available) filter (where s.station_state <> 'offline'), 2) as avg_ebikes_available,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.is_rainy
from {{ ref('int_station_status') }} s
left join {{ ref('int_weather') }} w on w.weather_date = s.status_date
group by s.status_date, w.temp_max_c, w.temp_min_c, w.precipitation_mm, w.is_rainy
