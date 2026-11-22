-- Витрина по часам. Зерно: день × час (Нью-Йорк) × категория размера станции.
-- Отвечает: в какие часы и какие станции чаще остаются без велосипедов.
delete from ozbekova_datamart.dm_hourly_station_state where activity_date = %(ds)s;

insert into ozbekova_datamart.dm_hourly_station_state (
    activity_date, hour_of_day, capacity_category, snapshots_cnt, working_snapshots,
    empty_snapshots, full_snapshots, empty_share, avg_bikes_available
)
select
    s.status_date,
    extract(hour from s.snapshot_ts at time zone 'America/New_York')::int,
    coalesce(st.capacity_category, 'unknown'),
    count(*),
    count(*) filter (where s.station_state <> 'offline'),
    count(*) filter (where s.station_state = 'empty'),
    count(*) filter (where s.station_state = 'full'),
    round(
        (count(*) filter (where s.station_state = 'empty'))::numeric
        / nullif(count(*) filter (where s.station_state <> 'offline'), 0), 4),
    round(avg(s.bikes_available) filter (where s.station_state <> 'offline'), 2)
from ozbekova_ods.ods_station_status s
left join ozbekova_ods.ods_stations st on st.station_id = s.station_id
where s.status_date = %(ds)s
group by 1, 2, 3;
