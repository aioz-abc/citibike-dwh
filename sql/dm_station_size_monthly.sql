-- Витрина по справочнику. Зерно: месяц × категория размера станции.
-- Пересчитывается весь месяц, в который попадает ds.
delete from ozbekova_datamart.dm_station_size_monthly
where month_start = date_trunc('month', %(ds)s::date)::date;

insert into ozbekova_datamart.dm_station_size_monthly (
    month_start, capacity_category, stations_cnt, snapshots_cnt,
    empty_snapshots, full_snapshots, offline_snapshots, rainy_day_empty_snapshots,
    empty_share, avg_fill_rate
)
select
    date_trunc('month', s.status_date)::date,
    coalesce(st.capacity_category, 'unknown'),
    count(distinct s.station_id),
    count(*),
    count(*) filter (where s.station_state = 'empty'),
    count(*) filter (where s.station_state = 'full'),
    count(*) filter (where s.station_state = 'offline'),
    count(*) filter (where s.station_state = 'empty' and w.is_rainy),
    round(
        (count(*) filter (where s.station_state = 'empty'))::numeric
        / nullif(count(*) filter (where s.station_state <> 'offline'), 0), 4),
    round(avg(
        s.bikes_available::numeric / nullif(s.bikes_available + s.docks_available, 0)
    ) filter (where s.station_state <> 'offline'), 4)
from ozbekova_ods.ods_station_status s
left join ozbekova_ods.ods_stations st on st.station_id = s.station_id
left join ozbekova_ods.ods_weather w on w.weather_date = s.status_date
where s.status_date >= date_trunc('month', %(ds)s::date)
  and s.status_date <  date_trunc('month', %(ds)s::date) + interval '1 month'
group by 1, 2;
