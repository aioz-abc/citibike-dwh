-- Витрина по станциям для карты. Зерно: день × станция (с координатами из справочника).
-- Отвечает: где в городе станции чаще остаются без велосипедов.
delete from ozbekova_datamart.dm_station_daily where activity_date = %(ds)s;

insert into ozbekova_datamart.dm_station_daily (
    activity_date, station_id, station_name, latitude, longitude, capacity, capacity_category,
    snapshots_cnt, working_snapshots, empty_snapshots, full_snapshots, empty_share, avg_bikes_available
)
select
    s.status_date,
    s.station_id,
    st.station_name,
    st.latitude,
    st.longitude,
    st.capacity,
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
group by s.status_date, s.station_id, st.station_name, st.latitude, st.longitude,
         st.capacity, st.capacity_category;
