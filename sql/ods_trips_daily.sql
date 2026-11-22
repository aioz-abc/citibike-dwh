-- stg_trips_daily -> ods_trips_daily за месяц ds (ds = первый день месяца).
delete from ozbekova_ods.ods_trips_daily where _ds = %(ds)s::date;

insert into ozbekova_ods.ods_trips_daily (
    trip_date, start_station_id, member_casual, rideable_type, trips, duration_sec_sum, _ds
)
select
    trip_date::date,
    coalesce(nullif(trim(start_station_id), ''), 'unknown'),
    lower(trim(member_casual)),
    lower(trim(rideable_type)),
    sum(trips::int),
    sum(duration_sec_sum::bigint),
    _ds
from ozbekova_stg.stg_trips_daily
where _ds = %(ds)s::date
group by 1, 2, 3, 4, _ds;
