-- Витрина «поездки и погода». Зерно: день. Пересчитывается месяц ds (ds = первый день месяца).
-- Отвечает: сколько поездок теряет Citi Bike в дождливые дни.
delete from ozbekova_datamart.dm_weather_trips_daily
where trip_date >= %(ds)s::date and trip_date < %(ds)s::date + interval '1 month';

insert into ozbekova_datamart.dm_weather_trips_daily (
    trip_date, iso_weekday, is_weekend, trips_cnt, member_trips, casual_trips, ebike_trips,
    avg_duration_min, temp_max_c, temp_min_c, precipitation_mm, is_rainy, rain_level
)
select
    t.trip_date,
    extract(isodow from t.trip_date)::int,
    extract(isodow from t.trip_date) >= 6,
    sum(t.trips),
    sum(t.trips) filter (where t.member_casual = 'member'),
    sum(t.trips) filter (where t.member_casual = 'casual'),
    sum(t.trips) filter (where t.rideable_type = 'electric_bike'),
    round(sum(t.duration_sec_sum)::numeric / nullif(sum(t.trips), 0) / 60, 1),
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.is_rainy,
    case
        when w.precipitation_mm is null then null
        when w.precipitation_mm < 1 then 'dry'
        when w.precipitation_mm < 10 then 'rain'
        else 'heavy_rain'
    end
from ozbekova_ods.ods_trips_daily t
left join ozbekova_ods.ods_weather w on w.weather_date = t.trip_date
where t.trip_date >= %(ds)s::date and t.trip_date < %(ds)s::date + interval '1 month'
group by t.trip_date, w.temp_max_c, w.temp_min_c, w.precipitation_mm, w.is_rainy;
