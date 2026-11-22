-- stg_weather -> ods_weather за дату ds.
delete from ozbekova_ods.ods_weather
where weather_date = %(ds)s::date;

insert into ozbekova_ods.ods_weather (
    weather_date, temp_max_c, temp_min_c, precipitation_mm, wind_max_kmh, is_rainy, _ds
)
select distinct on (date)
    date::date,
    nullif(temperature_2m_max, '')::numeric(5, 2),
    nullif(temperature_2m_min, '')::numeric(5, 2),
    nullif(precipitation_sum, '')::numeric(6, 2),
    nullif(wind_speed_10m_max, '')::numeric(6, 2),
    coalesce(nullif(precipitation_sum, '')::numeric, 0) >= 1,
    _ds
from ozbekova_stg.stg_weather
where _ds = %(ds)s::date
order by date, _loaded_at desc;
