-- stg_stations -> ods_stations. Справочник: перезаписывается целиком.
delete from ozbekova_ods.ods_stations;

insert into ozbekova_ods.ods_stations (
    station_id, station_name, latitude, longitude, capacity, capacity_category,
    is_renting, is_returning, _ds
)
select distinct on (station_id)
    station_id,
    trim(name),
    nullif(lat, '')::numeric(9, 6),
    nullif(lon, '')::numeric(9, 6),
    nullif(capacity, '')::int,
    case
        when coalesce(nullif(capacity, '')::int, 0) = 0 then 'unknown'
        when capacity::int < 20 then 'small'
        when capacity::int < 40 then 'medium'
        else 'large'
    end,
    is_renting = '1',
    is_returning = '1',
    _ds
from ozbekova_stg.stg_stations
where station_id is not null and station_id <> ''
order by station_id, _loaded_at desc;
