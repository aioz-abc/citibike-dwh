-- stg_station_status -> ods_station_status за дату ds.
delete from ozbekova_ods.ods_station_status
where status_date = %(ds)s::date;

insert into ozbekova_ods.ods_station_status (
    station_id, snapshot_ts, status_date, legacy_id,
    bikes_available, ebikes_available, bikes_disabled, docks_available, docks_disabled,
    is_installed, is_renting, is_returning, last_reported_at, station_state
)
select distinct on (station_id, snapshot_ts)
    station_id,
    snapshot_ts,
    status_date,
    legacy_id,
    bikes_available, ebikes_available, bikes_disabled, docks_available, docks_disabled,
    is_installed, is_renting, is_returning,
    -- в фиде встречаются заглушки вроде 86400 (1970 год) — считаем их пустыми
    case when last_reported_epoch >= 1000000000 then to_timestamp(last_reported_epoch) end,
    case
        when not is_installed or not is_renting then 'offline'
        when bikes_available = 0 then 'empty'
        when docks_available = 0 then 'full'
        else 'normal'
    end
from (
    select
        station_id,
        snapshot_ts::timestamptz        as snapshot_ts,
        _ds                             as status_date,
        nullif(legacy_id, '')           as legacy_id,
        num_bikes_available::int        as bikes_available,
        num_ebikes_available::int       as ebikes_available,
        num_bikes_disabled::int         as bikes_disabled,
        num_docks_available::int        as docks_available,
        num_docks_disabled::int         as docks_disabled,
        is_installed = '1'              as is_installed,
        is_renting = '1'                as is_renting,
        is_returning = '1'              as is_returning,
        nullif(last_reported, '')::bigint as last_reported_epoch,
        _loaded_at
    from ozbekova_stg.stg_station_status
    where _ds = %(ds)s::date
      and station_id <> ''
) s
order by station_id, snapshot_ts, _loaded_at desc;
