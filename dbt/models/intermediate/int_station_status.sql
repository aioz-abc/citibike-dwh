-- Снимки станций из ods (ephemeral: отдельного объекта в базе не создаёт).
select
    station_id,
    snapshot_ts,
    status_date,
    bikes_available,
    ebikes_available,
    docks_available,
    station_state
from {{ source('ods', 'ods_station_status') }}
