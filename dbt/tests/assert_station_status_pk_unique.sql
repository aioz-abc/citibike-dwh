-- Ключ ods_station_status (station_id, snapshot_ts) уникален. Тест красный, если вернул строки.
select station_id, snapshot_ts, count(*) as cnt
from {{ source('ods', 'ods_station_status') }}
group by station_id, snapshot_ts
having count(*) > 1
