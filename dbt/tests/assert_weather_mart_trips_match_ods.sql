-- Сумма поездок в витрине DAG истории равна сумме в ods_trips_daily (ничего не потеряно при свёртке).
select *
from (select sum(trips_cnt) as mart_trips from {{ source('datamart', 'dm_weather_trips_daily') }}) a
cross join (select sum(trips) as ods_trips from {{ source('ods', 'ods_trips_daily') }}) b
where a.mart_trips <> b.ods_trips
