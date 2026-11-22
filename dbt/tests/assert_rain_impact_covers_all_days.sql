-- dbt-модель и витрина DAG истории видят одни и те же дни.
select *
from (select sum(days_cnt) as dbt_days from {{ ref('dm_rain_impact_dbt') }}) a
cross join (select count(*) as dag_days from {{ source('datamart', 'dm_weather_trips_daily') }}) b
where a.dbt_days <> b.dag_days
