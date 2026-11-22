-- dbt-модель совпадает с витриной из DAG (в обе стороны).
with dag as (
    select activity_date, stations_reported, snapshots_cnt, empty_snapshots, full_snapshots,
           offline_snapshots, empty_share, avg_bikes_available, avg_ebikes_available,
           temp_max_c, temp_min_c, precipitation_mm, is_rainy
    from ozbekova_datamart.dm_daily_station_activity
),
dbt as (
    select * from {{ ref('dm_daily_station_activity_dbt') }}
)
(select 'only_in_dag' as side, * from (select * from dag except select * from dbt) a)
union all
(select 'only_in_dbt' as side, * from (select * from dbt except select * from dag) b)
