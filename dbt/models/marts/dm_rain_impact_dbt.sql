-- Влияние дождя на спрос одним запросом. Зерно: тип дня (будни / выходные) × уровень осадков.
-- change_vs_dry — изменение среднего числа поездок против сухого дня того же типа.
with daily as (
    select
        trip_date,
        sum(trips) as trips,
        sum(trips) filter (where member_casual = 'member') as member_trips,
        sum(trips) filter (where member_casual = 'casual') as casual_trips
    from {{ source('ods', 'ods_trips_daily') }}
    group by trip_date
),

days as (
    select
        d.*,
        case when extract(isodow from d.trip_date) >= 6 then 'weekend' else 'weekday' end as day_type,
        case
            when w.precipitation_mm < 1 then 'dry'
            when w.precipitation_mm < 10 then 'rain'
            else 'heavy_rain'
        end as rain_level
    from daily d
    join {{ source('ods', 'ods_weather') }} w on w.weather_date = d.trip_date
    where w.precipitation_mm is not null
),

agg as (
    select
        day_type,
        rain_level,
        count(*) as days_cnt,
        round(avg(trips)) as avg_trips,
        round(avg(member_trips)) as avg_member_trips,
        round(avg(casual_trips)) as avg_casual_trips
    from days
    group by day_type, rain_level
)

select
    a.*,
    round(a.avg_trips / dry.avg_trips - 1, 3) as change_vs_dry,
    round(a.avg_member_trips / dry.avg_member_trips - 1, 3) as member_change_vs_dry,
    round(a.avg_casual_trips / dry.avg_casual_trips - 1, 3) as casual_change_vs_dry
from agg a
join agg dry on dry.day_type = a.day_type and dry.rain_level = 'dry'
