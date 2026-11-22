-- Зерно dm_rain_impact_dbt: одна строка на (day_type, rain_level).
select day_type, rain_level, count(*)
from {{ ref('dm_rain_impact_dbt') }}
group by day_type, rain_level
having count(*) > 1
