-- Погода из ods (ephemeral).
select
    weather_date,
    temp_max_c,
    temp_min_c,
    precipitation_mm,
    is_rainy
from {{ source('ods', 'ods_weather') }}
