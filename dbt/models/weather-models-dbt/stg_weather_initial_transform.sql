select *,
value/10 AS min_temperature from {{ ref('stg_weather_data') }}