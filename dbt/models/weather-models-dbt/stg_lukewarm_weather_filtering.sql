SELECT * FROM {{ ref('stg_weather_initial_transform') }}
where min_temperature < 20 and min_temperature >= 12