SELECT * FROM {{ ref('stg_weather_initial_transform') }}
where min_temperature > 20 