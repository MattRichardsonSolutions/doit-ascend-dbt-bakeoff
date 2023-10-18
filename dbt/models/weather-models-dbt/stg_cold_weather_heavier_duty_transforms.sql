SELECT name,
AVG(min_temperature) as avg_temps,
MAX(min_temperature) as corr_temps,
MIN(min_temperature) as covar_temps,
CASE WHEN AVG(min_temperature) > 10 then 'hot for northerners'
WHEN AVG(min_temperature) > 7 then 'might need a jacket'
else 'cold, cold, COLD!!!'
END
as temp_experience,
CASE WHEN AVG(min_temperature) > 10 then 'northerners delight'
WHEN AVG(min_temperature) > 7 then 'indoor weather'
else 'not worth the risk!!!'
END
as holiday_y_n
FROM {{ ref('stg_cold_weather_filtering') }}
GROUP BY name