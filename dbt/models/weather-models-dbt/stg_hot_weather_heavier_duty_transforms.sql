SELECT name,
AVG(min_temperature) as avg_temps,
MAX(min_temperature) as corr_temps,
MIN(min_temperature) as covar_temps,
CASE WHEN AVG(min_temperature) > 25 then 'very hot'
WHEN AVG(min_temperature) > 22 then 'slightly hot'
else 'not that hot'
END
as temp_experience,
CASE WHEN AVG(min_temperature) > 25 then 'dont visit - its too hot!!'
WHEN AVG(min_temperature) > 22 then 'perfect holiday'
else 'not hot enough!'
END
as holiday_y_n
FROM {{ ref('stg_hot_weather_filtering') }}
GROUP BY name