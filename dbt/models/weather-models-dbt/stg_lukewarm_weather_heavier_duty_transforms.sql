SELECT name,
AVG(min_temperature) as avg_temps,
MAX(min_temperature) as corr_temps,
MIN(min_temperature) as covar_temps,
CASE WHEN AVG(min_temperature) > 18 then 'passable'
WHEN AVG(min_temperature) > 15 then 'copeable'
else 'southerners wont enjoy this'
END
as temp_experience,
CASE WHEN AVG(min_temperature) > 18 then 'could visit for a weekend'
WHEN AVG(min_temperature) > 15 then 'could spend a day'
else 'northerners only!'
END
as holiday_y_n
FROM {{ ref('stg_lukewarm_weather_filtering') }}
GROUP BY name