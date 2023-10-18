SELECT * FROM {{weather_ALL_FILES_copy_transform_copy}} 
where min_temperature < 20 and min_temperature >= 12