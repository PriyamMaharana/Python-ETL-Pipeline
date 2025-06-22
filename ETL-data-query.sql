-- creating database --
CREATE DATABASE weather_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- basic query --
-- location --
select * from location;

-- current weather--
select * from current_weather;

select country , count(region) from location
group by 1;

select distinct(region) as city from location where country in ('India');

select loc.region, loc.country, cw.condition_text as weather, cw.temp_c as temperature
from location loc join current_weather cw on loc.location_id = cw.location_id;