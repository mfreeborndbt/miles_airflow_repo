{{ config(materialized='table') }}

with base as (

    select * from {{ ref('int_weather_with_description') }}

),

final as (

    select
        weather_pk,
        weather_date,
        weather_code,
        weather_description,
        avg_temp,
        max_temp,
        min_temp,
        total_precipitation,
        sunshine_hours,
        daylight_hours,
        wind_gusts,
        wind_speed,

        -- Derived metrics
        round(max_temp - min_temp,1) as temp_dif,
        round(wind_gusts - wind_speed,1) as gust_dif,
        round(daylight_hours - sunshine_hours,1) as cloud_hours,
        case 
            when daylight_hours > 0 then round(sunshine_hours / daylight_hours, 1)
            else null
        end as sunshine_pct

    from base

)

select * from final
