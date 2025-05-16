with weather as (

    select * from {{ ref('stg_austin_weather') }}

),

codes as (

    select * from {{ ref('stg_weather_codes') }}

),

joined as (

    select
        w.*,
        c.description as weather_description
    from weather w
    left join codes c
        on w.weather_code = c.weather_code

)

select * from joined
