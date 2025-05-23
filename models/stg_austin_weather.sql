{{ config(
    materialized = 'incremental',
    unique_key = 'weather_pk',
    incremental_strategy = 'merge'
) }}

with source as (

    select * 
    from {{ source('raw', 'austin_weather') }}
    {% if is_incremental() %}
    where date < (
        select coalesce(min(weather_date), '9999-12-31') from {{ this }}
    )
    {% endif %}

),

renamed as (

    {{
        standardize_columns(
            {
                "date": {"type": "whole_date", "alias": "weather_date"},
                "weather_code": "default",
                "temperature_2m_mean": {"type": "precise", "alias": "avg_temp"},
                "temperature_2m_max": {"type": "precise", "alias": "max_temp"},
                "temperature_2m_min": {"type": "precise", "alias": "min_temp"},
                "precipitation_sum": {"type": "precise", "alias": "total_precipitation"},
                "sunshine_duration": {"type": "hours", "alias": "sunshine_hours"},
                "daylight_duration": {"type": "hours", "alias": "daylight_hours"},
                "wind_gusts_10m_max": {"type": "precise", "alias": "wind_gusts"},
                "wind_speed_10m_max": {"type": "precise", "alias": "wind_speed"}
            },
            source_relation = "source"
        )
    }}

),

with_pk as (

    select
        md5(cast(weather_date as string)) as weather_pk,
        *
    from renamed

)

select * from with_pk
