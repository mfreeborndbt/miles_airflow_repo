with source as (

    select * 
    from {{ source('raw', 'weather_codes') }}

),

renamed as (

    select
        cast(weather_code as string) as weather_code,
        description
    from source

)

select * from renamed
