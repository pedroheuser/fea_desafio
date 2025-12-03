with source as (

    select * from {{ source('adventure_works', 'person_countryregion') }}

)

, renamed as (

    select
        countryregioncode as country_region_code
        , name as country_region_name
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed
where country_region_code is not null

