with source as (

    select * from {{ source('adventure_works', 'person_stateprovince') }}

)

, renamed as (

    select
        cast(stateprovinceid as int) as state_province_id
        , stateprovincecode as state_province_code
        , countryregioncode as country_region_code
        , cast(isonlystateprovinceflag as boolean) as is_only_state_province_flag
        , name as state_province_name
        , cast(territoryid as int) as territory_id
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

