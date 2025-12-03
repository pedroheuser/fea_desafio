with source as (

    select * from {{ source('adventure_works', 'person_address') }}

)

, renamed as (

    select
        cast(addressid as int) as address_id
        , addressline1 as address_line1
        , addressline2 as address_line2
        , city as city
        , cast(stateprovinceid as int) as state_province_id
        , postalcode as postal_code
        , spatiallocation as spatial_location
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

