with source as (

    select * from {{ source('adventure_works', 'purchasing_shipmethod') }}

)

, renamed as (

    select
        cast(shipmethodid as int) as ship_method_id
        , name as ship_method_name
        , cast(shipbase as decimal(19, 4)) as ship_base
        , cast(shiprate as decimal(19, 4)) as ship_rate
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

