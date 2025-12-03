with source as (

    select * from {{ source('adventure_works', 'sales_customer') }}

)

, renamed as (

    select
        cast(customerid as int) as customer_id
        , cast(personid as int) as person_id
        , cast(storeid as int) as store_id
        , cast(territoryid as int) as territory_id
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

