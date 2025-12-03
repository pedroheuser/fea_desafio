with source as (

    select * from {{ source('adventure_works', 'sales_store') }}

)

, renamed as (

    select
        cast(businessentityid as int) as store_id
        , name as store_name
        , cast(salespersonid as int) as salesperson_id
        , demographics as store_demographics
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

