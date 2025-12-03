with source as (

    select * from {{ source('adventure_works', 'production_productmodel') }}

)

, renamed as (

    select
        cast(productmodelid as int) as product_model_id
        , name as product_model_name
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

