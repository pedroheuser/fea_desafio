with source as (

    select * from {{ source('adventure_works', 'production_productsubcategory') }}

)

, renamed as (

    select
        cast(productsubcategoryid as int) as product_subcategory_id
        , cast(productcategoryid as int) as product_category_id
        , name as product_subcategory_name
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

