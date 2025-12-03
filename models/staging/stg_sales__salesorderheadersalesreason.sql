with source as (

    select * from {{ source('adventure_works', 'sales_salesorderheadersalesreason') }}

)

, renamed as (

    select
        cast(salesorderid as int) as sales_order_id
        , cast(salesreasonid as int) as sales_reason_id
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

