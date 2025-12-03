with source as (

    select * from {{ source('adventure_works', 'sales_specialoffer') }}

)

, renamed as (

    select
        cast(specialofferid as int) as special_offer_id
        , description as offer_description
        , cast(discountpct as decimal(5, 4)) as discount_pct
        , type as offer_type
        , category as offer_category
        , cast(startdate as date) as start_date
        , cast(enddate as date) as end_date
        , cast(minqty as int) as min_qty
        , cast(maxqty as int) as max_qty
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

