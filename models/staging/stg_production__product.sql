with source as (

    select * from {{ source('adventure_works', 'production_product') }}

)

, renamed as (

    select
        cast(productid as int) as product_id
        , name as product_name
        , productnumber as product_number
        , cast(makeflag as boolean) as make_flag
        , cast(finishedgoodsflag as boolean) as finished_goods_flag
        , color as product_color
        , cast(safetystocklevel as int) as safety_stock_level
        , cast(reorderpoint as int) as reorder_point
        , cast(standardcost as decimal(19, 4)) as standard_cost
        , cast(listprice as decimal(19, 4)) as list_price
        , size as product_size
        , sizeunitmeasurecode as size_unit_measure_code
        , weightunitmeasurecode as weight_unit_measure_code
        , cast(weight as decimal(8, 2)) as product_weight
        , cast(daystomanufacture as int) as days_to_manufacture
        , productline as product_line
        , class as product_class
        , style as product_style
        , cast(productsubcategoryid as int) as product_subcategory_id
        , cast(productmodelid as int) as product_model_id
        , cast(sellstartdate as date) as sell_start_date
        , cast(sellenddate as date) as sell_end_date
        , try_cast(cast(discontinueddate as string) as date) as discontinued_date
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

