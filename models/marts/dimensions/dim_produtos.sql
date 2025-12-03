{{ config(materialized='table') }}

with
    stg_product as (
        select * from {{ ref('stg_production__product') }}
    )

    , stg_subcategory as (
        select * from {{ ref('stg_production__productsubcategory') }}
    )

    , stg_category as (
        select * from {{ ref('stg_production__productcategory') }}
    )

    , stg_model as (
        select * from {{ ref('stg_production__productmodel') }}
    )

    , product_with_hierarchy as (
        select
            p.product_id
            , p.product_number
            , p.product_name
            , p.product_color
            , p.product_size
            , p.size_unit_measure_code
            , p.product_weight
            , p.weight_unit_measure_code
            , p.product_line
            , p.product_class
            , p.product_style
            , p.product_subcategory_id
            , coalesce(sub.product_subcategory_name, 'Sem Subcategoria') as product_subcategory_name
            , coalesce(sub.product_category_id, -1) as product_category_id
            , coalesce(cat.product_category_name, 'Sem Categoria') as product_category_name
            , p.product_model_id
            , coalesce(m.product_model_name, 'Sem Modelo') as product_model_name
            , p.standard_cost
            , p.list_price
            , p.safety_stock_level
            , p.reorder_point
            , p.days_to_manufacture
            , p.sell_start_date
            , p.sell_end_date
            , p.discontinued_date
            , p.make_flag
            , p.finished_goods_flag
        from stg_product p
        left join stg_subcategory sub
            on p.product_subcategory_id = sub.product_subcategory_id
        left join stg_category cat
            on sub.product_category_id = cat.product_category_id
        left join stg_model m
            on p.product_model_id = m.product_model_id
    )

select * from product_with_hierarchy

