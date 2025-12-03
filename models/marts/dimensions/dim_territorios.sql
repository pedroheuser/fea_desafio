{{ config(materialized='table') }}

with
    stg_territory as (
        select * from {{ ref('stg_sales__salesterritory') }}
    )

    , stg_country as (
        select * from {{ ref('stg_person__countryregion') }}
    )

    , territory_with_country as (
        select
            t.territory_id
            , t.territory_name
            , t.country_region_code
            , coalesce(c.country_region_name, 'Desconhecido') as country_region_name
            , t.territory_group
            , t.sales_ytd
            , t.sales_last_year
            , t.cost_ytd
            , t.cost_last_year
        from stg_territory t
        left join stg_country c
            on t.country_region_code = c.country_region_code
    )

select * from territory_with_country

