{{ config(materialized='table') }}

with
    stg_address as (
        select * from {{ ref('stg_person__address') }}
    )

    , stg_stateprovince as (
        select * from {{ ref('stg_person__stateprovince') }}
    )

    , stg_country as (
        select * from {{ ref('stg_person__countryregion') }}
    )

    , address_with_location as (
        select
            a.address_id
            , a.address_line1
            , a.address_line2
            , a.city
            , a.state_province_id
            , coalesce(sp.state_province_code, 'N/A') as state_province_code
            , coalesce(sp.state_province_name, 'Desconhecido') as state_province_name
            , coalesce(sp.country_region_code, 'N/A') as country_region_code
            , coalesce(c.country_region_name, 'Desconhecido') as country_region_name
            , a.postal_code
        from stg_address a
        left join stg_stateprovince sp
            on a.state_province_id = sp.state_province_id
        left join stg_country c
            on sp.country_region_code = c.country_region_code
    )

select * from address_with_location

