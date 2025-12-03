{{ config(materialized='table') }}

with
    stg_salesperson as (
        select * from {{ ref('stg_sales__salesperson') }}
    )

    , stg_person as (
        select * from {{ ref('stg_person__person') }}
    )

    , stg_territory as (
        select * from {{ ref('stg_sales__salesterritory') }}
    )

    , salesperson_with_details as (
        select
            sp.salesperson_id
            , concat(
                coalesce(p.first_name, '')
                , case when p.first_name is not null and p.last_name is not null then ' ' else '' end
                , coalesce(p.last_name, 'Vendedor Desconhecido')
            ) as salesperson_name
            , sp.territory_id
            , coalesce(t.territory_name, 'Sem Território') as territory_name
            , sp.sales_quota
            , sp.bonus
            , sp.commission_pct
            , sp.sales_ytd
            , sp.sales_last_year
        from stg_salesperson sp
        inner join stg_person p
            on sp.salesperson_id = p.person_id
        left join stg_territory t
            on sp.territory_id = t.territory_id
    )

select * from salesperson_with_details

