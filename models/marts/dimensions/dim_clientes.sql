{{ config(materialized='table') }}

with
    stg_customer as (
        select * from {{ ref('stg_sales__customer') }}
    )

    , stg_person as (
        select * from {{ ref('stg_person__person') }}
    )

    , stg_store as (
        select * from {{ ref('stg_sales__store') }}
    )

    , stg_territory as (
        select * from {{ ref('stg_sales__salesterritory') }}
    )

    , customer_base as (
        select
            c.customer_id
            , c.person_id
            , c.store_id
            , c.territory_id
            , case
                when c.person_id is not null then 'Pessoa'
                when c.store_id is not null then 'Loja'
                else 'Desconhecido'
            end as customer_type
            , coalesce(t.territory_name, 'Sem Território') as territory_name
            , coalesce(t.country_region_code, 'N/A') as country_region_code
            , coalesce(t.territory_group, 'N/A') as territory_group
        from stg_customer c
        left join stg_territory t
            on c.territory_id = t.territory_id
    )

    , customer_with_person as (
        select
            cb.customer_id
            , cb.customer_type
            , cb.person_id
            , cb.store_id
            , cb.territory_id
            , cb.territory_name
            , cb.country_region_code
            , cb.territory_group
            , case
                when cb.customer_type = 'Pessoa' then
                    concat(
                        coalesce(p.first_name, '')
                        , case when p.first_name is not null and p.last_name is not null then ' ' else '' end
                        , coalesce(p.last_name, 'Cliente Desconhecido')
                    )
                when cb.customer_type = 'Loja' then s.store_name
                else 'Cliente Desconhecido'
            end as customer_name
            , p.first_name
            , p.middle_name
            , p.last_name
            , p.title
            , p.suffix
            , p.email_promotion
            , s.store_name
            , s.salesperson_id as store_salesperson_id
        from customer_base cb
        left join stg_person p
            on cb.person_id = p.person_id
            and cb.customer_type = 'Pessoa'
        left join stg_store s
            on cb.store_id = s.store_id
            and cb.customer_type = 'Loja'
    )

select * from customer_with_person

