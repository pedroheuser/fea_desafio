with source as (

    select * from {{ source('adventure_works', 'sales_salesperson') }}

)

, renamed as (

    select
        cast(businessentityid as int) as salesperson_id
        , cast(territoryid as int) as territory_id
        , cast(salesquota as decimal(19, 4)) as sales_quota
        , cast(bonus as decimal(19, 4)) as bonus
        , cast(commissionpct as decimal(5, 4)) as commission_pct
        , cast(salesytd as decimal(19, 4)) as sales_ytd
        , cast(saleslastyear as decimal(19, 4)) as sales_last_year
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

