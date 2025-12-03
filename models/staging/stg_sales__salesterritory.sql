with source as (

    select * from {{ source('adventure_works', 'sales_salesterritory') }}

)

, renamed as (

    select
        cast(territoryid as int) as territory_id
        , name as territory_name
        , countryregioncode as country_region_code
        , group as territory_group
        , cast(salesytd as decimal(19, 4)) as sales_ytd
        , cast(saleslastyear as decimal(19, 4)) as sales_last_year
        , cast(costytd as decimal(19, 4)) as cost_ytd
        , cast(costlastyear as decimal(19, 4)) as cost_last_year
        , rowguid as row_guid
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

