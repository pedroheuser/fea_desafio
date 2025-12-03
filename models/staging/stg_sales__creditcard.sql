with source as (

    select * from {{ source('adventure_works', 'sales_creditcard') }}

)

, renamed as (

    select
        cast(creditcardid as int) as creditcard_id
        , cardtype as card_type
        , cardnumber as card_number
        , cast(expmonth as int) as exp_month
        , cast(expyear as int) as exp_year
        , cast(modifieddate as date) as modified_date
    from source

)

select * from renamed

