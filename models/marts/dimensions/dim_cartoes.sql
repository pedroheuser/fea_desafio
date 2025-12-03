{{ config(materialized='table') }}

with
    stg_creditcard as (
        select * from {{ ref('stg_sales__creditcard') }}
    )

    , creditcard_masked as (
        select
            creditcard_id
            , card_type
            , case
                when card_number is not null
                    then concat('****-****-****-', right(card_number, 4))
                else null
            end as card_number_masked
            , exp_month
            , exp_year
        from stg_creditcard
    )

select * from creditcard_masked

