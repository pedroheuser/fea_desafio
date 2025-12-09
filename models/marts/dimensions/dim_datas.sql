{{ config(materialized='table') }}

with
    date_spine as (
        select explode(sequence(
            to_date('2008-01-01')
            , to_date('2014-12-31')
            , interval 1 day
        )) as date_actual
    )

    , date_attributes as (
        select
            cast(date_format(date_actual, 'yyyyMMdd') as int) as date_id
            , date_actual
            , dayofweek(date_actual) as day_of_week
            , date_format(date_actual, 'EEEE') as day_of_week_name
            , day(date_actual) as day_of_month
            , dayofyear(date_actual) as day_of_year
            , weekofyear(date_actual) as week_of_year
            , month(date_actual) as month_number
            , date_format(date_actual, 'MMM') as month_abbreviated_name
            , date_format(date_actual, 'MMMM') as month_full_name
            , date_format(date_actual, 'MMM/yy') as month_year
            , quarter(date_actual) as quarter_number
            , concat('Q', quarter(date_actual)) as quarter_name
            , year(date_actual) as year_number
            , case
                when dayofweek(date_actual) in (1, 7) then true
                else false
            end as is_weekend
            , false as is_holiday
            , year(date_actual) as fiscal_year
            , quarter(date_actual) as fiscal_quarter
        from date_spine
    )

select * from date_attributes

