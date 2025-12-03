{{ config(materialized='table') }}

select * from {{ ref('stg_sales__specialoffer') }}

