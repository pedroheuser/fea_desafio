{{ config(materialized='table') }}

select * from {{ ref('stg_purchasing__shipmethod') }}

