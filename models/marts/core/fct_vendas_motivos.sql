{{ config(materialized='table') }}

with
    stg_order_header as (
        select * from {{ ref('stg_sales__salesorderheader') }}
    )

    , stg_order_reason as (
        select * from {{ ref('stg_sales__salesorderheadersalesreason') }}
    )

    , dim_motivo_venda as (
        select * from {{ ref('dim_motivo_venda') }}
    )

    , order_reasons as (
        select
            orr.sales_order_id
            , orr.sales_reason_id
        from stg_order_reason orr
        inner join stg_order_header oh
            on orr.sales_order_id = oh.sales_order_id
        inner join dim_motivo_venda dmv
            on orr.sales_reason_id = dmv.sales_reason_id
    )

select * from order_reasons

