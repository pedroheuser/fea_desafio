{{ config(materialized='table') }}

with
    stg_order_detail as (
        select * from {{ ref('stg_sales__salesorderdetail') }}
    )

    , stg_order_header as (
        select * from {{ ref('stg_sales__salesorderheader') }}
    )

    , dim_produtos as (
        select * from {{ ref('dim_produtos') }}
    )

    , dim_clientes as (
        select * from {{ ref('dim_clientes') }}
    )

    , dim_vendedores as (
        select * from {{ ref('dim_vendedores') }}
    )

    , dim_territorios as (
        select * from {{ ref('dim_territorios') }}
    )

    , dim_cartoes as (
        select * from {{ ref('dim_cartoes') }}
    )

    , dim_metodo_envio as (
        select * from {{ ref('dim_metodo_envio') }}
    )

    , dim_locais as (
        select * from {{ ref('dim_locais') }}
    )

    , dim_ofertas as (
        select * from {{ ref('dim_ofertas') }}
    )

    , dim_datas as (
        select * from {{ ref('dim_datas') }}
    )

    , order_detail_with_header as (
        select
            od.sales_order_detail_id
            , od.sales_order_id
            , od.product_id
            , od.order_qty
            , od.unit_price
            , od.unit_price_discount
            , od.special_offer_id
            , od.carrier_tracking_number
            , oh.customer_id
            , oh.salesperson_id
            , oh.territory_id
            , oh.creditcard_id
            , oh.ship_method_id
            , oh.bill_to_address_id
            , oh.ship_to_address_id
            , oh.order_date
            , oh.due_date
            , oh.ship_date
            , oh.order_subtotal
            , oh.order_tax_amt
            , oh.order_freight
            , oh.order_total_due
            , oh.revision_number
            , oh.online_order_flag
            , oh.purchase_order_number
            , oh.account_number
            , oh.order_status
        from stg_order_detail od
        inner join stg_order_header oh
            on od.sales_order_id = oh.sales_order_id
    )

    , fact_with_calculations as (
        select
            odh.sales_order_detail_id
            , odh.sales_order_id
            , odh.product_id
            , odh.customer_id
            , odh.salesperson_id
            , odh.territory_id
            , odh.creditcard_id
            , odh.ship_method_id
            , odh.bill_to_address_id
            , odh.ship_to_address_id
            , cast(date_format(odh.order_date, 'yyyyMMdd') as int) as order_date_id
            , cast(date_format(odh.due_date, 'yyyyMMdd') as int) as due_date_id
            , case
                when odh.ship_date is not null
                    then cast(date_format(odh.ship_date, 'yyyyMMdd') as int)
                else null
            end as ship_date_id
            , odh.order_qty
            , odh.unit_price
            , odh.unit_price_discount
            , odh.order_qty * odh.unit_price * (1 - odh.unit_price_discount) as line_total
            , odh.special_offer_id
            , odh.carrier_tracking_number
            , odh.order_subtotal
            , odh.order_tax_amt
            , odh.order_freight
            , odh.order_total_due
            , odh.revision_number
            , odh.online_order_flag
            , case when odh.online_order_flag = true then 'Online' else 'Loja' end as online_order_flag_formatted
            , odh.purchase_order_number
            , odh.account_number
            , odh.order_status
        from order_detail_with_header odh
    )

    , fact_with_dimensions as (
        select
            fwc.sales_order_detail_id
            , fwc.sales_order_id
            , fwc.product_id
            , fwc.customer_id
            , fwc.salesperson_id
            , fwc.territory_id
            , fwc.creditcard_id
            , fwc.ship_method_id
            , fwc.bill_to_address_id
            , fwc.ship_to_address_id
            , fwc.order_date_id
            , fwc.due_date_id
            , fwc.ship_date_id
            , fwc.order_qty
            , fwc.unit_price
            , fwc.unit_price_discount
            , fwc.line_total
            , fwc.special_offer_id
            , fwc.carrier_tracking_number
            , fwc.order_subtotal
            , fwc.order_tax_amt
            , fwc.order_freight
            , fwc.order_total_due
            , fwc.revision_number
            , fwc.online_order_flag
            , fwc.online_order_flag_formatted
            , fwc.purchase_order_number
            , fwc.account_number
            , fwc.order_status
        from fact_with_calculations fwc
        inner join dim_produtos dp
            on fwc.product_id = dp.product_id
        inner join dim_clientes dc
            on fwc.customer_id = dc.customer_id
        inner join dim_territorios dt
            on fwc.territory_id = dt.territory_id
        inner join dim_metodo_envio dme
            on fwc.ship_method_id = dme.ship_method_id
        inner join dim_locais dl_bill
            on fwc.bill_to_address_id = dl_bill.address_id
        inner join dim_locais dl_ship
            on fwc.ship_to_address_id = dl_ship.address_id
        inner join dim_datas dd_order
            on fwc.order_date_id = dd_order.date_id
        inner join dim_datas dd_due
            on fwc.due_date_id = dd_due.date_id
        left join dim_vendedores dv
            on fwc.salesperson_id = dv.salesperson_id
        left join dim_cartoes dcc
            on fwc.creditcard_id = dcc.creditcard_id
        left join dim_datas dd_ship
            on fwc.ship_date_id = dd_ship.date_id
        left join dim_ofertas dof
            on fwc.special_offer_id = dof.special_offer_id
    )

select * from fact_with_dimensions

