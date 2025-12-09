with source as (

    select * from {{ source('adventure_works', 'sales_salesorderheader') }}

)

, renamed as (

    select
        cast(salesorderid as int) as sales_order_id
        , cast(revisionnumber as int) as revision_number
        , cast(orderdate as date) as order_date
        , cast(duedate as date) as due_date
        , cast(shipdate as date) as ship_date
        , cast(status as string) as order_status
        , cast(onlineorderflag as boolean) as online_order_flag
        , purchaseordernumber as purchase_order_number
        , accountnumber as account_number
        , cast(customerid as int) as customer_id
        , cast(salespersonid as int) as salesperson_id
        , cast(territoryid as int) as territory_id
        , cast(billtoaddressid as int) as bill_to_address_id
        , cast(shiptoaddressid as int) as ship_to_address_id
        , cast(shipmethodid as int) as ship_method_id
        , cast(creditcardid as int) as creditcard_id
        , creditcardapprovalcode as creditcard_approval_code
        , cast(currencyrateid as int) as currency_rate_id
        , cast(subtotal as decimal(19, 4)) as order_subtotal
        , cast(taxamt as decimal(19, 4)) as order_tax_amt
        , cast(freight as decimal(19, 4)) as order_freight
        , cast(totaldue as decimal(19, 4)) as order_total_due
        , comment as order_comment
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

