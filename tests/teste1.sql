-- Este teste garante a veracidade dos dados conforme solicitado pelo CEO Carlos Silveira.
-- Vendas Brutas em 2011 devem ser 12.646.112,16.
-- O teste falha se retornar linhas (ou seja, se a soma for diferente do esperado).

with vendas_2011 as (
    select
        round(sum(fv.order_qty * fv.unit_price), 2) as total_vendas
    from {{ ref('fct_vendas') }} fv
    inner join {{ ref('dim_datas') }} dd
        on fv.order_date_id = dd.date_id
    where dd.year_number = 2011
)

select *
from vendas_2011
where round(total_vendas, 2) != 12646112.16