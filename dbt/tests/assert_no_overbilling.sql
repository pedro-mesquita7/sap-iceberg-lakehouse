-- No order item may be invoiced for more than was ordered.
with billed as (
    select order_id, order_item_number, sum(billed_quantity) as billed_quantity
    from {{ ref('fct_billing_items') }}
    where not is_credit_memo
    group by 1, 2
)

select billed.*, items.order_quantity
from billed
inner join {{ ref('fct_sales_order_items') }} as items
    on billed.order_id = items.order_id
    and billed.order_item_number = items.item_number
where billed.billed_quantity > items.order_quantity
