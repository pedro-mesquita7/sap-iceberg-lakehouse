-- Order-to-Cash cycle per order item: order -> delivery -> goods issue -> invoice.
with deliveries as (
    select
        delivery_items.order_id,
        delivery_items.order_item_number,
        min(delivery_items.delivery_id) as delivery_id,
        min(deliveries.created_on) as delivery_created_on,
        min(deliveries.goods_issue_on) as goods_issue_on
    from {{ ref('stg_sap__delivery_items') }} as delivery_items
    inner join {{ ref('stg_sap__deliveries') }} as deliveries
        on delivery_items.delivery_id = deliveries.delivery_id
    group by 1, 2
),

invoices as (
    select
        order_id,
        order_item_number,
        min(billing_date) as invoiced_on
    from {{ ref('fct_billing_items') }}
    where not is_credit_memo
    group by 1, 2
)

select
    items.order_item_key,
    items.order_id,
    items.item_number,
    items.customer_id,
    items.material_id,
    items.sales_org,
    items.order_date,
    deliveries.delivery_id,
    deliveries.delivery_created_on,
    deliveries.goods_issue_on,
    invoices.invoiced_on,
    cast(date_diff('day', items.order_date, deliveries.goods_issue_on) as integer)
        as days_order_to_goods_issue,
    cast(date_diff('day', deliveries.goods_issue_on, invoices.invoiced_on) as integer)
        as days_goods_issue_to_invoice,
    cast(date_diff('day', items.order_date, invoices.invoiced_on) as integer) as days_order_to_invoice,
    case
        when items.is_rejected then 'rejected'
        when invoices.invoiced_on is not null then 'invoiced'
        when deliveries.goods_issue_on is not null then 'shipped'
        when deliveries.delivery_created_on is not null then 'in_delivery'
        else 'open'
    end as fulfillment_status,
    items.net_value_eur
from {{ ref('fct_sales_order_items') }} as items
left join deliveries
    on items.order_id = deliveries.order_id
    and items.item_number = deliveries.order_item_number
left join invoices
    on items.order_id = invoices.order_id
    and items.item_number = invoices.order_item_number
