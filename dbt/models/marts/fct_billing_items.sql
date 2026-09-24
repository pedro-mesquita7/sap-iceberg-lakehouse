select
    items.billing_id || '/' || items.item_number as billing_item_key,
    items.billing_id,
    items.item_number,
    headers.billing_type,
    billing_types.billing_type_name,
    billing_types.is_credit_memo,
    headers.billing_date,
    headers.payer_id as customer_id,
    items.material_id,
    items.order_id,
    items.order_item_number,
    orders.sales_org,
    items.reference_document_id,
    cast(items.billed_quantity as decimal(18, 3)) as billed_quantity,
    cast(items.net_value as decimal(18, 3)) as net_value,
    items.currency_code,
    cast(items.net_value * fx.eur_per_unit as decimal(18, 2)) as net_value_eur
from {{ ref('stg_sap__billing_items') }} as items
inner join {{ ref('stg_sap__billing_documents') }} as headers
    on items.billing_id = headers.billing_id
left join {{ ref('stg_sap__sales_orders') }} as orders
    on items.order_id = orders.order_id
left join {{ ref('sap_billing_types') }} as billing_types
    on headers.billing_type = billing_types.billing_type
left join {{ ref('fx_rates_to_eur') }} as fx
    on items.currency_code = fx.currency_code
where not headers.is_cancelled
