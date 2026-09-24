select
    items.order_id || '/' || items.item_number as order_item_key,
    items.order_id,
    items.item_number,
    orders.created_on as order_date,
    orders.customer_id,
    items.material_id,
    order_types.order_type_code as order_type,
    orders.sales_org,
    cast(items.order_quantity as decimal(18, 3)) as order_quantity,
    units.unit_code as sales_unit,
    cast(items.net_value as decimal(18, 3)) as net_value,
    items.currency_code,
    cast(items.net_value * fx.eur_per_unit as decimal(18, 2)) as net_value_eur,
    items.is_rejected,
    reasons.rejection_reason,
    orders.overall_status
from {{ ref('stg_sap__sales_order_items') }} as items
inner join {{ ref('stg_sap__sales_orders') }} as orders
    on items.order_id = orders.order_id
left join {{ ref('sap_order_types') }} as order_types
    on orders.order_type_sap = order_types.order_type_sap
left join {{ ref('sap_units') }} as units
    on items.sales_unit_sap = units.sap_unit
left join {{ ref('sap_rejection_reasons') }} as reasons
    on items.rejection_reason_code = reasons.rejection_reason_code
left join {{ ref('fx_rates_to_eur') }} as fx
    on items.currency_code = fx.currency_code
