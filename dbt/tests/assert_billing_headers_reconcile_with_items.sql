-- Reconciliation: every billing header (VBRK) must equal the sum of its items (VBRP).
-- A mismatch usually means items were extracted without their header or vice versa.
select
    headers.billing_id,
    headers.net_value as header_value,
    sum(items.net_value) as items_value
from {{ ref('stg_sap__billing_documents') }} as headers
left join {{ ref('stg_sap__billing_items') }} as items
    on headers.billing_id = items.billing_id
group by 1, 2
having abs(headers.net_value - coalesce(sum(items.net_value), 0)) > 0.01
