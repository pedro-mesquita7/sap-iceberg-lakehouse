-- VBRP has no currency field: item amounts are interpreted with the header currency (VBRK-WAERK).
with items as (
    {{ sap_current_state(source('sap', 'vbrp'), ['mandt', 'vbeln', 'posnr']) }}
),

headers as (
    select billing_id, currency_code
    from {{ ref('stg_sap__billing_documents') }}
)

select
    {{ sap_alpha_out('items.vbeln') }} as billing_id,
    cast(items.posnr as integer) as item_number,
    {{ sap_alpha_out('items.vgbel') }} as reference_document_id,
    cast(items.vgpos as integer) as reference_item_number,
    {{ sap_alpha_out('items.aubel') }} as order_id,
    cast(items.aupos as integer) as order_item_number,
    {{ sap_alpha_out('items.matnr') }} as material_id,
    {{ sap_number('items.fkimg') }} as billed_quantity,
    {{ sap_amount('items.netwr', 'headers.currency_code') }} as net_value,
    headers.currency_code,
    items._seq as source_seq,
    items._extracted_at
from items
left join headers
    on {{ sap_alpha_out('items.vbeln') }} = headers.billing_id
