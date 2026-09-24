with current_rows as (
    {{ sap_current_state(source('sap', 'vbap'), ['mandt', 'vbeln', 'posnr']) }}
)

select
    {{ sap_alpha_out('vbeln') }} as order_id,
    cast(posnr as integer) as item_number,
    {{ sap_alpha_out('matnr') }} as material_id,
    {{ sap_number('kwmeng') }} as order_quantity,
    vrkme as sales_unit_sap,
    {{ sap_amount('netwr', 'waerk') }} as net_value,
    waerk as currency_code,
    nullif(abgru, '') as rejection_reason_code,
    abgru <> '' as is_rejected,
    {{ sap_date('erdat') }} as created_on,
    {{ sap_date('aedat') }} as last_changed_on,
    _seq as source_seq,
    _extracted_at
from current_rows
