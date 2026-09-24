with current_rows as (
    {{ sap_current_state(source('sap', 'lips'), ['mandt', 'vbeln', 'posnr']) }}
)

select
    {{ sap_alpha_out('vbeln') }} as delivery_id,
    cast(posnr as integer) as item_number,
    {{ sap_alpha_out('vgbel') }} as order_id,
    cast(vgpos as integer) as order_item_number,
    {{ sap_alpha_out('matnr') }} as material_id,
    {{ sap_number('lfimg') }} as delivered_quantity,
    vrkme as sales_unit_sap,
    _seq as source_seq,
    _extracted_at
from current_rows
