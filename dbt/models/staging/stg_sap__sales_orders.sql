with current_rows as (
    {{ sap_current_state(source('sap', 'vbak'), ['mandt', 'vbeln']) }}
)

select
    {{ sap_alpha_out('vbeln') }} as order_id,
    {{ sap_date('erdat') }} as created_on,
    strptime(erdat || erzet, '%Y%m%d%H%M%S') as created_at,
    auart as order_type_sap,
    vkorg as sales_org,
    vtweg as distribution_channel,
    spart as division,
    {{ sap_alpha_out('kunnr') }} as customer_id,
    {{ sap_amount('netwr', 'waerk') }} as net_value,
    waerk as currency_code,
    {{ sap_date('aedat') }} as last_changed_on,
    gbstk as overall_status,
    _seq as source_seq,
    _extracted_at
from current_rows
