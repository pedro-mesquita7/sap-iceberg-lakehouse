with current_rows as (
    {{ sap_current_state(source('sap', 'vbrk'), ['mandt', 'vbeln']) }}
)

select
    {{ sap_alpha_out('vbeln') }} as billing_id,
    fkart as billing_type,
    {{ sap_date('fkdat') }} as billing_date,
    {{ sap_alpha_out('kunrg') }} as payer_id,
    {{ sap_amount('netwr', 'waerk') }} as net_value,
    waerk as currency_code,
    fksto = 'X' as is_cancelled,
    _seq as source_seq,
    _extracted_at
from current_rows
