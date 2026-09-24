with current_rows as (
    {{ sap_current_state(source('sap', 'likp'), ['mandt', 'vbeln']) }}
)

select
    {{ sap_alpha_out('vbeln') }} as delivery_id,
    {{ sap_date('erdat') }} as created_on,
    {{ sap_alpha_out('kunnr') }} as customer_id,
    {{ sap_date('wadat_ist') }} as goods_issue_on,
    {{ sap_date('aedat') }} as last_changed_on,
    _seq as source_seq,
    _extracted_at
from current_rows
