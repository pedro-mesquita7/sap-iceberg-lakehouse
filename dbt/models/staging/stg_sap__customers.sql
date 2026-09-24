with current_rows as (
    {{ sap_current_state(ref('stg_sap__customer_events'), ['customer_id']) }}
)

select
    customer_id,
    customer_name,
    country_code,
    city,
    account_group,
    account_group = '0005' as is_intercompany,
    created_on,
    deletion_flag = 'X' as is_marked_for_deletion,
    _seq as source_seq,
    _extracted_at
from current_rows
