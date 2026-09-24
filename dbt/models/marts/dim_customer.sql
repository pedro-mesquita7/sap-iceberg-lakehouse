select
    customer_id,
    customer_name,
    country_code,
    city,
    account_group,
    is_intercompany,
    created_on,
    is_marked_for_deletion
from {{ ref('stg_sap__customers') }}
