-- Every extracted change to KNA1, typed. Feeds both the current-state model and the SCD2 history.
select
    {{ sap_alpha_out('kunnr') }} as customer_id,
    name1 as customer_name,
    land1 as country_code,
    ort01 as city,
    ktokd as account_group,
    {{ sap_date('erdat') }} as created_on,
    loevm as deletion_flag,
    _op,
    _seq,
    -- initial-load rows carry no change timestamp; the record's creation date is the best proxy
    coalesce(_changed_at, {{ sap_date('erdat') }}::timestamp) as changed_at,
    _extracted_at
from {{ source('sap', 'kna1') }}
