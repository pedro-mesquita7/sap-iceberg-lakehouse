-- SCD Type 2 built from the CDC event log itself rather than from snapshots, so every change
-- is captured with its source timestamp - even several changes between two dbt runs.
with events as (
    -- the same change can be extracted twice (re-run after a failure): keep one per sequence
    select distinct customer_id, customer_name, country_code, city, _op, _seq, changed_at
    from {{ ref('stg_sap__customer_events') }}
),

versions as (
    -- drop events that did not change any tracked attribute
    select *
    from events
    qualify md5(concat_ws('|', _op, customer_name, country_code, city))
        is distinct from lag(md5(concat_ws('|', _op, customer_name, country_code, city)))
            over (partition by customer_id order by _seq)
),

windowed as (
    select
        *,
        lead(changed_at) over (partition by customer_id order by _seq) as next_changed_at
    from versions
)

select
    md5(customer_id || '|' || _seq) as customer_version_key,
    customer_id,
    customer_name,
    country_code,
    city,
    changed_at as valid_from,
    next_changed_at as valid_to,
    next_changed_at is null as is_current
from windowed
where _op <> 'D'
