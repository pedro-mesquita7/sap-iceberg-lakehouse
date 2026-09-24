with bookings as (
    select
        order_date as sales_date,
        sales_org,
        count(distinct order_id) as orders_booked,
        sum(net_value_eur) filter (where not is_rejected) as booked_eur
    from {{ ref('fct_sales_order_items') }}
    group by 1, 2
),

billing as (
    select
        billing_date as sales_date,
        sales_org,
        sum(net_value_eur) filter (where not is_credit_memo) as invoiced_eur,
        sum(net_value_eur) filter (where is_credit_memo) as credited_eur
    from {{ ref('fct_billing_items') }}
    group by 1, 2
),

combined as (
    select
        coalesce(bookings.sales_date, billing.sales_date) as sales_date,
        coalesce(bookings.sales_org, billing.sales_org) as sales_org,
        coalesce(bookings.orders_booked, 0) as orders_booked,
        coalesce(bookings.booked_eur, 0) as booked_eur,
        coalesce(billing.invoiced_eur, 0) as invoiced_eur,
        coalesce(billing.credited_eur, 0) as credited_eur
    from bookings
    full outer join billing
        on bookings.sales_date = billing.sales_date
        and bookings.sales_org = billing.sales_org
)

select
    combined.sales_date,
    combined.sales_org,
    orgs.sales_org_name,
    cast(combined.orders_booked as integer) as orders_booked,
    cast(combined.booked_eur as decimal(18, 2)) as booked_eur,
    cast(combined.invoiced_eur as decimal(18, 2)) as invoiced_eur,
    cast(combined.credited_eur as decimal(18, 2)) as credited_eur,
    cast(combined.invoiced_eur + combined.credited_eur as decimal(18, 2)) as net_revenue_eur
from combined
left join {{ ref('sap_sales_orgs') }} as orgs
    on combined.sales_org = orgs.sales_org
