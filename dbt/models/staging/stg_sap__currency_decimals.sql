with current_rows as (
    {{ sap_current_state(source('sap', 'tcurx'), ['currkey']) }}
)

select
    currkey as currency_code,
    cast(currdec as integer) as currency_decimals
from current_rows
