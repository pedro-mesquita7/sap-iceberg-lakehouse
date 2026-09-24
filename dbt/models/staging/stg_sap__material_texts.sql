with current_rows as (
    {{ sap_current_state(source('sap', 'makt'), ['mandt', 'matnr', 'spras']) }}
)

select
    {{ sap_alpha_out('matnr') }} as material_id,
    spras as language_key,
    maktx as description,
    _seq as source_seq,
    _extracted_at
from current_rows
