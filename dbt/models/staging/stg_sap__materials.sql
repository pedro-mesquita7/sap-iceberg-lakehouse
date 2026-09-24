with current_rows as (
    {{ sap_current_state(source('sap', 'mara'), ['mandt', 'matnr']) }}
)

select
    {{ sap_alpha_out('matnr') }} as material_id,
    mtart as material_type,
    matkl as material_group,
    meins as base_unit_sap,
    {{ sap_number('brgew') }} as gross_weight,
    gewei as weight_unit,
    {{ sap_date('ersda') }} as created_on,
    {{ sap_date('laeda') }} as last_changed_on,
    _seq as source_seq,
    _extracted_at
from current_rows
