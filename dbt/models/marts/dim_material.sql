with texts as (
    select
        material_id,
        max(description) filter (where language_key = 'E') as description_en,
        max(description) filter (where language_key = 'D') as description_de,
        max(description) filter (where language_key = 'P') as description_pt
    from {{ ref('stg_sap__material_texts') }}
    group by material_id
)

select
    materials.material_id,
    -- fall back through the languages when a text is missing in English
    coalesce(texts.description_en, texts.description_de, texts.description_pt, materials.material_id)
        as material_name,
    texts.description_de,
    texts.description_pt,
    materials.material_type,
    materials.material_group,
    units.unit_code as base_unit,
    units.iso_code as base_unit_iso,
    cast(materials.gross_weight as decimal(18, 3)) as gross_weight_kg,
    materials.created_on,
    materials.last_changed_on
from {{ ref('stg_sap__materials') }} as materials
left join texts
    on materials.material_id = texts.material_id
left join {{ ref('sap_units') }} as units
    on materials.base_unit_sap = units.sap_unit
