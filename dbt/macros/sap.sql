{#
    Helpers for the data-format quirks every SAP extraction runs into.
#}

{# Latest state per key from an append-only CDC event log, with deleted keys removed. #}
{% macro sap_current_state(relation, keys) %}
    select * exclude (_rn)
    from (
        select
            *,
            row_number() over (
                partition by {{ keys | join(', ') }}
                order by _seq desc, _extracted_at desc
            ) as _rn
        from {{ relation }}
    )
    where _rn = 1 and _op <> 'D'
{% endmacro %}


{# ALPHA conversion (output): strip leading zeros from purely numeric keys only.
   '0000100042' -> '100042', but external keys like 'IC-PORTO' stay untouched. #}
{% macro sap_alpha_out(column) %}
    case
        when regexp_full_match({{ column }}, '[0-9]+')
            then coalesce(nullif(ltrim({{ column }}, '0'), ''), '0')
        else {{ column }}
    end
{% endmacro %}


{# DATS fields: 'YYYYMMDD', where '00000000' (or blank) means "no date". #}
{% macro sap_date(column) %}
    case
        when {{ column }} is null or {{ column }} in ('', '00000000') then null
        else strptime({{ column }}, '%Y%m%d')::date
    end
{% endmacro %}


{# Numbers as SAP renders them: negative values carry a trailing minus ('125.00-'). #}
{% macro sap_number(column, scale=3) %}
    case
        when {{ column }} is null or trim({{ column }}) = '' then null
        when {{ column }} like '%-' then -1 * cast(rtrim(trim({{ column }}), '-') as decimal(18, {{ scale }}))
        else cast(trim({{ column }}) as decimal(18, {{ scale }}))
    end
{% endmacro %}


{# CURR fields are stored with 2 decimals whatever the currency. TCURX lists the exceptions
   (JPY has 0, KWD has 3), so the real amount is internal * 10^(2 - decimals). #}
{% macro sap_amount(amount_column, currency_column) %}
    cast(
        {{ sap_number(amount_column) }} * pow(10, 2 - coalesce((
            select tcurx.currency_decimals
            from {{ ref('stg_sap__currency_decimals') }} as tcurx
            where tcurx.currency_code = {{ currency_column }}
        ), 2))
        as decimal(18, 3)
    )
{% endmacro %}
