{% macro hash_key(columns) %}
    {#- Generate a SHA-256 based hash key from one or more business key columns -#}
    {%- if columns | length == 1 -%}
        upper(sha2(cast({{ columns[0] }} as varchar), 256))
    {%- else -%}
        upper(sha2(
            concat_ws('||',
                {%- for col in columns %}
                    coalesce(cast({{ col }} as varchar), '^^')
                    {%- if not loop.last %},{% endif %}
                {%- endfor %}
            ),
            256
        ))
    {%- endif -%}
{% endmacro %}
