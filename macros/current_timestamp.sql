{% macro current_load_date() %}
    {#-
        Returns the current UTC timestamp as TIMESTAMP_NTZ.
        Centralised here so that all vault models use the same
        load-date expression and it's easy to mock in tests.
    -#}
    convert_timezone('UTC', current_timestamp())::timestamp_ntz
{% endmacro %}
