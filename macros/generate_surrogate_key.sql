{% macro generate_surrogate_key(field_list) %}
    {#-
        Thin wrapper around dbt_utils.generate_surrogate_key.
        Provides a single call-site so that the hashing algorithm
        can be swapped project-wide in one place.
    -#}
    {{ dbt_utils.generate_surrogate_key(field_list) }}
{% endmacro %}
