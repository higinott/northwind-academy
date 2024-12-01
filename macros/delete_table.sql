{% macro delete_table(schema_name, table_name) %}
    {{ log("Deleting table: " ~ schema_name ~ "." ~ table_name, info=True) }}
    {% if execute %}
        {% set sql %}
        DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }};
        {% endset %}
        {{ run_query(sql) }}
    {% endif %}
{% endmacro %}
