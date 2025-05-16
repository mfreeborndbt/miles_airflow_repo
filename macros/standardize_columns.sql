{% macro standardize_columns(columns_dict, source_relation) %}
    select
        {% for col, config in columns_dict.items() %}
            {% if config is string %}
                {# Support old format like "weather_code": "default" #}
                {% set col_type = config %}
                {% set alias = col %}
            {% else %}
                {% set col_type = config.get('type', 'default') %}
                {% set alias = config.get('alias', col) %}
            {% endif %}

            {% if col_type == 'default' %}
                cast({{ col }} as string) as {{ alias }},
            {% elif col_type == 'precise' %}
                round({{ col }}, 1) as {{ alias }},
            {% elif col_type == 'whole_number' %}
                round({{ col }}) as {{ alias }},
            {% elif col_type == 'whole_date' %}
                cast({{ col }} as date) as {{ alias }},
            {% elif col_type == 'hours' %}
                round({{ col }} / 3600.0, 1) as {{ alias }},
            {% else %}
                {{ col }} as {{ alias }},
            {% endif %}
        {% endfor %}
    from {{ source_relation }}
{% endmacro %}
