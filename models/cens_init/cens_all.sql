{{ config(
    materialized="table"
) }}

{%- call statement('showTables', fetch_result=True) -%}
      SHOW TABLES
{%- endcall -%}

{%- set rows_ = load_result('showTables')['data'] -%}

--{{print(my_var)}}
{% for row in rows_ %}
    {{print(row[0])}}
{% endfor %}

with all_tables as (
    SELECT * FROM ts001
    {% for row in rows_ %}
        {% if row[0] != 'ts001' and 'ts0' in row[0] %}
            left outer join {{row[0]}} on
            ts001.date = {{row[0]}}.date and
            ts001."geography code" = {{row[0]}}."geography code"
        {% endif %}
    {% endfor %}
)
SELECT * FROM all_tables
