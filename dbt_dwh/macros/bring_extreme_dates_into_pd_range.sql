{% macro bring_extreme_dates_into_pd_range(input_col, output_col=none) %}
case
    when {{ input_col }} < '1677-09-12' then '1677-09-12'
    when {{ input_col }} > '2262-04-11' then '2622-04-11'
    else {{ input_col }}
end as {% if output_col is not none %}{{- output_col -}}{% else %}{{- input_col -}}{% endif -%}
{% endmacro %}

