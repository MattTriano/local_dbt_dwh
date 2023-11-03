{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
 
    {%- if target.name == "dev" -%}
        {%- set schema_prefix = "dev_" -%}
    {%- elif target.name == "prod" -%}
        {%- set schema_prefix = "" -%}
    {%- else -%} {%- set schema_prefix = target.name ~ "_" -%}
    {%- endif -%}

   {%- if custom_schema_name is none -%}
        {{ schema_prefix ~ default_schema | trim }}
    {%- else -%}
        {{ schema_prefix ~ custom_schema_name | trim }}
    {%- endif -%}


{%- endmacro %}
