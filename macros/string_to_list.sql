{#
This macro takes an input string that is comma delimited
and returns a list for those items. 

Example: dbt run-operation string_to_list --args "{'input_string':'vanilla,chocolate,banana'}"
#}
{% macro string_to_list(input_string) %}

    {% set return_list = input_string.split(',') %}

    {% for item in return_list %}
        {{ log(item | trim, true) }}
    {% endfor %}

    {{ return(return_list) }}
    
{% endmacro %}