{#
This macro parses the graph object and returns a dictionary
with the model/source name, and its schema.

Sample output: {'my_first_dbt_model': 'my_schema', 'my_second_dbt_model': 'my_schema'}
#}
{% macro get_node_schemas() %}

    {% if execute %}

        {% set all_nodes = {} %}


        {# This gets information for all models #}
        {% for node in graph.nodes.values()
            | selectattr("resource_type", "equalto", "model") %}

            {% do all_nodes.update({node.name: node.schema}) %}

        {% endfor %}

        {# This gets information for all sources #}
        {% for source in graph.sources.values() -%}

            {% do all_nodes.update({source.name: source.schema}) %}

        {% endfor %}

        {{ log(all_nodes, true) }}

        {{ return(all_nodes) }}

    {% endif %}

{% endmacro %}