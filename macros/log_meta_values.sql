{#
This macro shows an example of how to parse the graph object to retrieve 
information in the meta fields for a table or column
#}
{% macro log_meta_values() %}

    {# Only grab the graph on the execute pass #}
    {% if execute %}
        {% set model_graph = graph %}
    {% else %}
        {% set model_graph = [] %}
    {% endif %}

    {# Parse the graph to get meta level fields #}
    {# The outer for loop grabs table level details #}
    {% for model in graph.nodes.values() %}

        {% if model.config.meta.contains_pii %}

            {{ log('The model '~model.unique_id~' has a meta tag of  '~model.config.meta.contains_pii, true) }}

            {# The inner for loop grabs column level details #}
            {% for col in model.columns.values() %}

                {% if col.meta.contains_pii %}

                    {{ log('The column '~col.name~' has a meta tag of  '~col.meta.contains_pii, true) }}
                    
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}


{% endmacro %}