{# 
    This is an incomplete macro. 
    It was designed to see which macros are
    being used in tests.

    You can put this code snip into a scratchpad
    and hit compile to see the results.
    
#}

{%- for node in graph.nodes.items() -%}

    {%- if node[1].resource_type == 'test' -%}

        {%- set test_name = node[0] -%}

        {%- for macro in node[1].depends_on.macros -%}

            {# 
                This if statement limits the macros to those written in this project.
                You'll need to adjust this if you import your macros from a different project.
            #}
            {%- if project_name in macro %}

                Test {{ test_name }} depends on {{ macro }}
            
            {%- endif -%}
        
        {%- endfor -%}
    
    {% endif %}

{%- endfor -%}