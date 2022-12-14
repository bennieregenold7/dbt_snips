/*
  --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml 
*/
{% macro store_test_results(results) %}

  {%- set central_tbl -%} {{ target.schema }}.test_results_central {%- endset -%}
  {%- set test_results = [] -%}
  {%- for result in results if result.node.resource_type == 'test' -%}
    {%- do test_results.append(result) -%}
  {%- endfor -%}

  {# if no tests were run, skip this iteration #}
  {% if test_results | length == 0 -%}
    {{ return('select 1') }}
  {%- endif -%}

  {{ log("Centralizing test data in " + central_tbl, info = true) if execute }}

  {# Check if table exists #}
  {% set central_table_query %} {{ dbt_utils.get_tables_by_pattern_sql(target.schema | upper, 'TEST_RESULTS_CENTRAL') }} {% endset %}
  {% if execute %}
    {% set central_table_exists = run_query(central_table_query) %}
  {% endif %}

  {% if central_table_exists%}
    insert into {{ central_tbl }} (
  {% else %}
    create table {{ central_tbl }} as (
  {% endif %}

  
  {% for result in test_results %}

    {% set test_name='' %}
    {% set test_type='' %}

    {% if result.node.test_metadata is defined %}
      {% set test_name = result.node.test_metadata.name %}
      {% set test_type='generic' %}
    {% elif result.node.name is defined %}
      {% set test_name = result.node.name %}
      {% set test_type='singular' %}
    {% endif %}
    
    select
      {{ dbt_utils.surrogate_key( "'"~test_name ~ "'" , dbt_utils.current_timestamp() ) }} as test_sk, 
      '{{ test_name }}'::text as test_name,
      '{{ result.node.name }}'::text as test_name_long,
      '{{ test_type}}'::text as test_type,
      '{{ process_refs(result.node.refs) }}'::text as model_refs,
      '{{ process_refs(result.node.sources, is_src=true) }}'::text as source_refs,
      '{{ result.node.config.severity }}'::text as test_severity_config,
      '{{ result.execution_time }}'::text as execution_time_seconds,
      '{{ result.status }}'::text as test_result,
      '{{ result.node.original_file_path }}'::text as file_test_defined,
      '{{ result.node.compiled_sql | replace('\'','\"') }}'::varchar as compiled_sql,
      current_timestamp as _timestamp
    
    {{ "union all" if not loop.last }}
  
  {% endfor %}
  
  );

{% endmacro %}


/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders
  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/
{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {{ refs.append(ref[0]) }}
        {% endif %} 
      {% endfor %}

      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}