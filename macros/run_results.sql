{% macro run_results(results) %}

{% if execute %}

{# this just puts the results into their own tabels 

    {% set query_stmts = [] %}
    {% for result in results if result %}
        {{ query_stmts.append('select $$'~result~'$$') }}
    {% endfor %}

    {% set stmt_unioned = query_stmts | join(' union all ') %}

    {% set query_to_run = 'create or replace table development.dbt_bregenold.run_results as '~stmt_unioned %}

    {{ return(query_to_run) }}
#}

create or replace table development.dbt_bregenold.run_results_models as 

{# grab all the model info #}
{# TODO: 
     - flatten the ColumnInfo object in columns 
     - swap the $$ format for something database agnostics
#}

    {% for result in results if result.node.resource_type == 'model' %}
        select
        {# top level result data #}
        '{{ result.node.name }}' as model_name,
        '{{ result.status }}' as run_status,
        '{{ result.thread_id }}' as run_thread_id,
        '{{ result.execution_time }}' as run_execution_time,
        $${{ result.adapter_response}}$$ as adapter_response,
        '{{ result.message }}' as message,
        '{{ result.agate_table }}' as agate_table,

        {# flattened timing info #}
        {% for step in result.timing %}
            {% set step_name = step.name %}
            '{{ step.started_at }}' as {{ step_name~'_stared_at' }},
            '{{ step.completed_at }}' as {{ step_name~'_completed_at' }},
        {% endfor %}

        {# model speific info #}
        $${{ result.node.raw_sql }}$$ as raw_sql,
        '{{ result.node.compiled }}' as compiled,
        '{{ result.node.database }}' as database,
        '{{ result.node.schema }}' as schema,
        '{{ result.node.fqn | join(".") }}' as fully_qualified_name,
        '{{ result.node.unique_id }}' as unique_id,
        '{{ result.node.package_name }}' as package_name,
        '{{ result.node.root_path }}' as root_path,
        '{{ result.node.path }}' as path,
        '{{ result.node.original_file_path }}' as original_file_path,
        '{{ result.node.name }}' as name,
        '{{ result.node.resource_type }}' as resource_type,
        '{{ result.node.alias }}' as alias,
        '{{ result.node.checksum.name }}' as checksum_name,
        '{{ result.node.checksum.checksum }}' as checksum_value,
        '{{ result.node.config._extra }}' as config__extra,
        '{{ result.node.config.enabled }}' as config_enabled,
        '{{ result.node.config.alias }}' as config_alias,
        '{{ result.node.config.schema }}' as config_schema,
        '{{ result.node.config.database }}' as config_database,
        '{{ result.node.config.tags }}' as config_tags,
        '{{ result.node.config.meta }}' as config_meta,
        '{{ result.node.config.materialized }}' as config_materialized,
        '{{ result.node.config.persist_docs }}' as config_persist_docs,
        '{{ result.node.config.post_hook }}' as config_post_hook,
        '{{ result.node.config.pre_hook }}' as config_pre_hook,
        '{{ result.node.config.quoting }}' as config_quoting,
        '{{ result.node.config.column_types }}' as config_column_types,
        '{{ result.node.config.full_refresh }}' as config_full_refresh,
        '{{ result.node.config.on_schema_change }}' as config_on_schema_change,
        '{{ result.node.tags }}' as tags,
        '{{ result.node.refs }}' as refs,
        $${{ result.node.sources }}$$ as sources,
        --depends_on
        '{{ result.node.description }}' as description,
        $${{ result.node.columns }}$$ as columns,
        '{{ result.node.meta }}' as meta,
        --docs
        '{{ result.node.patch_path }}' as patch_path,
        '{{ result.node.compiled_path }}' as compiled_path,
        '{{ result.node.build_path }}' as build_path,
        '{{ result.node.deferred }}' as deferred,
        '{{ result.node.unrendered_config }}' as unrendered_config,
        '{{ result.node.created_at }}' as created_at,
        '{{ result.node.onfig_call_dict }}' as onfig_call_dict,
        '{{ result.node._event_status }}' as _event_status,
        $${{ result.node.compiled_sql }}$$ as compiled_sql,
        '{{ result.node.extra_ctes_injected }}' as extra_ctes_injected,
        '{{ result.node.extra_ctes }}' as extra_ctes,
        '{{ result.node.relation_name }}' as relation_name,
        '{{ result.node._pre_injected_sql }}' as _pre_injected_sql

        {{ 'union all' if not loop.last }}
    {% endfor %}

{% endif %}

{% endmacro %}