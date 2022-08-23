{% test not_null_incremental(model, column_name) %}

{% set column_list = '*' if should_store_failures() else column_name %}

select {{ column_list }}
from {{ model }}
where 
    {{ column_name }} is null
    and created_at = (select max(created_at) from {{ model }} )

{% endtest %}