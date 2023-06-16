{% macro wiki_regex_title(column_name) %}
    	trim(regexp_replace(regexp_replace({{ column_name }},'\(\d{4}\s\w+\)',''),'\(film\)',''))
{% endmacro %}