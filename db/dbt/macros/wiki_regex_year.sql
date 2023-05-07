{% macro wiki_regex_year(column_name) %} 
CASE WHEN 
regexp_extract({{ column_name }},'\[\[Category:(\d{4}).*\]\]',1) <> ''
THEN regexp_extract({{ column_name }},'\[\[Category:(\d{4}).*\]\]',1)
ELSE NULL 
END 
{% endmacro %}