{% macro wiki_regex_imdb_title_id(column_name) %}
    	CASE WHEN regexp_extract(regexp_extract({{ column_name }},'\{\{[Ii][Mm][Dd][Bb] title(.*)(\|.*)?\}\}',1),'\d+') <>''
		THEN regexp_extract(regexp_extract({{ column_name }},'\{\{[Ii][Mm][Dd][Bb] title(.*)(\|.*)?\}\}',1),'\d+') 
		WHEN regexp_extract({{ column_name }},'https:\/\/www\.imdb\.com\/title\/tt(\d+).*',1) <>''
		THEN regexp_extract({{ column_name }},'https:\/\/www\.imdb\.com\/title\/tt(\d+).*',1)
		WHEN {{ column_name }} = 'No external links available' THEN '-1'
        END
{% endmacro %}