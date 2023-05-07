{% macro wiki_regex_plot(column_name) %} 
    regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace({{ column_name }}, '<!--.*?-->', -- remove comments
									'', 'g'), '\[\[(.+?)\]\]', -- remove wiki link markup
								'\1', 'g'), '<ref>.*?</ref>', -- remove reference links
							'', 'g'), '<ref .*?/>', -- remove self-closing reference tags
						'', 'g'), '<[^>]*>', -- remove any remaining HTML tags
					'', 'g'), '\{\{.*?\}\}', -- remove reference links
				'', 'g'), '\|', -- Remove link aliases
			' ', 'g'), '\s+', -- Remove extra whitespace
		' ', 'g'),'''''', -- Remove extra quotes
		'''', 'g')
{% endmacro %}