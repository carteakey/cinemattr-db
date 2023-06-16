{{ config(materialized='table') }}

select {{ wiki_regex_title('title') }} as title,
{{ wiki_regex_imdb_title_id('external_links') }} as imdb_title_id,
{{ wiki_regex_summary('summary') }} as summary,
{{ wiki_regex_plot('plot') }} as plot,
{{ wiki_regex_year('external_links')  }} as year
from
    wiki_plots s
WHERE
    plot <> 'No plot available'
    AND plot <> ' '