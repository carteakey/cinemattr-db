{{ config(materialized='table') }}

SELECT
*
FROM
	{{ref('wiki_plots_final')}}
UNION ALL
SELECT 
* 
FROM 
    {{ref('imdb_plots_final')}}