
{{ config(materialized='table') }}

SELECT
	m.title,
	m.imdb_title_id,
	m.description,
	m.stars,
	m.directors,
	m.year, 
	m.certificate, 
	m.genre, 
	m.runtime,
	m.IMDb_rating,
	m.MetaScore, 
	m.ratingCount,
	t.summary,
	t.plot
FROM
	imdb_movies m
	INNER JOIN {{ref('wiki_plots_clean')}} t ON ('tt' || lpad(t.imdb_title_id, 7, '0') = m.imdb_title_id)
		--Title matching as a fallback
		OR (t.title like '%' || m.title || '%' and m.year =  t.year) 
	WHERE m.ratingCount>1000
	