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
	INNER JOIN imdb_plots t ON ('tt' || lpad(t.imdb_title_id, 7, '0') = m.imdb_title_id)
	WHERE m.ratingCount>1000
	AND t.summary IS NOT NULL
    AND m.imdb_title_id NOT IN (SELECT imdb_title_id
    FROM {{ref('wiki_plots_final')}})
	