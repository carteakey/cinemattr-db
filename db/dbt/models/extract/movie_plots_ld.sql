{{ config(materialized = 'table') }}

SELECT
    lower(m.title) as title,
    COALESCE(str_split(lower(m.stars), ','), list_value('NA')) as stars,
    COALESCE(
        str_split(lower(m.directors), ','),
        list_value('NA')
    ) as directors,
    m.year,
    COALESCE(str_split(lower(m.genre), ','), list_value('NA')) as genre,
    COALESCE(
        CAST(trim(replace(RUNTIME, 'min', '')) AS INTEGER),
        -1
    ) AS runtime,
    m.ratingCount,
    m.plot,
    m.summary,
    CAST(m.imdb_rating AS FLOAT) as imdb_rating,
    m.imdb_title_id as source
FROM
    {{ref('movie_plots')}}