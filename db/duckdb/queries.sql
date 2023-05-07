SELECT
    COUNT(
        CASE
            WHEN 0 <= ratingCount
            AND ratingCount <= 10 THEN ratingCount
        END
    ) AS in_range_0_10,
    COUNT(
        CASE
            WHEN 10 < ratingCount
            AND ratingCount <= 50 THEN ratingCount
        END
    ) AS in_range_10_50,
    COUNT(
        CASE
            WHEN 50 < ratingCount
            AND ratingCount <= 100 THEN ratingCount
        END
    ) AS in_range_50_100,
    COUNT(
        CASE
            WHEN 100 < ratingCount
            AND ratingCount <= 500 THEN ratingCount
        END
    ) AS in_range_100_500,
    COUNT(
        CASE
            WHEN 500 < ratingCount
            AND ratingCount <= 1000 THEN ratingCount
        END
    ) AS in_range_500_1000 COUNT(
        CASE
            WHEN 1000 < ratingCount
            AND ratingCount <= 5000 THEN ratingCount
        END
    ) AS in_range_1000_5000,
    COUNT(
        CASE
            WHEN 5000 < ratingCount
            AND ratingCount <= 10000 THEN ratingCount
        END
    ) AS in_range_5000_10000,
    COUNT(
        CASE
            WHEN 10000 < ratingCount
            AND ratingCount <= 50000 THEN ratingCount
        END
    ) AS in_range_10000_50000,
    COUNT(
        CASE
            WHEN 50000 < ratingCount
            AND ratingCount <= 100000 THEN ratingCount
        END
    ) AS in_range_50000_100000,
    COUNT(
        CASE
            WHEN 100000 < ratingCount
            AND ratingCount <= 500000 THEN ratingCount
        END
    ) AS in_range_100000_500000,
FROM
    imdb_movies;

select
    array_slice(imdb_title_id, 3, null)
from
    main.imdb_movies
WHERE
    year = {year}
    and ratingCount > 1000
    and imdb_title_id not in (
        SELECT
            imdb_title_id
        FROM
            imdb_wiki)

SELECT 
    m.title,
    m.description,
    COALESCE(str_split(m.stars,','),list_value('NA')) as stars,
    COALESCE(str_split(m.directors,','),list_value('NA')) as directors,
    m.year,
    COALESCE(m.certificate,'NA') as certificate,
    COALESCE(str_split(m.genre,','),list_value('NA')) as genre,
    COALESCE(m.runtime,'NA') as runtime,
    COALESCE(m.MetaScore,0) as MetaScore,
    m.ratingCount,''
    m.plot,
    m.summary,
    CAST(m.imdb_rating AS FLOAT) as imdb_rating, 
    m.imdb_title_id as source FROM imdb_wiki m
	