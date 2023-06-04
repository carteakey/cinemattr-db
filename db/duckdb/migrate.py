import duckdb
import sys

mode = sys.argv[1]
print(mode)

if mode == 'init':
    con = duckdb.connect('./db.duckdb')

elif mode =='export_db':
    con = duckdb.connect('./db.duckdb')
    con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")

elif mode =='import_db':
    con = duckdb.connect('./db.duckdb')
    con.sql("IMPORT DATABASE 'tmp'")

elif mode == 'export_movies':
    con = duckdb.connect('./db.duckdb')
    con.sql('''COPY ( SELECT 
    lower(m.title) as title,
    COALESCE(str_split(lower(m.stars),','),list_value('NA')) as stars,
    COALESCE(str_split(lower(m.directors),','),list_value('NA')) as directors,
    m.year,
    COALESCE(str_split(lower(m.genre),','),list_value('NA')) as genre,
    COALESCE(CAST(trim(replace(RUNTIME,'min','')) AS INTEGER),-1) AS runtime,
    m.ratingCount,
    m.plot,
    m.summary,
    CAST(m.imdb_rating AS FLOAT) as imdb_rating, 
    m.imdb_title_id as source FROM movie_plots m) TO 'movie_plots.csv' (HEADER, DELIMITER ',')
    ''')

    con = duckdb.connect('./db.duckdb_ex')
    con.sql('''CREATE TABLE movie_plots as SELECT * FROM 'movie_plots.csv'
    ''')

