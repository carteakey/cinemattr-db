import shutil
import duckdb
import sys

mode = sys.argv[1]
print(mode)

if mode == 'init':
    con = duckdb.connect('db.duckdb')
else:
    con = duckdb.connect('db.duckdb')
    if mode =='export_db':
        con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")
    elif mode =='import_db':
        con.sql("IMPORT DATABASE 'tmp'")
    elif mode =='import_csv':
        from glob import glob
        for file in glob('./*.csv'):
            print(file)
            con.sql('TRUNCATE TABLE wiki_plots_temp;')
            print("Truncate")
            try:
                con.sql(f'''
                        INSERT INTO wiki_plots_temp SELECT * FROM '{file}'
                        '''
                        )
                print("Inserted into temp")
            except duckdb.BinderException as e:
                print(e)
                shutil.move(file,file+'_bad')
                continue

            try:
                con.sql('INSERT INTO wiki_plots SELECT * FROM (SELECT DISTINCT * FROM wiki_plots_temp) t ON CONFLICT (title) DO UPDATE SET summary=EXCLUDED.summary, plot=EXCLUDED.plot, external_links=EXCLUDED.external_links;')
                print("Inserted into final")
                shutil.move(file,file+'_imported')

                con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")
            except duckdb.Error as e:
                print(e)
                shutil.move(file,file+'_failed')
           
            
    elif mode == 'export_movies':
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

