import shutil
import duckdb
import sys
from datetime import datetime
from importlib.metadata import version

timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
db_version = version("duckdb")
print("Timestamp:" + timestamp)
print("DuckDb version:" + db_version)

db_file = sys.argv[1]
mode = sys.argv[2]
print(db_file)
print(mode)

snapshot_file = db_file + "." + db_version + "." + timestamp
temp_file = "db.duckdb_tmp"

if mode == "init":
    con = duckdb.connect(db_file)

else:
    con = duckdb.connect(db_file)

    if mode == "export_db":
        con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")

    elif mode == "import_db":
        print("Creating Snapshot:", snapshot_file)
        shutil.copy(db_file, snapshot_file)
        con.sql("IMPORT DATABASE 'tmp'")

    elif mode == "vacuum":
        con_new = duckdb.connect(temp_file)

        con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")
        con_new.sql("IMPORT DATABASE 'tmp'")

        print("Creating Snapshot:", snapshot_file)
        shutil.copy(db_file, snapshot_file)
        shutil.move(temp_file, db_file)

    elif mode == "import_csv":
        from glob import glob

        for file in glob("./*.csv"):
            print(file)
            con.sql("TRUNCATE TABLE wiki_plots_temp;")
            print("Truncate")
            try:
                con.sql(
                    f"""
                        INSERT INTO wiki_plots_temp SELECT * FROM '{file}'
                        """
                )
                print("Inserted into temp")
            except duckdb.BinderException as e:
                print(e)
                shutil.move(file, file + "_bad")
                continue

            try:
                con.sql(
                    "INSERT INTO wiki_plots SELECT * FROM (SELECT DISTINCT * FROM wiki_plots_temp) t ON CONFLICT (title) DO UPDATE SET summary=EXCLUDED.summary, plot=EXCLUDED.plot, external_links=EXCLUDED.external_links;"
                )
                print("Inserted into final")
                shutil.move(file, file + "_imported")

                con.sql("EXPORT DATABASE 'tmp' (FORMAT PARQUET)")
            except duckdb.Error as e:
                print(e)
                shutil.move(file, file + "_failed")

    elif mode == "export_movies":
        con.sql(
            """COPY ( SELECT 
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
        """
        )

        con = duckdb.connect("./db.duckdb_ex")
        con.sql(
            """CREATE TABLE movie_plots as SELECT * FROM 'movie_plots.csv'
        """
        )
