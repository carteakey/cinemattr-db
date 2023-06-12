COPY movie_plots FROM 'tmp/movie_plots.parquet' (FORMAT 'parquet');
COPY wiki_plots_final FROM 'tmp/wiki_plots_final.parquet' (FORMAT 'parquet');
COPY imdb_plots FROM 'tmp/imdb_plots.parquet' (FORMAT 'parquet');
COPY imdb_movies_temp FROM 'tmp/imdb_movies_temp.parquet' (FORMAT 'parquet');
COPY imdb_movies FROM 'tmp/imdb_movies.parquet' (FORMAT 'parquet');
COPY wiki_plots_temp FROM 'tmp/wiki_plots_temp.parquet' (FORMAT 'parquet');
COPY wiki_plots FROM 'tmp/wiki_plots.parquet' (FORMAT 'parquet');
COPY imdb_plots_temp FROM 'tmp/imdb_plots_temp.parquet' (FORMAT 'parquet');
COPY wiki_plots_clean FROM 'tmp/wiki_plots_clean.parquet' (FORMAT 'parquet');
COPY imdb_plots_final FROM 'tmp/imdb_plots_final.parquet' (FORMAT 'parquet');
