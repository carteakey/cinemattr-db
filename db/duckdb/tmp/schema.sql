


CREATE TABLE movie_plots(title VARCHAR, imdb_title_id VARCHAR, description VARCHAR, stars VARCHAR, directors VARCHAR, "year" INTEGER, certificate VARCHAR, genre VARCHAR, runtime VARCHAR, "IMDb_rating" DECIMAL(18,3), "MetaScore" DECIMAL(18,3), "ratingCount" INTEGER, summary VARCHAR, plot VARCHAR);
CREATE TABLE wiki_plots_final(title VARCHAR, imdb_title_id VARCHAR, description VARCHAR, stars VARCHAR, directors VARCHAR, "year" INTEGER, certificate VARCHAR, genre VARCHAR, runtime VARCHAR, "IMDb_rating" DECIMAL(18,3), "MetaScore" DECIMAL(18,3), "ratingCount" INTEGER, summary VARCHAR, plot VARCHAR);
CREATE TABLE imdb_plots(imdb_title_id VARCHAR PRIMARY KEY, summary VARCHAR, plot VARCHAR);
CREATE TABLE imdb_movies_temp(imdb_title_id VARCHAR PRIMARY KEY, title VARCHAR, "year" INTEGER, certificate VARCHAR, genre VARCHAR, runtime VARCHAR, description VARCHAR, "IMDb_rating" DECIMAL(18,3), "MetaScore" DECIMAL(18,3), "ratingCount" INTEGER, directors VARCHAR, stars VARCHAR);
CREATE TABLE imdb_movies(imdb_title_id VARCHAR PRIMARY KEY, title VARCHAR, "year" INTEGER, certificate VARCHAR, genre VARCHAR, runtime VARCHAR, description VARCHAR, "IMDb_rating" DECIMAL(18,3), "MetaScore" DECIMAL(18,3), "ratingCount" INTEGER, directors VARCHAR, stars VARCHAR);
CREATE TABLE wiki_plots_temp(title VARCHAR PRIMARY KEY, summary VARCHAR, plot VARCHAR, external_links VARCHAR);
CREATE TABLE wiki_plots(title VARCHAR PRIMARY KEY, summary VARCHAR, plot VARCHAR, external_links VARCHAR);
CREATE TABLE imdb_plots_temp(imdb_title_id VARCHAR PRIMARY KEY, summary VARCHAR, plot VARCHAR);
CREATE TABLE wiki_plots_clean(title VARCHAR, imdb_title_id VARCHAR, summary VARCHAR, plot VARCHAR, "year" VARCHAR);
CREATE TABLE imdb_plots_final(title VARCHAR, imdb_title_id VARCHAR, description VARCHAR, stars VARCHAR, directors VARCHAR, "year" INTEGER, certificate VARCHAR, genre VARCHAR, runtime VARCHAR, "IMDb_rating" DECIMAL(18,3), "MetaScore" DECIMAL(18,3), "ratingCount" INTEGER, summary VARCHAR, plot VARCHAR);




