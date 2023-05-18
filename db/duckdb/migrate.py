import duckdb

# con = duckdb.connect('./db.duckdb_old')
# con.sql("EXPORT DATABASE 'tmp'")

con = duckdb.connect('./db.duckdb')
con.sql("IMPORT DATABASE 'tmp'")