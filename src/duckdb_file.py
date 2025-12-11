import duckdb

conn = duckdb.connect('db.duckdb')

conn.execute("CREATE TABLE if not exists nppes AS SELECT * FROM read_csv_auto('C:/Users/khare/DE_Oct2025/big_file/data/nppes_sample.csv')")

conn.close()