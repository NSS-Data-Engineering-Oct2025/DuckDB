import duckdb

def main():
    conn = duckdb.connect('db.duckdb')
    
    conn.execute("CREATE TABLE if not exists nppes AS SELECT * FROM read_csv('C:/Users/khare/DE_Oct2025/big_file/data/npidata_pfile_20050523-20250413.csv', all_varchar=True)")
    
    conn.sql("SELECT NPI from nppes limit 10").show()
    conn.close()

if __name__ == '__main__':
    main()