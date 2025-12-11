import duckdb
import polars as pl

def main():
    #connect to an in-memory DuckDB database
   conn = duckdb.connect(database=':memory:')
   
   file_path = "C:/Users/khare/DE_Oct2025/big_file/data/nppes_sample.csv"
   quary = f"SELECT * FROM read_csv_auto('{file_path}')LIMIT 5"
   
   sample_data = (conn.execute(quary).fetchall())
   data = pl.DataFrame(sample_data, strict=False)
   clean_data=data.drop_nulls()
   print(clean_data.head())
  
   sample_data = (conn.execute(quary).df())
   
   clean_data=sample_data.dropna()
   
   
   print(clean_data.head())
  
#   #make file size smaller almost half
#    sample_data.to_parquet('nppes_sample.parquet')
    
#    products = (conn.execute("SELECT * FROM read_json_auto('C:/Users/khare/DE_Oct2025/big_file/data/products.json')").df())
   
#    print(products.head())
   
#    conn.execute(
#         "CREATE TABLE if not exists nppes AS SELECT * FROM read_csv_auto('C:/Users/khare/DE_Oct2025/big_file/data/nppes_sample.csv')"
#     )

#    conn.sql("select NPI from nppes limit 10").show()
   conn.close()
   
   
   
if __name__ == '__main__':
    main()