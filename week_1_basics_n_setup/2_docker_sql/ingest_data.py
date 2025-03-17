
import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_name = 'output.parquet'   
    csv_name = 'output.csv'
    
    # download data
    os.system(f'wget {url} -O {parquet_name}')
    pq = pd.read_parquet(parquet_name)
    pq.to_csv(csv_name, index=False)
    
    # create database server
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # read csv
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime =pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Create table and table head
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # ingest first 100000 rows
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # use iterator to read the dataset, each time with chunksize = 10000
    while True:
        t_start = time()
        df = next(df_iter)
        df.tpep_pickup_datetime =pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print("inserted another trunk..., took %.3f seconds" %(t_end-t_start))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data to Postgres")

    #user, password, host, port, database name = ny_taxi, table name = yellow_taxi_data
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for psotgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for posgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the file')

    args = parser.parse_args()
    
    main(args)


