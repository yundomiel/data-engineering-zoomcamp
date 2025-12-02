#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq


def main(params):
    user = params.user
    password = params.password
    host= params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    #csv_name = 'output.parquet'
 
    if not os.path.exists(url):
        raise FileNotFoundError(f"Local file not found: {url}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    if url.endswith('.parquet'):
        print(f"Reading parquet file: {url}")
        parquet_file = pq.ParquetFile(url)
        iterator = parquet_file.iter_batches(batch_size=100000)

        first_batch = next(iterator).to_pandas()
    else:
        print(f"Reading CSV file: {url}")
        first_batch = pd.read_csv(url, nrows=100000)
        iterator = pd.read_csv(url, iterator=True, chunksize=100000)

   

    first_batch.tpep_pickup_datetime = pd.to_datetime(first_batch.tpep_pickup_datetime)
    first_batch.tpep_dropoff_datetime = pd.to_datetime(first_batch.tpep_dropoff_datetime)


    first_batch.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    first_batch.to_sql(name=table_name, con=engine, if_exists='append')
    print("Inserted first chunk")

    for batch in iterator:
        t_start = time()

        df = batch if isinstance(batch, pd.DataFrame) else batch.to_pandas()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    #user, password, host, port, database name, table name, url of the csv or parquet

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the target file')
    args = parser.parse_args()
   
    main(args)














