# Ingest parquet file
# !/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

@click.command()
@click.option('--target-table', default='zones', help='Target table name')
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
def run(target_table, pg_user, pg_pass, pg_host, pg_port, pg_db):
    
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    trips = pd.read_parquet("green_tripdata_2025-11.parquet")
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }
    trips = trips.astype(dtype)

    CHUNK_SIZE = 100_000

    first = True

    for i in tqdm(range(0, len(trips), CHUNK_SIZE)):
        df_chunk = trips.iloc[i : i + CHUNK_SIZE]

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name="green_taxi_data",
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name="green_taxi_data",
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        print("Inserted:", len(df_chunk))
        

if __name__ == "__main__":
    run()