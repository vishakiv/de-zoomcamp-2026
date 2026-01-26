#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String
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

    df_iter = pd.read_csv(
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    iterator=True,
    )

    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                dtype={
                        "LocationID": Integer(),
                        "Borough": String(),
                        "Zone": String(),
                        "service_zone": String()
                        }
            )
            first = False

            print(f"Table {target_table} created")

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")
    print(f'done ingesting to {target_table}')
        

if __name__ == "__main__":
    run()


