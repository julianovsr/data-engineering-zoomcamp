#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


@click.command()
@click.option('--parquet-file', required=True, help='Path to parquet file')
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', required=True, help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for processing')
@click.option('--if-exists', default='replace', type=click.Choice(['replace', 'append', 'fail']), help='How to behave if table exists')
def run(parquet_file, pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, chunksize, if_exists):
    """Read a parquet file and insert it into a PostgreSQL database."""
    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    
    # Read parquet file in chunks
    parquet_file_path = parquet_file
    
    try:
        # Read the parquet file
        df = pd.read_parquet(parquet_file_path)
        
        # Process in chunks
        total_rows = len(df)
        num_chunks = (total_rows + chunksize - 1) // chunksize
        
        first = True
        
        with tqdm(total=total_rows, desc='Processing rows') as pbar:
            for i in range(0, total_rows, chunksize):
                df_chunk = df.iloc[i:i+chunksize]
                
                if first:
                    # Create table structure with first chunk
                    df_chunk.head(0).to_sql(
                        name=target_table,
                        con=engine,
                        if_exists=if_exists,
                        index=False
                    )
                    first = False
                
                # Insert data
                df_chunk.to_sql(
                    name=target_table,
                    con=engine,
                    if_exists='append',
                    index=False
                )
                
                pbar.update(len(df_chunk))
        
        click.echo(f'✓ Successfully inserted {total_rows} rows into {target_table}')
        
    except FileNotFoundError:
        click.echo(f'❌ Error: Parquet file not found: {parquet_file_path}', err=True)
        raise
    except Exception as e:
        click.echo(f'❌ Error: {str(e)}', err=True)
        raise


if __name__ == '__main__':
    run()
