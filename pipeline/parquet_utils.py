"""
Utility functions for ingesting parquet files into PostgreSQL.
"""

import pandas as pd
from sqlalchemy import create_engine, URL
from pathlib import Path
from typing import Optional
from tqdm.auto import tqdm


def create_db_url(user: str, password: str, host: str, port: int, database: str) -> str:
    """Create PostgreSQL connection URL."""
    return f'postgresql+psycopg://{user}:{password}@{host}:{port}/{database}'


def parquet_to_postgres(
    parquet_file: str,
    table_name: str,
    pg_user: str = 'root',
    pg_pass: str = 'root',
    pg_host: str = 'localhost',
    pg_port: int = 5432,
    pg_db: str = 'ny_taxi',
    chunksize: int = 100000,
    if_exists: str = 'replace',
    verbose: bool = True
) -> dict:
    """
    Read a parquet file and insert it into a PostgreSQL table.
    
    Args:
        parquet_file: Path to parquet file
        table_name: Target table name in PostgreSQL
        pg_user: PostgreSQL username
        pg_pass: PostgreSQL password
        pg_host: PostgreSQL host
        pg_port: PostgreSQL port
        pg_db: PostgreSQL database name
        chunksize: Number of rows to process at a time
        if_exists: How to handle existing table ('replace', 'append', 'fail')
        verbose: Print progress information
    
    Returns:
        Dictionary with operation statistics
    """
    parquet_path = Path(parquet_file)
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
    
    # Create database connection
    db_url = create_db_url(pg_user, pg_pass, pg_host, pg_port, pg_db)
    engine = create_engine(db_url)
    
    # Read parquet file
    if verbose:
        print(f"Reading parquet file: {parquet_file}")
    
    df = pd.read_parquet(parquet_path)
    total_rows = len(df)
    
    if verbose:
        print(f"Total rows to insert: {total_rows}")
    
    # Insert data in chunks
    first = True
    inserted_rows = 0
    
    pbar = tqdm(total=total_rows, desc='Inserting rows', disable=not verbose)
    
    for i in range(0, total_rows, chunksize):
        df_chunk = df.iloc[i:i+chunksize]
        
        if first:
            # Create table structure
            df_chunk.head(0).to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False
            )
            first = False
        
        # Insert data
        df_chunk.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        
        inserted_rows += len(df_chunk)
        pbar.update(len(df_chunk))
    
    pbar.close()
    
    if verbose:
        print(f"✓ Successfully inserted {inserted_rows} rows into table '{table_name}'")
    
    return {
        'file': parquet_file,
        'table': table_name,
        'total_rows': inserted_rows,
        'status': 'success'
    }


if __name__ == '__main__':
    # Example usage
    result = parquet_to_postgres(
        parquet_file='data.parquet',
        table_name='test_table'
    )
    print(f"\nResult: {result}")
