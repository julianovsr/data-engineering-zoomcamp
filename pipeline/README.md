# Pipeline Scripts

## Scripts

### `ingest_parquet.py`
Reads a parquet file and inserts it into a PostgreSQL table.

**Usage:**
```bash
python ingest_parquet.py --parquet-file data.parquet --target-table my_table
```

**Options:**
- `--parquet-file` (required): Path to the parquet file
- `--target-table` (required): Target PostgreSQL table name
- `--pg-user` (default: root): PostgreSQL username
- `--pg-pass` (default: root): PostgreSQL password
- `--pg-host` (default: localhost): PostgreSQL host
- `--pg-port` (default: 5432): PostgreSQL port
- `--pg-db` (default: ny_taxi): PostgreSQL database name
- `--chunksize` (default: 100000): Number of rows to process at a time
- `--if-exists` (default: replace): How to handle existing table (replace/append/fail)

**Example:**
```bash
# Insert into a new table
python ingest_parquet.py \
  --parquet-file /path/to/data.parquet \
  --target-table yellow_taxi_data \
  --pg-host localhost \
  --pg-db ny_taxi

# Append to an existing table
python ingest_parquet.py \
  --parquet-file /path/to/data.parquet \
  --target-table yellow_taxi_data \
  --if-exists append
```

### `ingest_data.py`
Downloads and ingests NYC taxi data from CSV format.

**Usage:**
```bash
python ingest_data.py --year 2021 --month 1
```
