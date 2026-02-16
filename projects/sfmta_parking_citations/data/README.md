# Data Layer

This directory contains data organized by processing stage:

- raw/    -> Original citation-level dataset
- clean/  -> Cleaned and validated Parquet files
- mart/   -> Aggregated analytical marts (source of truth) and DuckDB database file

The project follows a layered structure:

raw -> clean -> mart

Large data files are excluded from version control.